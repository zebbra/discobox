"""Netbox client: lookups, writes, type resolution, read retries."""

import ipaddress
import logging
import re
from typing import Callable, Optional, Union

import pynetbox
import requests
import urllib3

from .log import logger
from .util import (
    CREATED_BY_COMMENT,
    DUMMY_INTERFACES,
    TypeAlias,
    _compile_type_aliases,
    _delete_with_retry,
    _merged_custom_fields,
    _mount_read_retries,
    _norm_vendor,
    _split_aliases,
    is_oid_model,
    parse_os_release,
    slugify,
)


class _ChangelogSession(requests.Session):
    """requests.Session that sets the discobox User-Agent, counts requests and retries dropped reads."""
    def __init__(self, on_request: Optional[Callable[[str], None]] = None):
        super().__init__()
        _mount_read_retries(self)
        self._on_request = on_request or (lambda method: None)
        self.headers.update({"User-Agent": "discobox"})

    def request(self, method, url, **kwargs):
        self._on_request(method.upper())
        return super().request(method, url, **kwargs)


class NetboxClient:
    def __init__(
        self,
        url: str,
        token: str,
        verify_tls: bool = True,
        on_request: Optional[Callable[[str], None]] = None,
        type_alias_cf: Optional[str] = "snmp_models",
        device_type_aliases: Optional[dict] = None,
        module_type_aliases: Optional[dict] = None,
        create_missing_types: bool = True,
    ):
        session = _ChangelogSession(on_request=on_request)
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session.verify = False
        self.nb = pynetbox.api(url, token=token)
        self.nb.http_session = session
        self.type_alias_cf = type_alias_cf
        # called as on_type_created(kind, manufacturer, model) after a bare
        # DeviceType ("device") / ModuleType ("module") was minted (metrics)
        self.on_type_created: Optional[Callable[[str, str, str], None]] = None
        self.device_type_aliases = _compile_type_aliases(device_type_aliases)
        self.module_type_aliases = _compile_type_aliases(module_type_aliases)
        self.create_missing_types = create_missing_types
        # object type ("dcim.devicetype") → whether type_alias_cf exists on it;
        # only cached after a successful lookup
        self._alias_cf_enabled: dict[str, bool] = {}
        # source marker on types discobox mints bare (e.g. source=netdisco); set by the caller
        self.type_source_cf: Optional[str] = None
        self.type_source_value: str = "netdisco"
        self._cf_on_cache: dict[tuple[str, str], bool] = {}

    def find_device_by_ip(self, ip: str, hostname: str = "", serial: str = "") -> Optional[pynetbox.core.response.Record]:
        """
        Find a Netbox device by management IP, with hostname and serial fallbacks.

        Strategy:
          1. Search IPAM for the address; walk the assignment back to a device.
          2. Scan devices whose primary_ip4 matches.
          3. Exact hostname match (name__ie): works when Netdisco has the full FQDN.
          4. Short-name contains match (name__ic): fallback for when Netdisco returns
             a hostname from PTR that differs from the Netbox FQDN (e.g. wrong domain).
          5. Serial match: most reliable identifier when IP and hostname both fail.
        """
        for addr in self.nb.ipam.ip_addresses.filter(address=ip):
            if addr.assigned_object_type == "dcim.interface" and addr.assigned_object:
                iface = self.nb.dcim.interfaces.get(addr.assigned_object.id)
                if iface and iface.device:
                    device = self.nb.dcim.devices.get(iface.device.id)
                    if device:
                        return device

        for dev in self.nb.dcim.devices.filter(q=ip):
            if dev.primary_ip4 and dev.primary_ip4.address.split("/")[0] == ip:
                return dev

        if hostname:
            short = hostname.lower().split(".")[0]
            for dev in self.nb.dcim.devices.filter(name__ie=hostname):
                logger.info("Device found by hostname %r (no IP match for %s)", dev.name, ip)
                return dev
            for dev in self.nb.dcim.devices.filter(name__ic=short):
                if dev.name and dev.name.lower().split(".")[0] == short:
                    logger.info("Device found by short hostname %r (no IP match for %s)", dev.name, ip)
                    return dev

        if serial:
            dev = self.find_device_by_serial(serial)
            if dev:
                logger.info("Device found by serial %r (no IP/hostname match for %s)", serial, ip)
                return dev

        return None

    def update_device_fields(
        self,
        device: pynetbox.core.response.Record,
        nd_device: dict,
        log: Optional[logging.Logger] = None,
        cf_os_version: Optional[str] = "os_version",
        cf_os_name: Optional[str] = "os_name",
        cf_os_release: Optional[str] = "os_release",
    ) -> bool:
        """Update serial, and custom fields os_ver / os_name / os_release.

        Returns True if a PATCH was actually sent (pynetbox diffs against the
        record's fetched state, so an unchanged value never issues a request).
        """
        _log = log or logger
        patch: dict = {}

        # A multi-unit system (e.g. a 9800 HA pair) reports every unit's serial
        # ("A B"): that's no single device's serial. The chassis pass sets each
        # unit's own one instead of fighting it every sync.
        nd_serial = (nd_device.get("serial") or "").strip()
        if nd_serial and len(nd_serial.split()) == 1:
            patch["serial"] = nd_serial

        custom: dict = {}
        if cf_os_version and nd_device.get("os_ver"):
            custom[cf_os_version] = nd_device["os_ver"]
        if cf_os_name and nd_device.get("os"):
            custom[cf_os_name] = nd_device["os"]
        if cf_os_release:
            os_release = parse_os_release(nd_device.get("description"))
            if os_release:
                custom[cf_os_release] = os_release
        if custom:
            patch["custom_fields"] = _merged_custom_fields(device, custom)

        if not patch:
            _log.debug("Device fields: nothing to update")
            return False

        changed = device.update(patch)
        _log.debug(
            "Device fields: serial=%r os_name=%r os_ver=%r os_release=%r",
            patch.get("serial"), custom.get(cf_os_name or ""),
            custom.get(cf_os_version or ""), custom.get(cf_os_release or ""),
        )
        return bool(changed)

    def fetch_interfaces(self, device_id: int) -> dict[str, pynetbox.core.response.Record]:
        """Return all existing interfaces for a device, keyed by name."""
        return {
            iface.name: iface
            for iface in self.nb.dcim.interfaces.filter(device_id=device_id)
            if iface.name is not None
        }

    @staticmethod
    def _nb_value(val) -> object:
        """Unwrap pynetbox ChoiceValue / Record to a plain comparable value."""
        if hasattr(val, "value"):   # ChoiceValue (type, duplex, ...)
            return val.value
        if hasattr(val, "id"):      # Related Record (device, ...)
            return val.id
        return val

    def upsert_interface(
        self,
        device_id: int,
        data: dict,
        existing: Optional[pynetbox.core.response.Record],
        source_cf: Optional[str] = None,
        source_value: str = "netdisco",
        batch: Optional[list] = None,
    ) -> tuple:
        """
        Create or update a Netbox interface.

        If source_cf is set and the existing interface has that CF set to a value
        other than empty or source_value, the interface is left untouched ("skipped").

        MAC address is handled separately via upsert_mac() because in Netbox 4.x
        it is its own model (dcim.mac-addresses) rather than a plain string field.
        Custom fields are compared key-by-key to avoid overwriting unrelated fields.

        If batch is given, a field patch on an existing interface is appended to it
        (as {"id": ..., **patch}) instead of being PATCHed immediately — the caller
        is responsible for flushing it (e.g. via flush_interface_updates()) so that
        one device's interface changes land in a single bulk request/changelog
        request_id instead of one per interface. The in-memory record is updated
        either way, so callers reading `iface` attributes see the new values
        immediately regardless of whether the write has actually been flushed yet.

        Returns (action, iface) where action is one of: "created", "updated", "unchanged", "skipped".
        """
        mac = data.pop("mac_address", None)
        custom_fields = data.pop("custom_fields", None) or {}

        # Inject ownership marker so we can identify our interfaces later
        if source_cf and source_value:
            custom_fields[source_cf] = source_value

        if existing is None:
            try:
                create_data = dict(data)
                if not create_data.get("type"):
                    create_data["type"] = "other"   # Netbox requires a type on create
                iface = self.nb.dcim.interfaces.create(**create_data, device=device_id)
                if custom_fields:
                    iface.update({"custom_fields": custom_fields})
                action = "created"
            except pynetbox.RequestError as exc:
                if "already exists" in str(exc):
                    # Cache miss: interface exists in NetBox but wasn't in the snapshot — update instead
                    fetched = list(self.nb.dcim.interfaces.filter(device_id=device_id, name=data["name"]))
                    if fetched:
                        existing = fetched[0]
                    else:
                        raise
                else:
                    raise
        if existing is not None:
            # Skip interfaces owned by someone else
            if source_cf:
                existing_cf = dict(getattr(existing, "custom_fields", {}) or {})
                owner = existing_cf.get(source_cf) or ""
                if owner and owner != source_value:
                    return "skipped", existing

            # speed/duplex flap on their own (autonegotiation jitter) without
            # anything else about the interface actually changing, so neither
            # justifies a patch by itself — but if something else already
            # triggers one, apply their current values too rather than pinning
            # them to a stale one.
            _NON_TRIGGER_FIELDS = {"speed", "duplex"}
            patch = {}
            trigger_fields: set[str] = set()
            for k, v in data.items():
                if v is None:
                    continue
                nb_val = self._nb_value(getattr(existing, k, None))
                if nb_val != v:
                    logger.debug("  diff %-20s  nb=%r  nd=%r", k, nb_val, v)
                    patch[k] = v
                    if k not in _NON_TRIGGER_FIELDS:
                        trigger_fields.add(k)
            if custom_fields:
                existing_cf = dict(getattr(existing, "custom_fields", {}) or {})
                cf_patch = {k: v for k, v in custom_fields.items() if existing_cf.get(k) != v}
                if cf_patch:
                    patch["custom_fields"] = cf_patch
                    trigger_fields.add("custom_fields")
            if not trigger_fields:
                for field in _NON_TRIGGER_FIELDS:
                    patch.pop(field, None)
            if patch:
                if batch is not None:
                    for k, v in patch.items():
                        if k == "custom_fields":
                            existing_cf = dict(getattr(existing, "custom_fields", {}) or {})
                            existing.custom_fields = {**existing_cf, **v}
                        else:
                            setattr(existing, k, v)
                    batch.append({"id": existing.id, **patch})
                else:
                    existing.update(patch)
                action = "updated"
            else:
                action = "unchanged"
            iface = existing

        if mac:
            self._upsert_mac(iface, mac)

        return action, iface

    def _flush_bulk_update(
        self, endpoint, batch: list, label: str, log: Optional[logging.Logger] = None,
    ) -> set:
        """
        Send a batch of {"id": ..., **patch} dicts (built up by an upsert_*(..., batch=[])
        call) to `endpoint` as a single bulk PATCH, so all resulting changelog entries
        share one request_id instead of one per object.

        If the bulk request fails (e.g. one bad item in the batch — Netbox bulk PATCH
        is atomic, so one invalid item rolls back the whole request), falls back to
        applying each patch individually so a single bad item doesn't silently drop
        every other update in the batch.

        Returns the set of ids that failed to update (empty on full success), so the
        caller can correct any optimistic "updated" counts.
        """
        _log = log or logger
        if not batch:
            return set()
        try:
            endpoint.update(batch)
            return set()
        except Exception as exc:
            _log.warning(
                "Bulk %s update failed (%d items), falling back to per-item: %s",
                label, len(batch), exc,
            )
        failed_ids: set = set()
        for item in batch:
            try:
                endpoint.update([item])
            except Exception as exc:
                failed_ids.add(item["id"])
                _log.error("  %s id=%s update error: %s", label, item["id"], exc)
        return failed_ids

    def flush_interface_updates(
        self, batch: list, log: Optional[logging.Logger] = None,
    ) -> set:
        """Flush a batch of interface patches built up by upsert_interface(..., batch=[])."""
        return self._flush_bulk_update(self.nb.dcim.interfaces, batch, "interface", log)

    def flush_module_updates(
        self, batch: list, log: Optional[logging.Logger] = None,
    ) -> set:
        """Flush a batch of module patches built up by upsert_module(..., batch=[])."""
        return self._flush_bulk_update(self.nb.dcim.modules, batch, "module", log)

    def upsert_cable(
        self,
        iface_a_id: int,
        iface_b_id: int,
        cable_type: str = "smf",
        source_cf: Optional[str] = "source",
        source_value: Optional[str] = "netdisco",
    ) -> str:
        """
        Create a cable between two dcim.interface endpoints if none exists.

        An existing cable on either termination is checked against the wanted
        link, not just its presence:
          - it connects exactly iface_a ↔ iface_b: "exists" (an unowned one is
            claimed by setting source_cf; another source's one is a "conflict")
          - it is dangling (one end gone, e.g. the peer device was deleted and
            Netbox kept the cable) or discobox owns it and it leads elsewhere
            (re-patched link): deleted, then the right cable is created
          - otherwise it leads elsewhere and isn't discobox's (manually
            maintained, or another source's): "conflict", left alone
        Returns one of: "created", "exists", "conflict", "skipped".
        """
        iface_a = self.nb.dcim.interfaces.get(iface_a_id)
        iface_b = self.nb.dcim.interfaces.get(iface_b_id)
        if not iface_a or not iface_b:
            return "skipped"

        wanted = {iface_a_id, iface_b_id}
        cable_ids = {c.id for c in (getattr(iface_a, "cable", None), getattr(iface_b, "cable", None)) if c}
        for cable_id in sorted(cable_ids):
            # the nested .cable on an interface is brief: fetch terminations + custom fields
            cable = self.nb.dcim.cables.get(cable_id)
            if not cable:
                continue
            a_ends = list(getattr(cable, "a_terminations", None) or [])
            b_ends = list(getattr(cable, "b_terminations", None) or [])
            ends = {getattr(t, "id", None) for t in a_ends + b_ends}
            owner = (dict(getattr(cable, "custom_fields", {}) or {}).get(source_cf) or "") if source_cf else ""
            dangling = not a_ends or not b_ends
            if ends == wanted and not dangling:
                if owner and owner != source_value:
                    logger.error(
                        "Cable conflict: iface %s ↔ %s: existing cable %s not owned by discobox, skipping",
                        iface_a_id, iface_b_id, cable_id,
                    )
                    return "conflict"
                if source_cf and source_value and not owner:
                    cable.update({"custom_fields": {source_cf: source_value}})   # unowned: claim it
                return "exists"
            if dangling or (source_value and owner == source_value):
                logger.info(
                    "Cable %s on iface %s ↔ %s replaced (%s)", cable_id, iface_a_id, iface_b_id,
                    "dangling" if dangling else f"led to interface(s) {sorted(e for e in ends if e not in wanted)}",
                )
                cable.delete()
                continue
            logger.error(
                "Cable conflict: iface %s ↔ %s: existing cable %s leads elsewhere and isn't discobox's, skipping",
                iface_a_id, iface_b_id, cable_id,
            )
            return "conflict"

        try:
            cable = self.nb.dcim.cables.create(
                a_terminations=[{"object_type": "dcim.interface", "object_id": iface_a_id}],
                b_terminations=[{"object_type": "dcim.interface", "object_id": iface_b_id}],
                type=cable_type,
            )
        except pynetbox.RequestError as exc:
            if "unique_termination" in str(exc):
                return "exists"
            raise
        if source_cf and source_value:
            cable.update({"custom_fields": {source_cf: source_value}})
        return "created"

    def delete_cable(self, cable_id: int) -> None:
        cable = self.nb.dcim.cables.get(cable_id)
        if cable:
            cable.delete()

    def _upsert_mac(self, iface: pynetbox.core.response.Record, mac: str) -> None:
        """
        Create or update the primary MAC address for an interface (Netbox 4.x).

        In Netbox 4.x MAC addresses are standalone objects assigned to an interface.
        After creating the MAC object we explicitly set primary_mac_address on the
        interface so it shows up as the primary.
        """
        primary = getattr(iface, "primary_mac_address", None)

        if primary:
            existing_mac = str(primary.mac_address).upper() if hasattr(primary, "mac_address") else ""
            if existing_mac == mac.upper():
                return  # already correct
            mac_obj = self.nb.dcim.mac_addresses.get(primary.id)
            mac_obj.update({"mac_address": mac})
            logger.debug("  MAC updated → %s", mac)
        else:
            mac_obj = self.nb.dcim.mac_addresses.create(
                mac_address=mac,
                assigned_object_type="dcim.interface",
                assigned_object_id=iface.id,
            )
            iface.update({"primary_mac_address": mac_obj.id})
            logger.debug("  MAC created → %s", mac)

    def upsert_ip(self, address: str, iface: pynetbox.core.response.Record) -> str:
        """
        Create or update an IP address assigned to an interface.

        Returns one of: "created", "unchanged", "skipped".
        """
        host = str(ipaddress.ip_interface(address).ip)

        # Check if this host IP already exists anywhere in Netbox
        existing = list(self.nb.ipam.ip_addresses.filter(address=host))

        if existing:
            nb_ip = existing[0]
            same_iface = (
                nb_ip.assigned_object_type == "dcim.interface"
                and nb_ip.assigned_object_id == iface.id
            )
            if same_iface:
                nb_host = str(ipaddress.ip_interface(nb_ip.address).ip)
                nd_host = str(ipaddress.ip_interface(address).ip)
                if nb_ip.address == address or nb_host != nd_host:
                    return "unchanged"
                # Same host, different prefix: fix it
                nb_ip.update({"address": address})
                return "fixed"

            # Unassigned IP (e.g. after VIP device was deleted): claim it
            if not nb_ip.assigned_object:
                nb_ip.update({
                    "address": address,
                    "assigned_object_type": "dcim.interface",
                    "assigned_object_id": iface.id,
                })
                logger.info("  IP %-20s was unassigned → assigned to %s", address, iface.name)
                return "moved"

            # Check if the IP is on a dummy placeholder interface
            assigned_name = (
                nb_ip.assigned_object.name
                if nb_ip.assigned_object_type == "dcim.interface" and nb_ip.assigned_object
                else None
            )
            if assigned_name and assigned_name.lower() in {d.lower() for d in DUMMY_INTERFACES}:
                nb_ip.update({
                    "address": address,
                    "assigned_object_type": "dcim.interface",
                    "assigned_object_id": iface.id,
                })
                logger.info(
                    "  IP %-20s moved from dummy interface %r → %s",
                    address, assigned_name, iface.name,
                )
                return "moved"

            # Same device, wrong interface: follow Netdisco's port assignment.
            # (The vip-role branch below is for IPs shared across HA members —
            # it must not fire for a misplaced IP on this very device.)
            assigned_dev_id = (
                getattr(getattr(nb_ip.assigned_object, "device", None), "id", None)
                if nb_ip.assigned_object_type == "dcim.interface" and nb_ip.assigned_object
                else None
            )
            target_dev_id = getattr(getattr(iface, "device", None), "id", None)
            if assigned_dev_id and target_dev_id and assigned_dev_id == target_dev_id:
                nb_ip.update({
                    "address": address,
                    "assigned_object_type": "dcim.interface",
                    "assigned_object_id": iface.id,
                })
                logger.info(
                    "  IP %-20s moved within device: %r → %r",
                    address, assigned_name, iface.name,
                )
                return "moved"

            # IP is shared across HA members: ensure it carries the vip role
            if getattr(nb_ip, "role", None) != "vip":
                nb_ip.update({"role": "vip"})
                logger.info(
                    "  IP %-20s already assigned to %s: role set to vip",
                    address, nb_ip.assigned_object or "unassigned",
                )
            else:
                logger.warning(
                    "  IP %-20s already in Netbox (id=%s, assigned to %s): skipping",
                    address, nb_ip.id, nb_ip.assigned_object or "unassigned",
                )
            return "skipped"

        self.nb.ipam.ip_addresses.create(
            address=address,
            status="active",
            assigned_object_type="dcim.interface",
            assigned_object_id=iface.id,
        )
        return "created"

    def get_or_create_manufacturer(self, name: str) -> pynetbox.core.response.Record:
        """
        Return an existing Manufacturer or create one.

        Case-insensitive lookup: Netdisco's device-level `vendor` field is
        lowercase (e.g. "cisco"), which would otherwise create a duplicate
        manufacturer alongside an existing properly-cased "Cisco".
        """
        existing = list(self.nb.dcim.manufacturers.filter(name__ie=name))
        if existing:
            return existing[0]
        mfr = self.nb.dcim.manufacturers.create(name=name, slug=slugify(name))
        logger.debug("  Manufacturer created: %s", name)
        return mfr

    def cf_on(self, name: Optional[str], object_type: str) -> bool:
        """True when custom field `name` exists on object_type (cached after a successful lookup)."""
        if not name:
            return False
        key = (name, object_type)
        if key not in self._cf_on_cache:
            try:
                assigned: set[str] = set()
                for cf in self.nb.extras.custom_fields.filter(name=name):
                    assigned.update(getattr(cf, "object_types", None) or getattr(cf, "content_types", None) or [])
            except Exception as exc:
                logger.warning("Custom field %r lookup failed: %s", name, exc)
                return False
            self._cf_on_cache[key] = object_type in assigned
        return self._cf_on_cache[key]

    def _type_source_kwargs(self, object_type: str) -> dict:
        """custom_fields for a bare type discobox creates: its source marker, if that CF exists there."""
        cf = getattr(self, "type_source_cf", None)     # tests build clients without __init__
        if cf and self.cf_on(cf, object_type):
            return {"custom_fields": {cf: self.type_source_value}}
        return {}

    def _alias_cf_active(self, object_type: str) -> bool:
        """True when type_alias_cf is configured and exists on object_type (e.g. "dcim.devicetype")."""
        if not self.type_alias_cf:
            return False
        if object_type not in self._alias_cf_enabled:
            try:
                assigned: set[str] = set()
                for cf in self.nb.extras.custom_fields.filter(name=self.type_alias_cf):
                    # NetBox 4.x: object_types; 3.x: content_types
                    assigned.update(getattr(cf, "object_types", None) or getattr(cf, "content_types", None) or [])
            except Exception as exc:
                logger.warning("Type alias custom field %r lookup failed: %s", self.type_alias_cf, exc)
                return False
            enabled = object_type in assigned
            self._alias_cf_enabled[object_type] = enabled
            if not enabled:
                logger.info(
                    "Type alias custom field %r not assigned to %s: alias lookup via Netbox disabled",
                    self.type_alias_cf, object_type,
                )
        return self._alias_cf_enabled[object_type]

    def _find_type_by_ref(self, endpoint, ref: str) -> Optional[pynetbox.core.response.Record]:
        """Exact part_number, then model, then slug match across all manufacturers."""
        for field in ("part_number", "model", "slug"):
            for r in endpoint.filter(**{field: ref}):
                if getattr(r, field, None) == ref:
                    return r
        return None

    def _lookup_type_alias(
        self,
        endpoint,
        object_type: str,
        aliases: list[TypeAlias],
        raws: list[Optional[str]],
        vendors: tuple[Optional[str], ...] = (),
    ) -> tuple[Optional[pynetbox.core.response.Record], bool]:
        """
        Translate raw SNMP model strings to an existing DeviceType/ModuleType.

        Config aliases are tried first (they override Netbox; entries scoped
        to one of vendors before unscoped ones), then the type_alias_cf custom
        field on the types themselves. Returns (record, matched): matched with
        record None means an alias exists but its target type doesn't: the
        caller must not create a type from the raw string.
        """
        candidates = [r for r in dict.fromkeys(raws) if r]
        vendor_keys = {_norm_vendor(v) for v in vendors if v}
        for vendor, matches, target in aliases:
            if vendor and vendor not in vendor_keys:
                continue
            raw = next((r for r in candidates if matches(r)), None)
            if raw is None:
                continue
            found = self._find_type_by_ref(endpoint, target)
            if found:
                logger.debug("  Type alias %r → %r (config)", raw, target)
            else:
                logger.warning(
                    "  Type alias %r → %r: no such %s in Netbox, create it or fix the alias",
                    raw, target, object_type,
                )
            return found, True
        if self._alias_cf_active(object_type):
            cf = self.type_alias_cf
            for raw in candidates:
                wanted = raw.strip().lower()
                hits = [
                    r for r in endpoint.filter(**{f"cf_{cf}__ic": raw})
                    if wanted in {a.lower() for a in _split_aliases((getattr(r, "custom_fields", {}) or {}).get(cf))}
                ]
                if hits:
                    # the same OID fragment may be listed on types of several vendors
                    hit = next(
                        (r for r in hits if _norm_vendor(getattr(getattr(r, "manufacturer", None), "name", None)) in vendor_keys),
                        hits[0],
                    )
                    logger.debug("  Type alias %r → %r (custom field %s)", raw, getattr(hit, "model", hit), cf)
                    return hit, True
        return None, False

    def resolve_device_type_alias(
        self, *raws: Optional[str], vendor: Optional[str] = None,
    ) -> Optional[pynetbox.core.response.Record]:
        """DeviceType a type alias maps any of raws to, or None: never falls back or creates."""
        found, _ = self._lookup_type_alias(
            self.nb.dcim.device_types, "dcim.devicetype", self.device_type_aliases, list(raws), (vendor,),
        )
        return found

    def get_or_create_device_type(
        self,
        manufacturer: Union[pynetbox.core.response.Record, str],
        model: str,
        part_number: Optional[str] = None,
        vendor: Optional[str] = None,
        create: bool = True,
    ) -> Optional[pynetbox.core.response.Record]:
        """
        Return an existing DeviceType or create one under manufacturer.

        Type aliases (see _lookup_type_alias) win over everything else, so a
        raw SNMP model like "enterprises.2440" can be pointed at a curated type.

        Then matches by part_number across ALL manufacturers: different
        vendors essentially never share a part number, so if this hardware
        already has a DeviceType record, reuse it (and its manufacturer)
        rather than risking a second copy under a wrongly-resolved vendor
        name (e.g. Netdisco's lowercase "cisco" vs. an existing "Cisco").

        OID fragments (see is_oid_model) are only ever resolved via an alias,
        never matched or created as-is.

        manufacturer may be a name, resolved (and created) only when needed.
        vendor (Netdisco's device vendor) selects vendor-scoped aliases, as
        does the manufacturer name. Returns None when nothing matches and
        create_missing_types is off (or create=False: a lookup that must never
        mint a type), when an alias points at a type that doesn't exist, or
        when only OID fragments are known.
        """
        mfr_name = manufacturer if isinstance(manufacturer, str) else getattr(manufacturer, "name", None)
        found, matched = self._lookup_type_alias(
            self.nb.dcim.device_types, "dcim.devicetype", self.device_type_aliases,
            [part_number, model], (vendor, mfr_name),
        )
        if matched:
            return found
        raw_model = model
        if is_oid_model(part_number):
            part_number = None
        if is_oid_model(model):
            model = part_number or ""
        if not model:
            logger.warning(
                "  DeviceType skipped: Netdisco model %r is an OID fragment, add a type alias (vendor %r)",
                raw_model, vendor or mfr_name,
            )
            return None
        if part_number:
            results = list(self.nb.dcim.device_types.filter(part_number=part_number))
            existing = next((r for r in results if getattr(r, "part_number", None) == part_number), None)
            if existing:
                return existing
        if isinstance(manufacturer, str):
            manufacturer = self.get_or_create_manufacturer(manufacturer)
        slug = slugify(model)
        results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if getattr(r, "model", None) == model), None)
        if not existing and part_number:
            results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, part_number=part_number))
            existing = next((r for r in results if getattr(r, "part_number", None) == part_number), None)
        if not existing:
            results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if getattr(r, "slug", None) == slug), None)
        if existing:
            if not getattr(existing, "part_number", None):
                existing.update({"part_number": part_number or model})
            return existing
        if not create:
            logger.debug("  DeviceType %s / %s not in Netbox: lookup only, not created", manufacturer.name, model)
            return None
        if not self.create_missing_types:
            logger.warning(
                "  DeviceType %s / %s not in Netbox and types.create_missing is off: skipped",
                manufacturer.name, model,
            )
            return None
        dt = self.nb.dcim.device_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=part_number or model,
            comments=CREATED_BY_COMMENT,
            **self._type_source_kwargs("dcim.devicetype"),
        )
        logger.debug("  DeviceType created: %s / %s", manufacturer.name, model)
        self._notify_type_created("device", manufacturer.name, model)
        return dt

    def get_or_create_module_type(
        self,
        manufacturer: pynetbox.core.response.Record,
        model: str,
        vendor: Optional[str] = None,
    ) -> Optional[pynetbox.core.response.Record]:
        """
        Return an existing ModuleType or create one under manufacturer.

        Type aliases first, then part_number across ALL manufacturers — see
        get_or_create_device_type for why, and for when None is returned.
        """
        found, matched = self._lookup_type_alias(
            self.nb.dcim.module_types, "dcim.moduletype", self.module_type_aliases,
            [model], (vendor, getattr(manufacturer, "name", None)),
        )
        if matched:
            return found
        if is_oid_model(model):
            logger.warning(
                "  ModuleType skipped: Netdisco model %r is an OID fragment, add a type alias (vendor %r)",
                model, vendor or getattr(manufacturer, "name", None),
            )
            return None
        results = list(self.nb.dcim.module_types.filter(part_number=model))
        existing = next((r for r in results if getattr(r, "part_number", None) == model), None)
        if existing:
            return existing
        slug = slugify(model)
        results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if getattr(r, "model", None) == model), None)
        if not existing:
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, part_number=model))
            existing = next((r for r in results if getattr(r, "part_number", None) == model), None)
        if not existing:
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if getattr(r, "slug", None) == slug), None)
        if existing:
            if not getattr(existing, "part_number", None):
                existing.update({"part_number": model})
            return existing
        if not self.create_missing_types:
            logger.warning(
                "  ModuleType %s / %s not in Netbox and types.create_missing is off: skipped",
                manufacturer.name, model,
            )
            return None
        mt = self.nb.dcim.module_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=model,
            comments=CREATED_BY_COMMENT,
            **self._type_source_kwargs("dcim.moduletype"),
        )
        logger.debug("  ModuleType created: %s / %s", manufacturer.name, model)
        self._notify_type_created("module", manufacturer.name, model)
        return mt

    def _notify_type_created(self, kind: str, manufacturer: str, model: str) -> None:
        hook = getattr(self, "on_type_created", None)   # tests build clients without __init__
        if hook:
            try:
                hook(kind, manufacturer, model)
            except Exception as exc:
                logger.debug("on_type_created hook failed: %s", exc)

    def upsert_module_bay(
        self,
        device: pynetbox.core.response.Record,
        name: str,
        position: str,
    ) -> pynetbox.core.response.Record:
        """Return an existing ModuleBay or create one on the device."""
        name = (name or "")[:64]
        results = list(self.nb.dcim.module_bays.filter(device_id=device.id, name=name))
        if len(results) > 1:
            logger.warning("  ModuleBay %r: %d duplicates in Netbox, using first", name, len(results))
            return results[0]
        if results:
            return results[0]
        bay = self.nb.dcim.module_bays.create(
            device=device.id,
            name=name,
            position=position,
        )
        logger.debug("  ModuleBay created: %s", name)
        return bay

    def upsert_module(
        self,
        device: pynetbox.core.response.Record,
        bay: pynetbox.core.response.Record,
        module_type: pynetbox.core.response.Record,
        serial: str,
        batch: Optional[list] = None,
    ) -> tuple[str, pynetbox.core.response.Record]:
        """
        Install or update a Module in a ModuleBay. Returns (action, module_record).

        If batch is given, a field patch on an existing module is appended to it
        instead of being PATCHed immediately — see upsert_interface()'s batch
        parameter for the full rationale. The in-memory record is updated either way.
        """
        serial = serial or ""
        results = list(self.nb.dcim.modules.filter(module_bay_id=bay.id))
        existing = results[0] if results else None
        if existing:
            patch = {}
            if self._nb_value(existing.module_type) != module_type.id:
                patch["module_type"] = module_type.id
            if (existing.serial or "") != serial:
                patch["serial"] = serial
            if not patch:
                return "unchanged", existing
            if batch is not None:
                for k, v in patch.items():
                    setattr(existing, k, v)
                batch.append({"id": existing.id, **patch})
            else:
                existing.update(patch)
            return "updated", existing

        # Note: the REST API has no replicate/adopt control (UI-form only, still
        # absent in Netbox 4.4) — creating a Module ALWAYS instantiates the
        # ModuleType's component templates, and any existing same-name interface
        # aborts the whole create with an IntegrityError 500.
        payload = {
            "device": device.id,
            "module_bay": bay.id,
            "module_type": module_type.id,
            "serial": serial,
            "status": "active",
        }
        try:
            module = self.nb.dcim.modules.create(**payload)
        except pynetbox.RequestError as exc:
            if "already exists" not in str(exc):
                raise
            if not self._clear_module_template_collisions(device, bay, module_type):
                raise
            module = self.nb.dcim.modules.create(**payload)
        return "created", module

    def _clear_module_template_collisions(
        self,
        device: pynetbox.core.response.Record,
        bay: pynetbox.core.response.Record,
        module_type: pynetbox.core.response.Record,
    ) -> int:
        """
        Emulate the UI's "adopt components" the only way the REST API allows:
        delete existing device interfaces that collide with the ModuleType's
        interface templates so a retried module create can replicate them.
        Only IP-less interfaces are deleted — their MAC, cable and neighbor
        fields are re-created by the interface sync later in the same run.
        Returns the number of interfaces cleared.
        """
        try:
            templates = list(self.nb.dcim.interface_templates.filter(module_type_id=module_type.id))
        except Exception as exc:
            logger.warning("  Module %r: could not fetch interface templates: %s", bay.name, exc)
            return 0
        if not templates:
            return 0
        position = str(getattr(bay, "position", "") or "")
        existing = {
            i.name: i
            for i in self.nb.dcim.interfaces.filter(device_id=device.id)
            if i.name
        }
        cleared = 0
        for tpl in templates:
            name = (tpl.name or "").replace("{module}", position)
            iface = existing.get(name)
            if iface is None:
                continue
            ips = list(self.nb.ipam.ip_addresses.filter(
                assigned_object_type="dcim.interface", assigned_object_id=iface.id,
            ))
            if ips:
                logger.warning(
                    "  Module %r: colliding interface %r holds %d IP(s): cannot adopt, keeping",
                    bay.name, name, len(ips),
                )
                continue
            try:
                iface.delete()
                cleared += 1
                logger.info(
                    "  Module %r: deleted colliding interface %r (re-created from template)",
                    bay.name, name,
                )
            except Exception as exc:
                logger.warning("  Module %r: could not delete colliding interface %r: %s", bay.name, name, exc)
        return cleared

    def find_ap_device(self, hostname: str, serial: str = "", ip: str = "") -> Optional[pynetbox.core.response.Record]:
        """
        Find a WLC-reported access point: serial first (exact, and APs are
        usually on DHCP so the IP rarely matches Netbox), then hostname, then
        short hostname. ip is only used for the log line.
        """
        ip_str = ip or "none"
        if serial:
            dev = self.find_device_by_serial(serial)
            if dev:
                logger.debug("  Found AP by serial %r → %s (ip %s)", serial, dev.name, ip_str)
                return dev
        short = hostname.lower().split(".")[0]
        for dev in self.nb.dcim.devices.filter(name__ie=hostname):
            logger.info("Found AP by name %r (ip %s, serial %r not in Netbox)", dev.name, ip_str, serial)
            return dev
        for dev in self.nb.dcim.devices.filter(name__ic=short):
            if dev.name and dev.name.lower().split(".")[0] == short:
                logger.info("Found AP by short name %r (ip %s, serial %r not in Netbox)", dev.name, ip_str, serial)
                return dev
        return None

    def find_device_by_serial(self, serial: str) -> Optional[pynetbox.core.response.Record]:
        results = list(self.nb.dcim.devices.filter(serial=serial))
        return results[0] if results else None

    def find_ha_partner(self, device_name: str) -> Optional[pynetbox.core.response.Record]:
        """
        Find the HA partner device by swapping the node indicator in the hostname.

        Matches patterns like p1h↔p2h, node1↔node2, -1↔-2 near the end of the
        short hostname (before the first dot).
        """
        short = device_name.split(".")[0]
        suffix = device_name[len(short):]
        m = re.search(r"(p|node|-)([12])([a-z]?)", short, re.IGNORECASE)
        if not m:
            return None
        other = "2" if m.group(2) == "1" else "1"
        partner_short = short[:m.start(2)] + other + short[m.end(2):]
        partner_name = partner_short + suffix
        results = list(self.nb.dcim.devices.filter(name__ie=partner_name))
        if not results:
            results = list(self.nb.dcim.devices.filter(name__ie=partner_short))
        return results[0] if results else None

    def upsert_virtual_chassis(
        self,
        name: str,
        members: list[tuple[pynetbox.core.response.Record, int]],
    ) -> tuple[str, pynetbox.core.response.Record]:
        """Create or update a Virtual Chassis and assign member devices."""
        results = list(self.nb.dcim.virtual_chassis.filter(name=name))
        vc = results[0] if results else None
        action = "unchanged"
        if not vc:
            # A member may already master a VC under a legacy name (Netbox
            # allows one VC per master device): adopt and rename it instead of
            # creating a duplicate, so members never have to move.
            adoptable = next(
                (r for device, _pos in members
                 for r in self.nb.dcim.virtual_chassis.filter(master_id=device.id)),
                None,
            )
            if adoptable is not None:
                vc = adoptable
                old_name = vc.name
                vc.update({"name": name})
                action = "renamed"
                logger.info("  VC %r renamed to %r (adopted from legacy name)", old_name, name)
            else:
                vc = self.nb.dcim.virtual_chassis.create(name=name, master=members[0][0].id)
                action = "created"

        for device, position in members:
            patch = {}
            original = {}
            cur_vc_id = self._nb_value(getattr(device, "virtual_chassis", None))
            if cur_vc_id != vc.id:
                original["virtual_chassis"] = getattr(device, "virtual_chassis", None)
                patch["virtual_chassis"] = vc.id
            if getattr(device, "vc_position", None) != position:
                original["vc_position"] = getattr(device, "vc_position", None)
                patch["vc_position"] = position
            # Netbox refuses to move a device out of a VC it is the master of.
            # When that old VC has no other members it is a stale artifact (e.g.
            # left behind by the earlier VC-name mismatch): release the master
            # so the move succeeds, and delete the empty shell afterwards.
            stale_vc = None
            if "virtual_chassis" in patch and cur_vc_id:
                old_vc = self.nb.dcim.virtual_chassis.get(cur_vc_id)
                if old_vc and self._nb_value(getattr(old_vc, "master", None)) == device.id:
                    others = [
                        d for d in self.nb.dcim.devices.filter(virtual_chassis_id=cur_vc_id)
                        if d.id != device.id
                    ]
                    if not others:
                        old_vc.update({"master": None})
                        stale_vc = old_vc
                        logger.info(
                            "  VC %r: released %s as master (stale single-member VC)",
                            old_vc.name, device.name,
                        )
            if patch:
                try:
                    device.update(patch)
                except Exception:
                    # update() assigns attributes before saving; a rejected save
                    # (e.g. Netbox refusing to move a VC master) must not leave
                    # the record dirty, or the next save() on this device would
                    # re-send the rejected change and fail the whole sync.
                    for k, v in original.items():
                        setattr(device, k, v)
                    raise
                if stale_vc is not None:
                    try:
                        stale_vc.delete()
                        logger.info("  VC %r: deleted (stale, now empty)", stale_vc.name)
                    except Exception as exc:
                        logger.warning("  VC %r: could not delete stale VC: %s", stale_vc.name, exc)
                action = action if action == "created" else "updated"
                logger.info("  VC member %-40s pos=%d  updated", device.name, position)
            else:
                logger.info("  VC member %-40s pos=%d  unchanged", device.name, position)
        return action, vc

    def remove_stale_device_bays(
        self,
        device: pynetbox.core.response.Record,
        patterns: list,
    ) -> int:
        """
        Delete device bays whose names match any of the given compiled regex patterns.

        These are typically auto-created from the DeviceType template
        (e.g. 'PS-A', 'PSU1', 'Fan 1') and should be removed when managing
        power supplies and modules via inventory items / module bays instead.

        Returns the number of bays deleted.
        """
        deleted = 0
        for bay in self.nb.dcim.device_bays.filter(device_id=device.id):
            if any(p.search(bay.name) for p in patterns):
                if bay.installed_device:
                    logger.warning(
                        "  Device bay %r has installed device %r: skipping deletion",
                        bay.name, bay.installed_device,
                    )
                    continue
                bay.delete()
                logger.debug("  Deleted device bay %r", bay.name)
                deleted += 1
        return deleted

    def remove_stale_module_bays(
        self,
        device: pynetbox.core.response.Record,
        patterns: list,
    ) -> int:
        """
        Delete module bays on a device whose names match any of the given compiled
        regex patterns and have no module installed.

        These are typically auto-created from the ModuleType template when a module
        is installed (e.g. 'FAN 1', 'PS-A', 'Network Module') and should be removed
        when discobox manages inventory via its own module bay names.

        Returns the number of bays deleted.
        """
        deleted = 0
        for bay in self.nb.dcim.module_bays.filter(device_id=device.id):
            if not any(p.search(bay.name) for p in patterns):
                continue
            if getattr(bay, "installed_module", None):
                logger.warning(
                    "  Module bay %r has installed module: skipping deletion", bay.name
                )
                continue
            bay.delete()
            logger.debug("  Deleted module bay %r", bay.name)
            deleted += 1
        return deleted

    def remove_empty_dummy_interfaces(
        self,
        device: pynetbox.core.response.Record,
        dummy_names: set[str],
        nd_port_names: set[str] | None = None,
    ) -> int:
        """
        Delete dummy placeholder interfaces that have no IPs assigned and do not
        appear in Netdisco's port list (interfaces present in Netdisco are real,
        not placeholders, regardless of their name).

        Safe to run after IP sync: if the IP was moved to the real interface
        the dummy is now empty and can be removed.

        Returns the number of interfaces deleted.
        """
        deleted = 0
        lower_names = {n.lower() for n in dummy_names}
        nd_lower = {n.lower() for n in (nd_port_names or set())}
        for iface in self.nb.dcim.interfaces.filter(device_id=device.id):
            if not iface.name:
                continue
            if iface.name.lower() not in lower_names:
                continue
            if iface.name.lower() in nd_lower:
                logger.debug(
                    "  Dummy interface %r exists in Netdisco: keeping", iface.name,
                )
                continue
            ips = list(self.nb.ipam.ip_addresses.filter(
                assigned_object_type="dcim.interface",
                assigned_object_id=iface.id,
            ))
            if ips:
                logger.warning(
                    "  Dummy interface %r still has %d IP(s): skipping deletion",
                    iface.name, len(ips),
                )
                continue
            iface.delete()
            logger.info("  Deleted empty dummy interface %r on %s", iface.name, device.name)
            deleted += 1
        return deleted

    def remove_stale_interfaces(
        self,
        device: pynetbox.core.response.Record,
        current_names: set[str],
        source_cf: Optional[str],
        source_value: str,
        dry_run: bool = True,
    ) -> int:
        """
        Delete discobox-owned interfaces on `device` that Netdisco no longer reports.

        Only interfaces whose custom_fields[source_cf] == source_value are touched —
        an interface with no ownership marker, or owned by something else, is left
        alone regardless of whether it's in current_names.

        Returns the number of interfaces deleted (or that would be, under dry_run).
        """
        if not source_cf:
            return 0
        lower_names = {n.lower() for n in current_names}
        deleted = 0
        for iface in self.nb.dcim.interfaces.filter(device_id=device.id):
            if not iface.name or iface.name.lower() in lower_names:
                continue
            owner = dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) or ""
            if owner != source_value:
                continue
            if dry_run:
                logger.info("  [dry-run] would delete stale interface %r", iface.name)
            elif _delete_with_retry(iface, f"stale interface {iface.name!r}"):
                logger.info("  Deleted stale interface %r", iface.name)
            else:
                continue
            deleted += 1
        return deleted

    def remove_stale_modules(
        self,
        device: pynetbox.core.response.Record,
        current_bay_names: set[str],
        iface_source_cf: Optional[str],
        iface_source_value: str,
        dry_run: bool = True,
    ) -> tuple[int, int]:
        """
        Delete module bays (+ their installed module) on `device` that Netdisco no
        longer reports as a chassis member / blade.

        A bay is kept (and the skip logged as a warning) if its installed module
        still has an interface attached that isn't discobox-owned — deleting the
        module would orphan someone else's manually-managed interface. Run
        remove_stale_interfaces() first so any discobox-owned interfaces on a
        genuinely-removed module are already gone by the time this runs.

        Returns (deleted, skipped_foreign).
        """
        deleted = 0
        skipped_foreign = 0
        for bay in self.nb.dcim.module_bays.filter(device_id=device.id):
            if bay.name in current_bay_names:
                continue
            module = getattr(bay, "installed_module", None)
            if module:
                foreign = [
                    iface for iface in self.nb.dcim.interfaces.filter(module_id=module.id)
                    if (dict(getattr(iface, "custom_fields", {}) or {}).get(iface_source_cf or "") or "")
                    != iface_source_value
                ]
                if foreign:
                    logger.warning(
                        "  Module bay %r: kept — %d non-netdisco interface(s) still attached",
                        bay.name, len(foreign),
                    )
                    skipped_foreign += 1
                    continue
                if dry_run:
                    logger.info("  [dry-run] would delete module %r in bay %r", module, bay.name)
                else:
                    module.delete()
            if dry_run:
                logger.info("  [dry-run] would delete module bay %r", bay.name)
            else:
                bay.delete()
                logger.info("  Deleted stale module bay %r", bay.name)
            deleted += 1
        return deleted, skipped_foreign

    def remove_stale_bare_inventory_items(
        self,
        device: pynetbox.core.response.Record,
        current_names: set[str],
        dry_run: bool = True,
    ) -> int:
        """
        Delete device-level Inventory Items (fans, PSUs — no component link) that
        Netdisco no longer reports. Items linked to an interface (SFPs) are left to
        remove_stale_sfps() instead.

        Also skips anything with no serial and no part_id: upsert_inventory_item()
        only ever creates an item when Netdisco reports a model or serial, so a
        bare item with neither wasn't created by discobox — it's almost certainly
        a Netbox ModuleType InventoryItemTemplate placeholder (e.g. a stack
        member's "StackPort1/1"/"StackAdapter1/1"), which Netbox auto-recreates
        whenever the parent Module is saved. Deleting those just starts a tug-of-
        war discobox can't win, identical to the interface-template collision
        problem _clear_module_template_collisions works around.

        Returns the number of items deleted (or that would be, under dry_run).
        """
        deleted = 0
        for item in self.nb.dcim.inventory_items.filter(device_id=device.id):
            if getattr(item, "component_type", None):
                continue
            if not (getattr(item, "serial", None) or getattr(item, "part_id", None)):
                continue
            if item.name in current_names:
                continue
            if dry_run:
                logger.info("  [dry-run] would delete inventory item %r", item.name)
            else:
                item.delete()
                logger.info("  Deleted stale inventory item %r", item.name)
            deleted += 1
        return deleted

    def remove_stale_sfps(
        self,
        device: pynetbox.core.response.Record,
        current_iface_ids: set[int],
        dry_run: bool = True,
    ) -> int:
        """
        Delete SFP Inventory Items (component-linked to an interface) on `device`
        whose interface no longer reports an SFP in this Netdisco snapshot.

        Returns the number of items deleted (or that would be, under dry_run).
        """
        deleted = 0
        for item in self.nb.dcim.inventory_items.filter(
            device_id=device.id, component_type="dcim.interface",
        ):
            if item.component_id in current_iface_ids:
                continue
            if dry_run:
                logger.info("  [dry-run] would delete SFP inventory item %r", item.name)
            else:
                item.delete()
                logger.info("  Deleted stale SFP inventory item %r", item.name)
            deleted += 1
        return deleted

    def upsert_sfp(
        self,
        device: pynetbox.core.response.Record,
        iface: pynetbox.core.response.Record,
        manufacturer: pynetbox.core.response.Record,
        name: str,
        model: str,
        serial: str,
    ) -> str:
        """
        Create or update an Inventory Item representing an SFP on an interface.

        Returns one of: "created", "updated", "unchanged".
        """
        existing = list(self.nb.dcim.inventory_items.filter(
            device_id=device.id,
            component_type="dcim.interface",
            component_id=iface.id,
        ))

        if existing:
            inv = existing[0]
            patch = {}
            if (inv.serial or "") != serial:
                patch["serial"] = serial
            if (inv.part_id or "") != model:
                patch["part_id"] = model
            if not patch:
                return "unchanged"
            inv.update(patch)
            return "updated"

        self.nb.dcim.inventory_items.create(
            device=device.id,
            name=name,
            manufacturer=manufacturer.id,
            part_id=model,
            serial=serial,
            component_type="dcim.interface",
            component_id=iface.id,
        )
        return "created"

    def upsert_inventory_item(
        self,
        device: pynetbox.core.response.Record,
        name: str,
        manufacturer: Optional[pynetbox.core.response.Record],
        model: str,
        serial: str,
    ) -> str:
        """
        Create or update a device-level Inventory Item (no component link).

        Returns one of: "created", "updated", "unchanged".
        """
        serial = serial or ""
        existing = list(self.nb.dcim.inventory_items.filter(
            device_id=device.id,
            name=name,
        ))

        if existing:
            inv = existing[0]
            patch = {}
            if (inv.serial or "") != serial:
                patch["serial"] = serial
            if model and (inv.part_id or "") != model:
                patch["part_id"] = model
            if not patch:
                return "unchanged"
            inv.update(patch)
            return "updated"

        payload: dict = {"device": device.id, "name": name, "serial": serial}
        if model:
            payload["part_id"] = model
        if manufacturer:
            payload["manufacturer"] = manufacturer.id
        self.nb.dcim.inventory_items.create(**payload)
        return "created"
