#!/usr/bin/env python3
"""
discobox: Netdisco → Netbox sync library.

Imported by cli.py (one-shot CLI) and server.py (FastAPI webhook receiver).
"""

import ipaddress
import json
import logging
import os
import re
import sys
from typing import Optional

import pynetbox
import requests
import urllib3

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("discobox")


# ── Interface type mapping ─────────────────────────────────────────────────────

def map_iftype(nd_type: Optional[str], iface_name: Optional[str], default: Optional[str] = "other") -> Optional[str]:
    """
    Map a Netdisco interface to a Netbox type slug.

    Name-prefix matching is tried first because Netdisco reports nearly every
    physical port as ifType=ethernetCsmacd regardless of speed: the interface
    name (e.g. "TenGigabitEthernet0/1") carries the real type information.
    SNMP ifType is used as a fallback for LAG and virtual interfaces.

    When Netdisco reports no ifType at all (e.g. FortiGate devices with null
    types on every port) and no name rule matches, `default` is returned —
    pass default=None to express "no opinion" so the caller can preserve
    whatever type Netbox already has.

    Adapted from github.com/joeladria/netdisco-netbox-diode.
    """
    lname = (iface_name or "").lower()
    nd_type_l = (nd_type or "").lower()

    # Dot-notation subinterfaces (e.g. "TwentyFiveGigE1/1/8.2802", "Po1.100") are
    # always virtual: Netbox only allows a `parent` on interfaces of type virtual,
    # and the name-prefix rules below would otherwise assign the parent's physical type.
    if re.search(r"\.\d+$", lname):                                 return "virtual"

    # LAG / virtual via ifType: these are reliable regardless of name
    if "lag" in nd_type_l:                                          return "lag"
    if nd_type_l in ("softwareloopback", "propvirtual", "l2vlan",
                     "l3ipvlan"):                                   return "virtual"
    if nd_type_l == "tunnel":                                       return "virtual"

    # Name-prefix matches
    if lname.startswith(("loopback", "lo")):                        return "virtual"
    if lname.startswith("vlan"):                                     return "virtual"
    if lname.startswith(("tunnel", "tun", "gr", "ipip", "null")):   return "virtual"
    if lname.startswith(("port-channel", "bundle-ether", "be",
                          "etherchannel", "eth-trunk", "ae")):      return "lag"
    if re.match(r"po\d", lname):                                    return "lag"
    if lname.startswith(("management", "mgmt", "fxp", "em")):       return "1000base-t"
    if lname.startswith(("hundredgig", "hu", "ce", "et")):          return "100gbase-x-qsfp28"
    if lname.startswith("fiftygig"):                                 return "50gbase-x-sfp28"
    if lname.startswith(("fortygig", "fo")):                        return "40gbase-x-qsfpp"
    if lname.startswith(("twentyfive", "tf")):                      return "25gbase-x-sfp28"
    if lname.startswith(("tengig", "te", "xe")):                    return "10gbase-x-sfpp"
    if lname.startswith(("fivegig", "fg")):                         return "5gbase-t"
    if lname.startswith(("2.5gig", "twoandahalf", "tg")):           return "2.5gbase-t"
    if lname.startswith(("gigabit", "gi")):                         return "1000base-t"
    if lname.startswith(("fastethernet", "fa")):                    return "100base-tx"
    if lname.startswith(("ethernet", "eth")):                       return "1000base-t"
    if lname.startswith(("vmbr", "br")):                            return "bridge"
    if lname.startswith(("serial", "se")):                          return "other"

    # ifType fallback
    if "virtual" in nd_type_l:                                      return "virtual"
    if "ethernet" in nd_type_l:                                     return "1000base-t"

    if not nd_type_l:
        return default
    return "other"


_COPPER_IFACE_TYPES = {"100base-tx", "1000base-t", "2.5gbase-t", "5gbase-t", "10gbase-t"}

def cable_type_from_iface_type(iface_type: str) -> str:
    return "cat5e" if iface_type in _COPPER_IFACE_TYPES else "smf"


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_os_release(description: Optional[str]) -> Optional[str]:
    """
    Extract the IOS release name from the Netdisco device description.

    Example: "Cisco IOS Software [Gibraltar], Catalyst ..."  →  "Gibraltar"
    """
    if not description:
        return None
    m = re.search(r"\[([A-Za-z0-9]+)\]", description)
    return m.group(1) if m else None


def parse_sw_ver(sw_ver: Optional[str]) -> Optional[str]:
    """
    Extract a plain version number from a chassis sw_ver string.

    Examples:
      "FortiGate-600F v7.4.8,build2795,250523 (GA.M)"  →  "7.4.8"
      "Version 17.3.4"                                  →  "17.3.4"
    """
    if not sw_ver:
        return None
    m = re.search(r"v?(\d+\.\d+[\.\d]*)", sw_ver)
    return m.group(1) if m else None


def parse_sw_model(sw_ver: Optional[str]) -> Optional[str]:
    """
    Extract the product model name from a sw_ver string (the word before the version).

    Example: "FortiGate-600F v7.4.8,build2795,250523 (GA.M)"  →  "FortiGate-600F"
    """
    if not sw_ver:
        return None
    m = re.match(r"(\S+)\s+v\d+", sw_ver)
    return m.group(1) if m else None


def parse_speed_kbps(speed_str: Optional[str]) -> Optional[int]:
    """
    Parse Netdisco speed string to kbps for Netbox.

    Examples: "10 Mbps" → 10_000, "1 Gbps" → 1_000_000, "auto" → None
    """
    if not speed_str:
        return None
    m = re.match(r"([\d.]+)\s*(kbps|mbps|gbps)", speed_str.strip(), re.IGNORECASE)
    if not m:
        return None
    value, unit = float(m.group(1)), m.group(2).lower()
    multipliers = {"kbps": 1, "mbps": 1_000, "gbps": 1_000_000}
    return int(value * multipliers[unit])



def slugify(value: str) -> str:
    """Convert a string to a Netbox-compatible slug (lowercase, alphanum + hyphens)."""
    s = value.lower()
    s = re.sub(r"[^a-z0-9_-]", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")


def validate_ip(value: str) -> str:
    """Validate that value is a valid IP address. Raises ValueError if invalid."""
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        raise ValueError(f"not a valid IP address: {value!r}")


# ── Netdisco client ────────────────────────────────────────────────────────────

class NetdiscoClient:
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        verify_tls: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = verify_tls
        self.session.headers.update({"Accept": "application/json", "User-Agent": "discobox"})
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._username = username
        self._password = password
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
            logger.debug("Netdisco auth via static token")
        elif username and password:
            self._login(username, password)
        else:
            raise ValueError("NetdiscoClient requires either token or username+password")

    def _login(self, username: str, password: str) -> None:
        url = f"{self.base_url}/login"
        logger.debug("POST %s", url)
        resp = self.session.post(url, auth=(username, password), timeout=15)
        resp.raise_for_status()
        token = resp.json().get("api_key") or resp.json().get("token")
        if not token:
            logger.error("Netdisco login response did not contain a token: %s", resp.text)
            sys.exit(1)
        self.session.headers["Authorization"] = token
        logger.debug("Netdisco login OK")

    def _reauth(self) -> bool:
        """Re-login with username/password if credentials are available. Returns True on success."""
        if self._username and self._password:
            logger.warning("Netdisco token expired: re-authenticating")
            self._login(self._username, self._password)
            return True
        return False

    def _get(self, path: str) -> dict | list:
        url = f"{self.base_url}{path}"
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 401 and self._reauth():
            resp = self.session.get(url, timeout=30)
        if resp.status_code == 401:
            logger.error("Netdisco 401 GET %s: response: %s", url, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body) -> dict | list:
        url = f"{self.base_url}{path}"
        logger.debug("POST %s", url)
        resp = self.session.post(url, json=body, timeout=30)
        if resp.status_code == 401 and self._reauth():
            resp = self.session.post(url, json=body, timeout=30)
        if resp.status_code == 401:
            logger.error("Netdisco 401 POST %s: response: %s", url, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    def get_device(self, ip: str) -> dict:
        result = self._get(f"/api/v1/object/device/{ip}")
        # Some Netdisco versions return a list, others a dict
        if isinstance(result, list):
            return result[0] if result else {}
        return result

    def get_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/ports")

    def get_device_ips(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/device_ips")

    def get_modules(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/modules")

    def get_powered_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/powered_ports")

    def get_all_devices(self) -> list[dict]:
        return self._get("/api/v1/object/devices?fields=ip,dns,name")

    def get_queue_status(self, since: str = "1h") -> dict:
        return self._get(f"/api/v1/queue/status?since={since}")

    def enqueue_discover(self, ip: str, device_auth_tag_hint: Optional[str] = None, snmp_timeout_us: Optional[int] = None) -> None:
        extra: dict = {
            "snmptimeout": snmp_timeout_us if snmp_timeout_us is not None else 3_000_000,
            "skip_neighbor_queue": True,
        }
        if device_auth_tag_hint:
            extra["device_auth_tag_hint"] = device_auth_tag_hint
        self._post("/api/v1/queue/jobs", [{"action": "discover", "device": ip, "extra": json.dumps(extra)}])


# ── Netbox client ──────────────────────────────────────────────────────────────

class _ChangelogSession(requests.Session):
    """requests.Session that injects changelog_message into every write body."""
    def __init__(self, changelog_message: str):
        super().__init__()
        self._changelog_message = changelog_message
        self.headers.update({"User-Agent": "discobox"})

    def request(self, method, url, **kwargs):
        if method.upper() in ("POST", "PATCH", "PUT") and self._changelog_message:
            json_data = kwargs.get("json")
            if isinstance(json_data, dict):
                kwargs["json"] = {**json_data, "changelog_message": self._changelog_message}
        return super().request(method, url, **kwargs)


class NetboxClient:
    def __init__(self, url: str, token: str, verify_tls: bool = True, changelog_message: str = "DiscoBox"):
        session = _ChangelogSession(changelog_message)
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session.verify = False
        self.nb = pynetbox.api(url, token=token)
        self.nb.http_session = session

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
    ) -> None:
        """Update serial, and custom fields os_ver / os_name / os_release."""
        _log = log or logger
        patch: dict = {}

        if nd_device.get("serial"):
            patch["serial"] = nd_device["serial"]

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
            patch["custom_fields"] = custom

        if not patch:
            _log.debug("Device fields: nothing to update")
            return

        device.update(patch)
        _log.debug(
            "Device fields: serial=%r os_name=%r os_ver=%r os_release=%r",
            patch.get("serial"), custom.get(cf_os_name or ""),
            custom.get(cf_os_version or ""), custom.get(cf_os_release or ""),
        )

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
    ) -> tuple:
        """
        Create or update a Netbox interface.

        If source_cf is set and the existing interface has that CF set to a value
        other than empty or source_value, the interface is left untouched ("skipped").

        MAC address is handled separately via upsert_mac() because in Netbox 4.x
        it is its own model (dcim.mac-addresses) rather than a plain string field.
        Custom fields are compared key-by-key to avoid overwriting unrelated fields.

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

            patch = {}
            for k, v in data.items():
                if v is None:
                    continue
                nb_val = self._nb_value(getattr(existing, k, None))
                if nb_val != v:
                    logger.debug("  diff %-20s  nb=%r  nd=%r", k, nb_val, v)
                    patch[k] = v
            if custom_fields:
                existing_cf = dict(getattr(existing, "custom_fields", {}) or {})
                cf_patch = {k: v for k, v in custom_fields.items() if existing_cf.get(k) != v}
                if cf_patch:
                    patch["custom_fields"] = cf_patch
            if patch:
                existing.update(patch)
                action = "updated"
            else:
                action = "unchanged"
            iface = existing

        if mac:
            self._upsert_mac(iface, mac)

        return action, iface

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
        If a cable already exists on either termination and is NOT tagged with
        source_value, log an error and skip (don't touch manually maintained cables).
        Returns one of: "created", "exists", "conflict", "skipped".
        """
        iface_a = self.nb.dcim.interfaces.get(iface_a_id)
        iface_b = self.nb.dcim.interfaces.get(iface_b_id)
        if not iface_a or not iface_b:
            return "skipped"

        cable_a = getattr(iface_a, "cable", None)
        cable_b = getattr(iface_b, "cable", None)
        existing_cable = cable_a or cable_b

        if existing_cable:
            if source_cf:
                cf = dict(getattr(existing_cable, "custom_fields", {}) or {})
                owner = cf.get(source_cf) or ""
                if owner and owner != source_value:
                    logger.error(
                        "Cable conflict: iface %s ↔ %s: existing cable %s not owned by discobox, skipping",
                        iface_a_id, iface_b_id, existing_cable.id,
                    )
                    return "conflict"
                # Unowned cable: claim it
                if not owner and source_value:
                    existing_cable.update({"custom_fields": {source_cf: source_value}})
            return "exists"

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
        """Return an existing Manufacturer or create one."""
        existing = self.nb.dcim.manufacturers.get(name=name)
        if existing:
            return existing
        mfr = self.nb.dcim.manufacturers.create(name=name, slug=slugify(name))
        logger.debug("  Manufacturer created: %s", name)
        return mfr

    def get_or_create_device_type(
        self,
        manufacturer: pynetbox.core.response.Record,
        model: str,
        part_number: Optional[str] = None,
    ) -> pynetbox.core.response.Record:
        """Return an existing DeviceType or create one under manufacturer."""
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
        dt = self.nb.dcim.device_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=part_number or model,
            comments="Created by discobox",
        )
        logger.debug("  DeviceType created: %s / %s", manufacturer.name, model)
        return dt

    def get_or_create_module_type(
        self,
        manufacturer: pynetbox.core.response.Record,
        model: str,
    ) -> pynetbox.core.response.Record:
        """Return an existing ModuleType or create one under manufacturer."""
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
        mt = self.nb.dcim.module_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=model,
            comments="Created by discobox",
        )
        logger.debug("  ModuleType created: %s / %s", manufacturer.name, model)
        return mt

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
    ) -> tuple[str, pynetbox.core.response.Record]:
        """Install or update a Module in a ModuleBay. Returns (action, module_record)."""
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
            existing.update(patch)
            return "updated", existing

        module = self.nb.dcim.modules.create(
            device=device.id,
            module_bay=bay.id,
            module_type=module_type.id,
            serial=serial,
            status="active",
            replication_mode="adopt",
        )
        return "created", module

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
        m = re.search(r"(p|node|-)([12])(h?)", short, re.IGNORECASE)
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
            logger.info("  Deleted empty dummy interface %r", iface.name)
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


# ── Field mapping ──────────────────────────────────────────────────────────────

# Device bay name patterns (regex) auto-created from DeviceType templates that
# should be removed when the device is managed via module bays / inventory items.
STALE_DEVICE_BAY_PATTERNS: list[re.Pattern] = [re.compile(p, re.IGNORECASE) for p in [
    r"^Network Module$",
    r"^PS-[AB]$",
    r"^PS\s*\d+$",
    r"^PSU\s*\d+$",
    r"^Fan\s*\d+$",
    r"^Slot\s*\d+$",
]]

# Dummy/placeholder interface names used as IP anchors before a proper sync.
# IPs found on these interfaces will be moved to the correct interface.
# In housekeeping, empty dummy interfaces are deleted.
DUMMY_INTERFACES: set[str] = {
    "main", "mgmt", "mgmt0",
}

# Interface name prefixes startwith (case-insensitive) to skip during port sync.
PORT_BLACKLIST_PREFIXES: tuple[str, ...] = (
    "null",
    "modem",
    "bluetooth",
    "ssl."
)

def vendor_from_chassis(chassis: dict) -> Optional[str]:
    """
    Extract a vendor/manufacturer name from a Netdisco chassis entry.

    Returns None for Cisco devices (caller falls back to device's existing
    manufacturer). For other vendors the name is encoded in the type string:
      "fortinet.6007.6007.0"  →  "Fortinet"

    Cisco ENTITY-MIB OID names all start with "cev" (e.g. cevChassisN9KC93600CDGX)
    and never contain a leading vendor prefix.
    """
    type_str = chassis.get("type") or ""
    # Cisco types: let caller use the device's existing Netbox manufacturer
    if type_str.lower().startswith("cev"):
        return None
    # Other vendors encode their name before the first dot
    if "." in type_str:
        vendor = type_str.split(".")[0]
        if vendor:
            return vendor.capitalize()
    return None


NULL_MAC = "00:00:00:00:00:00"


_CISCO_ABBREV = [
    ("HundredGigabitEthernet", "HundredGigE"),
    ("HundredGigabitEthernet", "Hu"),
    ("FortyGigabitEthernet",  "Fo"),
    ("TwentyFiveGigE",        "Twe"),
    ("TenGigabitEthernet",    "Te"),
    ("GigabitEthernet",       "Gi"),
    ("FastEthernet",          "Fa"),
    ("Ethernet",              "Et"),
    ("Management",            "Mg"),
    ("Loopback",              "Lo"),
    ("Vlan",                  "Vl"),
    ("Port-channel",          "Po"),
    ("Tunnel",                "Tu"),
    ("Serial",                "Se"),
    ("AppGigabitEthernet",    "Ap"),
]

def expand_iface_name(name: str) -> str:
    """Expand a Cisco abbreviated interface name to its full form.

    E.g. 'Gi3/0/1' → 'GigabitEthernet3/0/1', 'Te1/1' → 'TenGigabitEthernet1/1'.
    Returns the original string if no prefix matches.
    """
    for full, abbrev in _CISCO_ABBREV:
        if name.startswith(abbrev) and not name.startswith(full):
            return full + name[len(abbrev):]
    return name


def _slave_link_field(iface_name: str, nd_type: Optional[str], nb_type: Optional[str]) -> str:
    """Pick the Netbox field for a slave_of link.

    Virtual children (VLAN subinterfaces stacked on a LAG) attach via `parent`;
    physical members via `lag`. When Netdisco reports no ifType (e.g. FortiGate
    with null types on every port), trust the existing Netbox type — otherwise a
    virtual child would be mis-wired as a LAG member and rejected with 400.
    """
    if "." in iface_name:
        return "parent"
    if map_iftype(nd_type, iface_name, default=None) == "virtual":
        return "parent"
    if nb_type == "virtual":
        return "parent"
    return "lag"


def _ha_node_info(short: str) -> Optional[tuple[int, str, str]]:
    """Parse an HA node indicator from a short hostname.

    Returns (node_num, vc_base, vip_short) or None when no indicator matches:
      "zcgate0005p1h" → (1, "zcgate0005", "zcgate0005p0h")
      "fw-node2"      → (2, "fw", "fw-node0")
    """
    m = re.search(r"(p|node|-)([12])(h?)", short, re.IGNORECASE)
    if not m:
        return None
    node_num = int(m.group(2))
    vc_base = (short[: m.start(1)] + short[m.end(3):]).rstrip("-_") or short
    vip_short = short[: m.start(2)] + "0" + short[m.end(2):]
    return node_num, vc_base, vip_short


def _fill_module_names(nd_mods: list[dict]) -> None:
    """Synthesize missing module names in-place from description + parent chain.

    Newer IOS-XE (e.g. 17.15 on C9300X) reports entPhysicalName as null for every
    entity and moves the text to entPhysicalDescr. Downstream logic routes by name
    ("Switch N" extraction, module bay names, SFP→interface mapping), so rebuild
    names the way older images reported them:

      chassis  → "Switch {pos+1}"            (Netdisco pos is 0-indexed)
      port     → parent container description minus " Container"
                 ("Twe1/1/2 Container" → "Twe1/1/2", the SFP's interface)
      other    → description, prefixed with "Switch {N} " from the owning
                 chassis when the description doesn't already carry it
    """
    by_index = {m["index"]: m for m in nd_mods if m.get("index") is not None}

    def owning_switch(m: dict) -> Optional[int]:
        cur, hops = m, 0
        while cur is not None and hops < 20:
            if cur.get("class") == "chassis":
                pos = cur.get("pos")
                return pos + 1 if pos is not None else None
            cur = by_index.get(cur.get("parent"))
            hops += 1
        return None

    for m in nd_mods:
        if m.get("name"):
            continue
        descr = (m.get("description") or "").strip()
        cls = m.get("class")
        if cls == "chassis":
            pos = m.get("pos")
            m["name"] = f"Switch {pos + 1}" if pos is not None else descr
        elif cls == "port":
            parent_descr = ((by_index.get(m.get("parent")) or {}).get("description") or "").strip()
            m["name"] = parent_descr[: -len(" Container")] if parent_descr.endswith(" Container") else descr
        elif descr and not re.search(r"\bswitch\s*\d", descr, re.IGNORECASE):
            sw = owning_switch(m)
            m["name"] = f"Switch {sw} {descr}" if sw is not None else descr
        else:
            m["name"] = descr


def _reraise_if_gateway_error(exc: requests.HTTPError) -> None:
    """Re-raise 502/503/504 so server.py's retry/circuit-breaker logic can fire."""
    resp = getattr(exc, "response", None)
    if resp is not None and resp.status_code in (502, 503, 504):
        raise exc


def _slot_from_iface(topo: str, name: str) -> Optional[int]:
    """
    Extract module slot key from interface name for stack/fex topologies.

    Stack:  GigabitEthernet2/0/1  → 2   (first number = stack member, 1-indexed)
    FEX:    Ethernet101/1/1        → 101 (first number ≥ 100 = FEX ID)
    """
    m = re.match(r"[A-Za-z]+(\d+)/\d+/\d+", name)
    if not m:
        return None
    slot = int(m.group(1))
    if topo == "fex":
        return slot if slot >= 100 else None
    if topo in ("stack", "vss"):
        return slot
    return None


def clean_mac(raw: Optional[str]) -> Optional[str]:
    """Return uppercased MAC or None if missing, zero, or otherwise invalid."""
    if not raw:
        return None
    mac = raw.upper().replace("-", ":")
    if mac == NULL_MAC.upper():
        return None
    return mac

def _resolve_neighbor(
    nb: "NetboxClient", remote_ip: str, remote_port: str
) -> tuple[Optional[int], Optional[int]]:
    """
    Resolve a (remote_ip, remote_port) pair to (device_id, interface_id).

    Strategy:
    1. Find device by primary_ip4 match (common case).
    2. Fall back to any IP address assignment (covers VLAN/loopback IPs on L3 devices).
    Once the device is found, look up the interface by name.
    Either value may be None independently.
    """
    log = logging.getLogger("discobox.sync")
    if not remote_ip:
        return None, None

    def _iface_on_device(device_id: int) -> Optional[int]:
        if not remote_port:
            return None
        ifaces = list(nb.nb.dcim.interfaces.filter(device_id=device_id, name=remote_port))
        log.debug("    ifaces_found=%d", len(ifaces))
        return ifaces[0].id if ifaces else None

    try:
        # Pass 1: match by primary IP
        devs = list(nb.nb.dcim.devices.filter(q=remote_ip))
        log.debug("  neighbor resolve  ip=%s port=%s  primary_candidates=%d", remote_ip, remote_port, len(devs))
        for dev in devs:
            p4 = dev.primary_ip4
            p4_addr = str(p4).split("/")[0] if p4 else None
            log.debug("    dev=%s primary_ip4=%s match=%s", dev.name, p4_addr, p4_addr == remote_ip)
            if p4_addr == remote_ip:
                return dev.id, _iface_on_device(dev.id)

        # Pass 2: find by any IP address assignment (e.g. VLAN interface on L3 switch)
        ip_objs = list(nb.nb.ipam.ip_addresses.filter(q=remote_ip))
        log.debug("  neighbor resolve  ip=%s  ip_objects=%d (fallback)", remote_ip, len(ip_objs))
        for ip_obj in ip_objs:
            if str(ip_obj).split("/")[0] != remote_ip:
                continue
            if getattr(ip_obj, "assigned_object_type", None) != "dcim.interface":
                continue
            assigned = ip_obj.assigned_object
            if not assigned:
                continue
            device_id = getattr(getattr(assigned, "device", None), "id", None)
            if not device_id:
                continue
            log.debug("    fallback dev=%s via ip-object", getattr(assigned, "device", "?"))
            return device_id, _iface_on_device(device_id)

        return None, None
    except Exception as exc:
        log.debug("  neighbor resolve  ip=%s port=%s  error: %s", remote_ip, remote_port, exc)
        return None, None


def port_to_netbox(
    port: dict,
    lldp_clear_stale: bool = False,
    neighbor_device_id: Optional[int] = None,
    neighbor_iface_id: Optional[int] = None,
    cf_neighbor_text: Optional[str] = "neighbor",
    cf_neighbor_port: Optional[str] = "neighbor_port",
    cf_neighbor_device: Optional[str] = "neighbor_device",
    cf_neighbor_iface: Optional[str] = "neighbor_iface",
) -> dict:
    """
    Map a Netdisco port dict to Netbox dcim.interfaces fields.

    Netdisco field notes:
      port  = ifDescr  (full interface name, e.g. "FastEthernet0/4")
      name  = ifAlias  (configured description, e.g. "Fa0/4" or a custom label)
      descr = ifDescr  (same as port in practice)
      up_admin          "up" / "down"  →  Netbox enabled (bool)
      speed             "10 Mbps"      →  Netbox speed in kbps
      duplex            operational duplex; may be NULL → fall back to duplex_admin
    """
    speed_kbps = parse_speed_kbps(port.get("speed"))

    raw_duplex = port.get("duplex") or port.get("duplex_admin")
    duplex = raw_duplex if raw_duplex in ("full", "half", "auto") else None

    # Use ifAlias as description only when it differs from the interface name
    full_name = port.get("port") or port.get("descr") or ""
    alias = port.get("name", "")
    description = alias if alias and alias.lower() != full_name.lower() else ""

    data: dict = {
        "name":        full_name,
        # default=None: unknown type must not overwrite the existing Netbox type
        # (upsert_interface skips None values when diffing)
        "type":        map_iftype(port.get("type"), full_name, default=None),
        "enabled":     (port.get("up_admin") or "").lower() == "up",
        "mtu":         port.get("mtu") or None,
        "mac_address": clean_mac(port.get("mac")),
        "speed":       speed_kbps,
        "duplex":      duplex,
        "description": description,
    }

    nb_cf: dict = {}
    if port.get("remote_ip"):
        # Port has a neighbor: write all configured fields; object fields may be None (unresolved)
        if cf_neighbor_text and port.get("remote_id"):
            nb_cf[cf_neighbor_text] = port["remote_id"]
        if cf_neighbor_port and port.get("remote_port"):
            nb_cf[cf_neighbor_port] = port["remote_port"]
        if cf_neighbor_device:
            nb_cf[cf_neighbor_device] = neighbor_device_id
        if cf_neighbor_iface:
            nb_cf[cf_neighbor_iface] = neighbor_iface_id
    if nb_cf:
        data["custom_fields"] = nb_cf
    elif lldp_clear_stale:
        # No neighbor on this port: clear all stale neighbor fields
        stale = {k: None for k in [cf_neighbor_text, cf_neighbor_port, cf_neighbor_device, cf_neighbor_iface] if k}
        if stale:
            data["custom_fields"] = stale

    return data


# ── Sync logic ─────────────────────────────────────────────────────────────────

def _handle_vip_device(
    nb: "NetboxClient",
    vip_dev: "pynetbox.core.response.Record",
    vip_mode: str,
    log: logging.Logger,
    active_device: "pynetbox.core.response.Record | None" = None,
) -> None:
    """
    Handle a VIP/cluster placeholder device according to vip_mode:

    threenode     : keep the device as a VC member; mirror device_type from the
                     active node; set role=vip on its primary_ip4.
    soft          : clear primary_ip4 and unassign all IPs from the device's
                     interfaces so they can be claimed by the physical nodes on
                     the next IP sync. Device record is kept.
    hard          : delete the device entirely (implies soft cleanup first).
    off           : do nothing.
    """
    if vip_mode == "off":
        return
    if vip_mode == "threenode":
        try:
            patch: dict = {}
            # Mirror device_type from the active physical node
            if active_device:
                active_dt_id = nb._nb_value(getattr(active_device, "device_type", None))
                vip_dt_id   = nb._nb_value(getattr(vip_dev,       "device_type", None))
                if active_dt_id and active_dt_id != vip_dt_id:
                    patch["device_type"] = active_dt_id
            if patch:
                vip_dev.update(patch)
                log.info("VIP device %r: device_type updated", vip_dev.name)
            # Set role=vip on primary IP
            primary = getattr(vip_dev, "primary_ip4", None)
            if primary:
                ip_obj = nb.nb.ipam.ip_addresses.get(primary.id)
                if ip_obj and getattr(ip_obj, "role", None) != "vip":
                    ip_obj.update({"role": "vip"})
                    log.info("VIP device %r: primary IP %s role set to vip", vip_dev.name, ip_obj.address)
        except Exception as exc:
            log.error("VIP device %r: could not update in threenode mode: %s", vip_dev.name, exc)
        return
    try:
        if getattr(vip_dev, "primary_ip4", None):
            vip_dev.update({"primary_ip4": None})
        for iface in nb.nb.dcim.interfaces.filter(device_id=vip_dev.id):
            for ip in nb.nb.ipam.ip_addresses.filter(
                assigned_object_type="dcim.interface",
                assigned_object_id=iface.id,
            ):
                ip.update({
                    "assigned_object_type": None,
                    "assigned_object_id": None,
                    "role": "vip",
                })
                log.info("VIP device %r: unassigned IP %s (role=vip)", vip_dev.name, ip.address)
        if vip_mode == "hard":
            nb.nb.dcim.devices.get(vip_dev.id).delete()
            log.info("VIP device %r deleted (hard mode)", vip_dev.name)
        else:
            log.info("VIP device %r: IPs freed (soft mode)", vip_dev.name)
    except Exception as exc:
        log.error("VIP device %r handling error: %s", vip_dev.name, exc)


_TIMEOUT_UNITS = {"us": 1, "ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000}

def _parse_snmp_timeout_us(value: Optional[str]) -> Optional[int]:
    """Parse a Netbox snmp_polling_timeout string (e.g. '3m', '30s') to microseconds."""
    if not value:
        return None
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(us|ms|s|m|h)?", value.strip(), re.IGNORECASE)
    if not m:
        return None
    amount, unit = float(m.group(1)), (m.group(2) or "s").lower()
    return int(amount * _TIMEOUT_UNITS[unit])


def _slugify(text: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _resolve_location_chain(
    nb: "NetboxClient", fallback_site_id: int, location_str: str
) -> tuple[int, Optional[int]]:
    """
    Parse a comma-separated SNMP location string.

    If the first part matches an existing NetBox site slug, that site is used
    and the remaining parts become the location chain.  Otherwise all parts are
    treated as locations under fallback_site_id.

    Returns (site_id, deepest_location_id).  deepest_location_id is None when
    no location parts remain or all fail.
    """
    log = logging.getLogger("discobox.reconcile")
    SKIP = {"", "unknown", "-", "n/a", "na", "tbd"}
    parts = [p.strip() for p in location_str.split(",")]
    parts = [p for p in parts if p.lower() not in SKIP]
    if not parts:
        return fallback_site_id, None

    site_slug = _slugify(parts[0])
    matched_sites = list(nb.nb.dcim.sites.filter(slug=site_slug))
    if matched_sites:
        site_id = matched_sites[0].id
        log.debug("SNMP location: site %r (id=%s)", matched_sites[0].name, site_id)
    else:
        log.warning("SNMP location: site slug %r not found, falling back to configured site", site_slug)
        site_id = fallback_site_id
    parts = parts[1:]

    parent_id: Optional[int] = None
    location_id: Optional[int] = None

    for part in parts:
        slug = _slugify(part)
        filter_kwargs: dict = {"site_id": site_id, "slug": slug}
        if parent_id is not None:
            filter_kwargs["parent_id"] = parent_id
        else:
            filter_kwargs["parent_id__isnull"] = True

        existing = list(nb.nb.dcim.locations.filter(**filter_kwargs))
        if existing:
            loc = existing[0]
        else:
            create_kwargs: dict = {"name": part, "slug": slug, "site": site_id}
            if parent_id is not None:
                create_kwargs["parent"] = parent_id
            try:
                loc = nb.nb.dcim.locations.create(**create_kwargs)
                log.debug("Auto-create location: %r (slug=%s, parent_id=%s)", part, slug, parent_id)
            except Exception as exc:
                log.warning("Auto-create location %r failed: %s — stopping chain", part, exc)
                break

        parent_id = loc.id
        location_id = loc.id

    return site_id, location_id


def _create_device_from_nd(
    nb: "NetboxClient",
    nd: "NetdiscoClient",
    ip: str,
    nd_device: dict,
    role_slug: str,
    site_slug: str,
    status: str = "active",
    auto_create_location: bool = False,
    iface_source_cf: Optional[str] = None,
    iface_source_value: str = "netdisco",
) -> bool:
    """
    Bootstrap a new NetBox device from Netdisco discovery data.
    Creates the device, its management interface, and primary IP.
    Returns True on success (even if IP assignment partially fails).
    """
    log = logging.getLogger("discobox.reconcile")
    hostname = nd_device.get("name") or nd_device.get("dns") or ip

    roles = list(nb.nb.dcim.device_roles.filter(slug=role_slug))
    if not roles:
        log.error("Auto-create %s: role slug %r not found in NetBox", ip, role_slug)
        return False
    sites = list(nb.nb.dcim.sites.filter(slug=site_slug))
    if not sites:
        log.error("Auto-create %s: site slug %r not found in NetBox", ip, site_slug)
        return False

    try:
        nd_mods = nd.get_modules(ip)
    except Exception as exc:
        log.warning("Auto-create %s: could not fetch modules: %s: using description as model", ip, exc)
        nd_mods = []
    chassis = [m for m in nd_mods if m.get("class") == "chassis" and m.get("model")]
    primary_ch = chassis[0] if chassis else {}
    vendor_name = vendor_from_chassis(primary_ch) if primary_ch else None
    vendor_name = vendor_name or nd_device.get("vendor") or None
    model = primary_ch.get("model") or nd_device.get("model") or nd_device.get("description") or "Unknown"
    serial = primary_ch.get("serial") or nd_device.get("serial") or ""

    try:
        mfr = nb.get_or_create_manufacturer(vendor_name or "Unknown")
        device_type = nb.get_or_create_device_type(mfr, model)
    except Exception as exc:
        log.error("Auto-create %s: device type %r/%r failed: %s", ip, vendor_name, model, exc)
        return False

    resolved_site_id = sites[0].id
    location_id: Optional[int] = None
    if auto_create_location:
        nd_location = nd_device.get("location") or ""
        if nd_location:
            resolved_site_id, location_id = _resolve_location_chain(nb, sites[0].id, nd_location)

    existing = list(nb.nb.dcim.devices.filter(name=hostname, site_id=resolved_site_id))
    if existing:
        log.debug("Auto-create %s: device %r already exists, skipping", ip, hostname)
        nb_device = existing[0]
    else:
        try:
            create_kwargs: dict = dict(
                name=hostname,
                device_type=device_type.id,
                role=roles[0].id,
                site=resolved_site_id,
                status=status,
            )
            if serial:
                create_kwargs["serial"] = serial
            if location_id is not None:
                create_kwargs["location"] = location_id
            if iface_source_cf:
                create_kwargs["custom_fields"] = {iface_source_cf: iface_source_value}
            nb_device = nb.nb.dcim.devices.create(**create_kwargs)
        except Exception as exc:
            log.error("Auto-create %s: device create failed: %s", ip, exc)
            return False

    log.info("Auto-create: created %r (%s)  type=%s/%s", hostname, ip, vendor_name or "?", model)

    # Find management interface name and address from device_ips
    try:
        nd_ips_list = nd.get_device_ips(ip)
    except Exception as exc:
        log.warning("Auto-create %s: could not fetch device_ips: %s", ip, exc)
        nd_ips_list = []

    mgmt_entry = next(
        (e for e in nd_ips_list if (e.get("ip") or "").split("/")[0] == ip),
        None,
    )
    port_name = (mgmt_entry or {}).get("port") or "mgmt0"
    subnet = (mgmt_entry or {}).get("subnet")
    address = ip
    if "/" not in address and subnet:
        try:
            prefix = ipaddress.ip_network(subnet, strict=False).prefixlen
            address = f"{ip}/{prefix}"
        except ValueError:
            address = f"{ip}/32"

    try:
        iface_kwargs: dict = dict(device=nb_device.id, name=port_name, type="1000base-t")
        iface = nb.nb.dcim.interfaces.create(**iface_kwargs)
        if iface_source_cf:
            iface.update({"custom_fields": {iface_source_cf: iface_source_value}})
    except Exception as exc:
        log.error("Auto-create %s: interface %r create failed: %s", ip, port_name, exc)
        return True

    try:
        nb.upsert_ip(address, iface)
        ip_objs = list(nb.nb.ipam.ip_addresses.filter(address=ip))
        if ip_objs:
            nb_device.update({"primary_ip4": ip_objs[0].id})
            log.info("Auto-create: IP %s → %r set as primary on %r", address, port_name, hostname)
    except Exception as exc:
        log.error("Auto-create %s: IP assignment failed: %s", ip, exc)

    return True


def reconcile_devices(
    nd: "NetdiscoClient",
    nb: "NetboxClient",
    max_queued: Optional[int] = 500,
    max_failed: Optional[int] = 500,
    max_enqueue: Optional[int] = None,
    offset: Optional[int] = None,
    roles: Optional[list] = None,  # empty/None = all roles
    require_auth_tag: bool = False,
    auto_create_role: Optional[str] = None,
    auto_create_site: Optional[str] = None,
    auto_create_status: str = "active",
    auto_create_location: bool = False,
    iface_source_cf: Optional[str] = None,
    iface_source_value: str = "netdisco",
) -> dict:
    """
    Compare Netbox active devices against Netdisco and enqueue discovery
    for any device present in Netbox but missing from Netdisco.

    If auto_create_role and auto_create_site are set, also creates NetBox
    devices for any Netdisco device not yet present in NetBox.

    If the Netdisco queue exceeds max_queued or max_failed (last 1h), the
    enqueue step is skipped to avoid overloading Netdisco.

    max_enqueue=0 disables enqueueing entirely (no discovery jobs are ever
    submitted) while still running the full not_in_netdisco/not_in_netbox
    gap scan — use this to monitor drift without triggering discovery.

    Returns counts: enqueued / skipped (no primary IP) / already_known /
                    netbox_total / netdisco_total / aborted (bool).
    """
    log = logging.getLogger("discobox.reconcile")
    _aborted = {"aborted": True, "enqueued": 0, "skipped": 0, "already_known": 0, "netbox_total": 0, "netdisco_total": 0}

    if max_queued is not None or max_failed is not None:
        try:
            qs = nd.get_queue_status(since="1h")
        except requests.HTTPError as exc:
            log.error("Reconcile aborted: could not fetch Netdisco queue status: %s", exc)
            return _aborted
        queued, failed = qs.get("queued", 0), qs.get("failed", 0)
        if (max_queued is not None and queued > max_queued) or \
           (max_failed is not None and failed > max_failed):
            log.warning(
                "Reconcile aborted: queue too busy (queued=%d failed=%d, limits queued=%s failed=%s)",
                queued, failed, max_queued, max_failed,
            )
            return _aborted

    try:
        nd_all_devices = nd.get_all_devices()
        nd_ips = {d["ip"] for d in nd_all_devices if d.get("ip")}
    except requests.HTTPError as exc:
        log.error("Reconcile aborted: could not fetch Netdisco devices: %s", exc)
        return _aborted
    log.info("Netdisco knows %d devices", len(nd_ips))

    role_filter = {"role": roles} if roles else {}
    nb_total = nb.nb.dcim.devices.count(status="active", has_primary_ip=True, **role_filter)
    counts = {"enqueued": 0, "skipped": 0, "already_known": 0, "netdisco_total": len(nd_ips), "netbox_total": nb_total}
    if roles:
        log.info("Reconcile role filter: %s", roles)

    not_in_netdisco: list[dict] = []
    enqueue_cap_logged = False
    for device in nb.nb.dcim.devices.filter(status="active", has_primary_ip=True, **role_filter):
        primary = device.primary_ip4
        if not primary:
            counts["skipped"] += 1
            continue
        ip = str(primary).split("/")[0]

        if ip in nd_ips:
            counts["already_known"] += 1
            continue

        not_in_netdisco.append({"ip": ip, "name": device.name})

        cf = getattr(device, "custom_fields", {}) or {}
        device_auth_tag_hint = cf.get("snmp_auth_profile") or None
        if require_auth_tag and not device_auth_tag_hint:
            log.debug("Skipping %s (%s): no snmp_auth_profile set", ip, device.name)
            counts["skipped"] += 1
            continue
        snmp_timeout_us = _parse_snmp_timeout_us(cf.get("snmp_polling_timeout"))

        if offset and counts["enqueued"] + counts.get("offset_skipped", 0) < offset:
            counts["offset_skipped"] = counts.get("offset_skipped", 0) + 1
            continue

        if max_enqueue is not None and counts["enqueued"] >= max_enqueue:
            if not enqueue_cap_logged:
                enqueue_cap_logged = True
                log.info(
                    "Enqueue cap reached (%d): skipping further enqueues this run "
                    "(gap scan continues)", max_enqueue,
                )
            continue

        try:
            nd.enqueue_discover(ip, device_auth_tag_hint=device_auth_tag_hint, snmp_timeout_us=snmp_timeout_us)
            effective_timeout = snmp_timeout_us if snmp_timeout_us is not None else 3_000_000
            log.info("Enqueued discover for %s (%s) device_auth_tag_hint=%r timeout=%dus",
                     ip, device.name, device_auth_tag_hint, effective_timeout)
            counts["enqueued"] += 1
        except Exception as exc:
            log.error("Failed to enqueue discover for %s: %s", ip, exc)

    # Build full nb_all_ips across all statuses/roles for gap reporting and auto-create.
    # Deliberately NOT role_filter-scoped: the enqueue loop above only targets
    # reconcile's target roles, but a device under any other role still counts
    # as "in Netbox" for gap-reporting purposes.
    nb_all_ips = {
        str(d.primary_ip4).split("/")[0]
        for d in nb.nb.dcim.devices.filter(primary_ip4__isnull=False)
        if d.primary_ip4
    }
    not_in_netbox: list[dict] = [
        {"ip": d["ip"], "name": d.get("name") or d.get("dns") or d["ip"]}
        for d in nd_all_devices
        if d.get("ip") and d["ip"] not in nb_all_ips
    ]
    counts["not_in_netdisco"] = len(not_in_netdisco)
    counts["not_in_netbox"] = len(not_in_netbox)
    if not_in_netdisco or not_in_netbox:
        log.info("Gaps: not_in_netdisco=%d  not_in_netbox=%d", len(not_in_netdisco), len(not_in_netbox))

    # Auto-create: bootstrap NetBox devices for Netdisco entries not yet in NetBox
    if auto_create_role and auto_create_site:
        create_counts: dict[str, int] = {"created": 0, "failed": 0}
        for nd_dev in nd_all_devices:
            nd_ip = nd_dev.get("ip")
            if not nd_ip or nd_ip in nb_all_ips:
                continue
            try:
                nd_device_full = nd.get_device(nd_ip)
            except Exception as exc:
                log.error("Auto-create %s: could not fetch device info: %s", nd_ip, exc)
                create_counts["failed"] += 1
                continue
            ok = _create_device_from_nd(
                nb, nd, nd_ip, nd_device_full,
                role_slug=auto_create_role,
                site_slug=auto_create_site,
                status=auto_create_status,
                auto_create_location=auto_create_location,
                iface_source_cf=iface_source_cf,
                iface_source_value=iface_source_value,
            )
            create_counts["created" if ok else "failed"] += 1
        if any(create_counts.values()):
            log.info("Auto-create: %s", "  ".join(f"{k}: {v}" for k, v in create_counts.items() if v))
        counts.update(create_counts)

    log.info("Reconcile done: %s", counts)
    counts["not_in_netdisco_list"] = not_in_netdisco
    counts["not_in_netbox_list"] = not_in_netbox
    return counts


def sync_device(
    ip: str,
    nd: NetdiscoClient,
    nb: NetboxClient,
    sync_mac: bool = True,
    sync_ip: bool = True,
    sync_modules: bool = True,
    sync_sfp: bool = True,
    sync_poe: bool = True,
    housekeeping: bool = False,
    lldp_clear_stale: bool = False,
    vip_mode: str = "threenode",   # threenode | soft | hard | off
    cf_neighbor_text: Optional[str] = "neighbor",
    cf_neighbor_port: Optional[str] = "neighbor_port",
    cf_neighbor_device: Optional[str] = "neighbor_device",
    cf_neighbor_iface: Optional[str] = "neighbor_iface",
    cable_scope: str = "",          # "" = disabled, "site" = same-site only
    cable_source_cf: Optional[str] = "source",
    cable_source_value: Optional[str] = "netdisco",
    iface_source_cf: Optional[str] = "source",
    iface_source_value: str = "netdisco",
    cf_os_version: Optional[str] = "os_version",
    cf_os_name: Optional[str] = "os_name",
    cf_os_release: Optional[str] = "os_release",
) -> dict:
    """
    Sync device fields, interfaces, MACs, IPs, modules, and SFPs.

    Returns a dict with:
      ok          bool  : True if no errors occurred
      interfaces  dict  : created/updated/unchanged/error counts
      ips         dict  : created/fixed/moved/unchanged/skipped/error counts
      modules     dict  : created/updated/unchanged/error counts
    """
    log = logging.getLogger(f"discobox.{ip}")

    try:
        nd_device = nd.get_device(ip)
        nd_ports = nd.get_ports(ip)
    except requests.HTTPError as exc:
        _reraise_if_gateway_error(exc)
        log.error("Netdisco request failed: %s", exc)
        return {"ok": False, "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    nd_hostname = nd_device.get("name") or nd_device.get("dns") or ""
    nd_serial = nd_device.get("serial") or ""
    log.info("sync start %s", nd_hostname or ip)
    log.debug("Netdisco  hostname=%r  ports=%d", nd_hostname, len(nd_ports))

    nb_device = nb.find_device_by_ip(ip, hostname=nd_hostname, serial=nd_serial)
    if not nb_device:
        log.error("No Netbox device found for IP %s or hostname %r: skipping", ip, nd_hostname)
        return {"ok": False, "reason": "device_not_found", "hostname": nd_hostname,
                "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    logger.debug("Netbox    device=%r  id=%s", nb_device.name, nb_device.id)

    # ── HA / VIP detection ────────────────────────────────────────────────────────
    # Signal: SNMP hostname belongs to a different Netbox device than the IP lookup found.
    # This happens when the management IP is a cluster VIP. The serial approach alone
    # is unreliable because previous syncs may have written the active node's serial
    # onto the VIP device. Hostname mismatch is the primary signal.
    vip_device = None
    if nd_hostname and nb_device.name:
        nd_short = nd_hostname.lower().split(".")[0]
        nb_short = nb_device.name.lower().split(".")[0]
        if nd_short != nb_short:
            # Try to find the real physical device by SNMP hostname
            real_device = None
            # 1. Exact match, then contains with short-name verification
            for dev in nb.nb.dcim.devices.filter(name__ie=nd_hostname):
                real_device = dev
                break
            if not real_device:
                for dev in nb.nb.dcim.devices.filter(name__ic=nd_short):
                    if dev.name and dev.name.lower().split(".")[0] == nd_short:
                        real_device = dev
                        break
            # 2. Serial fallback
            if not real_device and nd_serial:
                serial_match = nb.find_device_by_serial(nd_serial)
                if serial_match and serial_match.id != nb_device.id:
                    real_device = serial_match

            if real_device and real_device.id != nb_device.id:
                log.info(
                    "HA VIP detected: %r → redirecting to %r",
                    nb_device.name, real_device.name,
                )
                vip_device = nb_device
                nb_device = real_device

                # Find partner node by swapping node indicator in hostname
                partner_dev = nb.find_ha_partner(nb_device.name)

                # Create / update Virtual Chassis using the VIP device's short name as identity
                vc_name = vip_device.name.split(".")[0]
                active_m = re.search(r"p(\d+)h", nb_device.name, re.IGNORECASE)
                active_pos = int(active_m.group(1)) if active_m else 1
                vc_members: list[tuple] = [(nb_device, active_pos)]
                if partner_dev:
                    partner_m = re.search(r"p(\d+)h", partner_dev.name, re.IGNORECASE)
                    partner_pos = int(partner_m.group(1)) if partner_m else (2 if active_pos == 1 else 1)
                    vc_members.append((partner_dev, partner_pos))
                    log.info("HA partner found: %r", partner_dev.name)
                else:
                    log.warning("HA partner not found for %r: VC will have one member", nb_device.name)
                if vip_mode == "threenode":
                    vc_members.append((vip_device, 0))
                    log.info("HA VIP device %r added as VC member pos=0", vip_device.name)
                try:
                    vc_action, _ = nb.upsert_virtual_chassis(vc_name, vc_members)
                    log.info("HA VirtualChassis %r: %s", vc_name, vc_action)
                except Exception as exc:
                    log.error("HA VirtualChassis error: %s", exc)

                _handle_vip_device(nb, vip_device, vip_mode, log, active_device=nb_device)
            else:
                log.warning(
                    "Hostname mismatch for %s: Netdisco=%r  Netbox=%r",
                    ip, nd_hostname, nb_device.name,
                )

    # ── HA peer detection (no VIP) ────────────────────────────────────────────────
    # Signal: device hostname contains an HA node indicator (p1h/p2h, node1/node2, -1/-2)
    # and a partner device exists in Netbox at the swapped hostname.
    # Handles pairs where Netdisco hooks each physical node directly with no shared VIP.
    if not vip_device:
        short = nb_device.name.split(".")[0]
        ha_info = _ha_node_info(short)
        if ha_info:
            partner_dev = nb.find_ha_partner(nb_device.name)
            if partner_dev:
                node_num, vc_base, vip_short = ha_info
                partner_num = 2 if node_num == 1 else 1
                vc_members = [(nb_device, node_num), (partner_dev, partner_num)]

                # Find VIP device (e.g. p0h) by replacing node number with 0
                found_vip_dev = None
                domain = nb_device.name[len(short):]
                for candidate in (vip_short + domain, vip_short):
                    vip_results = list(nb.nb.dcim.devices.filter(name__ie=candidate))
                    if vip_results:
                        found_vip_dev = vip_results[0]
                        break

                # VC identity: prefer the VIP's short name so this path and the
                # VIP-hook path (which names the VC after the VIP device) agree
                # on the same VC — otherwise each path creates its own VC and
                # fights over the members. Fall back to the short hostname with
                # the node indicator stripped when no VIP device exists.
                vc_name = found_vip_dev.name.split(".")[0] if found_vip_dev else vc_base

                if vip_mode == "threenode" and found_vip_dev:
                    vc_members.append((found_vip_dev, 0))
                    log.info("HA VIP device %r added as VC member pos=0", found_vip_dev.name)

                try:
                    vc_action, _ = nb.upsert_virtual_chassis(vc_name, vc_members)
                    log.info(
                        "HA peer VirtualChassis %r: %s  (topology=standalone per Netdisco; VC is Netbox-side)",
                        vc_name, vc_action,
                    )
                except Exception as exc:
                    log.error("HA peer VirtualChassis error: %s", exc)

                if found_vip_dev:
                    _handle_vip_device(nb, found_vip_dev, vip_mode, log, active_device=nb_device)

    nb.update_device_fields(nb_device, nd_device, log=log,
                            cf_os_version=cf_os_version, cf_os_name=cf_os_name, cf_os_release=cf_os_release)

    if housekeeping:
        deleted_bays = nb.remove_stale_device_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        deleted_mod_bays = nb.remove_stale_module_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        nd_port_names = {
            p.get("port") or p.get("descr") for p in nd_ports
            if p.get("port") or p.get("descr")
        }
        deleted_ifaces = nb.remove_empty_dummy_interfaces(nb_device, DUMMY_INTERFACES, nd_port_names)
        log.debug(
            "Housekeeping: deleted %d stale device bay(s), %d stale module bay(s), %d empty dummy interface(s)",
            deleted_bays, deleted_mod_bays, deleted_ifaces,
        )

    counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "skipped": 0, "error": 0}
    ip_counts: dict[str, int] = {"created": 0, "fixed": 0, "moved": 0, "unchanged": 0, "skipped": 0, "error": 0}
    mod_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    sfp_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}

    # slot_to_module populated during module sync; consumed by interface→module pass.
    slot_to_module: dict[int, int] = {}  # slot key (stack pos / FEX ID) → nb module id
    # slot_to_device populated for VSS; used to route blades to the correct member device.
    slot_to_device: dict[int, object] = {}  # VSS pos → nb device record
    topo = "standalone"                   # updated inside sync_modules block

    # ── Modules (before interfaces so bays exist when interfaces are assigned) ────

    if sync_modules:
        try:
            nd_mods = nd.get_modules(ip)
        except requests.HTTPError as exc:
            _reraise_if_gateway_error(exc)
            log.error("Could not fetch modules from Netdisco: %s", exc)
            nd_mods = []
        _fill_module_names(nd_mods)

        chassis = [m for m in nd_mods if m.get("class") == "chassis" and m.get("model")]
        stack_root = next((m for m in nd_mods if m.get("class") == "stack"), None)
        has_stack = stack_root is not None
        # Nexus FEX topology: stack root is a logical fabric, not a real member stack.
        # The primary N9K chassis + satellite FEX units all appear as chassis entries.
        is_fex = has_stack and ((stack_root.get("type") or "").lower() == "cevcontainernexuslogicalfabric")
        is_vss = has_stack and (
            "virtualstack" in (stack_root.get("type") or "").lower()
            or "virtual stack" in (stack_root.get("name") or "").lower()
        )
        is_standalone = not has_stack and len(chassis) == 1

        # Log tree
        root = next((m for m in nd_mods if not m.get("parent")), None)
        if root:
            log.debug("  %s (root)  %r  type=%s  model=%s  serial=%s",
                        root.get("class", "?"), root.get("name", ""),
                        root.get("type", ""), root.get("model", ""), root.get("serial", ""))
        for i, ch in enumerate(chassis):
            prefix = "└──" if i == len(chassis) - 1 else "├──"
            log.debug("  %s chassis  %r  model=%s  serial=%s",
                        prefix, ch.get("name", ""), ch.get("model", ""), ch.get("serial", ""))
        topo = "fex" if is_fex else ("vss" if is_vss else ("standalone" if is_standalone else "stack"))
        log.debug("Modules   chassis=%d  topology=%s", len(chassis), topo)

        manufacturer = nb_device.device_type.manufacturer

        def _update_device_type(ch: dict) -> None:
            """Update DeviceType (and serial) on nb_device from a chassis entry."""
            part_number = ch.get("model", "") or ""
            serial = ch.get("serial") or ""
            # Skip device type update when model looks like a raw OID fragment
            # (e.g. ".112.100.1003"): keep whatever is already set in Netbox.
            model = parse_sw_model(ch.get("sw_ver", "")) or part_number
            if not model or model.startswith("."):
                if serial and (nb_device.serial or "") != serial:
                    nb_device.update({"serial": serial})
                    log.debug("  serial=%s updated (no valid model from Netdisco)", serial)
                else:
                    log.debug("  DeviceType skipped: no valid model from Netdisco")
                mod_counts["unchanged"] += 1
                return
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            device_type = nb.get_or_create_device_type(mfr, model, part_number=part_number)
            patch = {}
            if nb_device.device_type.id != device_type.id:
                patch["device_type"] = device_type.id
            if serial and (nb_device.serial or "") != serial:
                patch["serial"] = serial
            if patch:
                nb_device.update(patch)
                # pynetbox replaces device_type with a plain int after update: restore the object
                nb_device.device_type = device_type
                log.debug("  DeviceType → %s / %s  serial=%s  updated", mfr.name, model, serial)
                mod_counts["updated"] += 1
            else:
                log.debug("  DeviceType unchanged")
                mod_counts["unchanged"] += 1

        def _upsert_chassis_bay(ch: dict, slot_key: Optional[int] = None) -> None:
            """Create/update a module bay + module for a chassis member."""
            name = ch.get("name") or ""
            model = ch.get("model") or ""
            serial = ch.get("serial") or ""
            position = str(ch.get("pos", ""))
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            module_type = nb.get_or_create_module_type(mfr, model)
            bay = nb.upsert_module_bay(nb_device, name, position)
            action, module = nb.upsert_module(nb_device, bay, module_type, serial)
            mod_counts[action] += 1
            if slot_key is not None and module:
                slot_to_module[slot_key] = module.id
            log.debug("  %s  %s  serial=%s  %s", name, model, serial, action)

        if is_standalone:
            # Single device: update DeviceType on the device itself
            try:
                _update_device_type(chassis[0])
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error: %s", exc)

        elif is_fex:
            # Nexus FEX: primary N9K chassis → DeviceType update; FEX units → module bays
            device_serial = nd_device.get("serial", "")
            primary = next((c for c in chassis if c.get("serial") == device_serial), chassis[0])
            fex_units = [c for c in chassis if c is not primary]
            try:
                _update_device_type(primary)
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error: %s", exc)
            for ch in fex_units:
                # Extract FEX ID from name: "Fex-101 Nexus2332 Chassis" → 101
                fex_match = re.match(r"[Ff]ex-(\d+)", ch.get("name") or "")
                slot_key = int(fex_match.group(1)) if fex_match else None
                try:
                    _upsert_chassis_bay(ch, slot_key)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  %-30s error: %s", ch.get("name", ""), exc)

        elif is_vss:
            # Cat9500 StackWise Virtual: two physical devices in separate Netbox records.
            # Create/update a Virtual Chassis to link them; no module bays on either device.
            device_serial = nd_device.get("serial", "")
            primary_ch = next((c for c in chassis if c.get("serial") == device_serial), chassis[0])
            partner_chs = [c for c in chassis if c is not primary_ch]

            # Update DeviceType for the primary (the device we're syncing right now)
            try:
                _update_device_type(primary_ch)
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error (primary): %s", exc)

            primary_pos = primary_ch.get("pos", 1)
            vc_members: list[tuple] = [(nb_device, primary_pos)]

            for partner_ch in partner_chs:
                partner_serial = partner_ch.get("serial", "")
                partner_pos = partner_ch.get("pos", 2)
                partner_model = partner_ch.get("model", "")

                # 1. Find partner by serial number
                partner_dev = nb.find_device_by_serial(partner_serial) if partner_serial else None

                # 2. Fallback: append "-<pos>" before the domain suffix
                #    e.g. "sw1.example.com" → "sw1-2.example.com"
                if not partner_dev and nd_hostname:
                    parts = nd_hostname.split(".", 1)
                    if len(parts) > 1:
                        partner_hostname = f"{parts[0]}-{partner_pos}.{parts[1]}"
                    else:
                        partner_hostname = f"{nd_hostname}-{partner_pos}"
                    results = list(nb.nb.dcim.devices.filter(name__ie=partner_hostname))
                    if results:
                        partner_dev = results[0]
                        log.info("  VSS partner found by hostname %r", partner_dev.name)

                if not partner_dev:
                    log.warning(
                        "  VSS partner not found (serial=%r): Virtual Chassis will be incomplete",
                        partner_serial,
                    )
                    mod_counts["error"] += 1
                    continue

                # Update DeviceType on the partner device
                try:
                    vendor_name = vendor_from_chassis(partner_ch)
                    mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
                    partner_part_number = partner_model
                    partner_model = parse_sw_model(partner_ch.get("sw_ver", "")) or partner_part_number
                    partner_dt = nb.get_or_create_device_type(mfr, partner_model, part_number=partner_part_number)
                    patch: dict = {}
                    if partner_dev.device_type.id != partner_dt.id:
                        patch["device_type"] = partner_dt.id
                    if partner_serial and (partner_dev.serial or "") != partner_serial:
                        patch["serial"] = partner_serial
                    if patch:
                        partner_dev.update(patch)
                        log.info(
                            "  VSS partner DeviceType → %s  serial=%s  updated",
                            partner_model, partner_serial,
                        )
                        mod_counts["updated"] += 1
                    else:
                        mod_counts["unchanged"] += 1
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  VSS partner DeviceType update error: %s", exc)

                vc_members.append((partner_dev, partner_pos))

            # Create/update Virtual Chassis linking both physical devices
            vc_name = nd_hostname.split(".")[0] if nd_hostname else f"vc-{ip}"
            try:
                vc_action, _vc = nb.upsert_virtual_chassis(vc_name, vc_members)
                log.info("  VirtualChassis %r: %s", vc_name, vc_action)
            except Exception as exc:
                log.error("  VirtualChassis error: %s", exc)

            # Build pos → device map so blades can be routed to the right member
            for _dev, _pos in vc_members:
                slot_to_device[_pos] = _dev

        else:
            # Traditional stack: create a module bay + module per chassis member.
            # Netdisco pos is 0-indexed; Cisco interface names are 1-indexed (Gi1/0/1 = member 1).
            for ch in chassis:
                try:
                    pos = ch.get("pos")
                    _upsert_chassis_bay(ch, slot_key=pos + 1 if pos is not None else None)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  %-30s error: %s", ch.get("name", ""), exc)

        log.debug(
            "Modules: updated=%d unchanged=%d errors=%d",
            mod_counts.get("updated", 0) + mod_counts.get("created", 0),
            mod_counts["unchanged"], mod_counts["error"],
        )

        # Supplement os_version from chassis sw_ver when the device field was empty
        # (e.g. Fortinet: "FortiGate-600F v7.4.8,build2795,250523 (GA.M)" → "7.4.8")
        if cf_os_version and not nd_device.get("os_ver"):
            for ch in chassis:
                ver = parse_sw_ver(ch.get("sw_ver", ""))
                if ver:
                    try:
                        nb_device.update({"custom_fields": {cf_os_version: ver}})
                        log.debug("  OS version from chassis sw_ver: %s", ver)
                    except Exception as exc:
                        log.error("  OS version update error: %s", exc)
                    break

        # Fans: inventory items only when model or serial is present (e.g. Nexus fan trays).
        # C9300-style fans report neither, so they are silently skipped.
        fans = [
            m for m in nd_mods
            if m.get("class") == "fan" and m.get("name") and (m.get("model") or m.get("serial"))
        ]
        if fans:
            log.debug("Fans      entries: %d", len(fans))
            fan_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
            for fan in fans:
                fan_name = fan.get("name", "")
                fan_model = fan.get("model", "")
                fan_serial = fan.get("serial", "")
                fan_target = nb_device
                if slot_to_device:
                    sw_match = re.match(r"Switch\s+(\d+)", fan_name, re.IGNORECASE)
                    if sw_match:
                        fan_target = slot_to_device.get(int(sw_match.group(1)), nb_device)
                try:
                    action = nb.upsert_inventory_item(
                        fan_target, fan_name,
                        manufacturer if fan_model else None,
                        fan_model, fan_serial,
                    )
                    fan_counts[action] += 1
                    if action != "unchanged":
                        log.debug("  Fan  %s  model=%s  serial=%s  %s",
                                  fan_name, fan_model or "-", fan_serial or "-", action)
                    else:
                        log.debug("  Fan  %-35s unchanged", fan_name)
                except Exception as exc:
                    fan_counts["error"] += 1
                    log.error("  Fan  %-35s error: %s", fan_name, exc)
            log.debug(
                "Fans: created=%d updated=%d unchanged=%d errors=%d",
                fan_counts["created"], fan_counts["updated"],
                fan_counts["unchanged"], fan_counts["error"],
            )

        # PSUs: inventory items on the device (skip Unknown type with no model)
        psus = [
            m for m in nd_mods
            if m.get("class") == "powerSupply" and m.get("name")
            and m.get("type") != "cevPowerSupplyUnknown"
        ]
        log.debug("PSUs      entries: %d", len(psus))
        psu_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
        for psu in psus:
            psu_name = psu.get("name", "")
            psu_model = psu.get("model", "")
            psu_serial = psu.get("serial", "")
            # For VSS route to the correct member device via "Switch N" prefix
            psu_target = nb_device
            if slot_to_device:
                sw_match = re.match(r"Switch\s+(\d+)", psu_name, re.IGNORECASE)
                if sw_match:
                    psu_target = slot_to_device.get(int(sw_match.group(1)), nb_device)
            try:
                action = nb.upsert_inventory_item(
                    psu_target, psu_name,
                    manufacturer if psu_model else None,
                    psu_model, psu_serial,
                )
                psu_counts[action] += 1
                if action != "unchanged":
                    log.debug("  PSU %s  model=%s  serial=%s  %s",
                              psu_name, psu_model or "-", psu_serial or "-", action)
                else:
                    log.debug("  PSU %-35s unchanged", psu_name)
            except Exception as exc:
                psu_counts["error"] += 1
                log.error("  PSU %-35s error: %s", psu_name, exc)

        log.debug(
            "PSUs: created=%d updated=%d unchanged=%d errors=%d",
            psu_counts["created"], psu_counts["updated"], psu_counts["unchanged"], psu_counts["error"],
        )

        # Pre-blade orphan pass: fetch existing interfaces and delete any that are not
        # in Netdisco before blade sync fires, so Netbox's template auto-creation doesn't
        # hit duplicate-key errors on stale / wrongly-named interfaces.
        if slot_to_device:
            vss_ifaces: dict[int, dict] = {
                pos: nb.fetch_interfaces(dev.id)
                for pos, dev in slot_to_device.items()
            }
            # Remove interfaces sitting on the wrong VSS member device from a prior run.
            # Subinterfaces first to avoid Netbox cascade-delete causing a 404 on the child.
            for pos, dev_ifaces in vss_ifaces.items():
                misplaced = [
                    (iface_name, iface, owner)
                    for iface_name, iface in dev_ifaces.items()
                    if (
                        (owner := _slot_from_iface("vss", iface_name)) is not None
                        and owner != pos
                        and owner in slot_to_device
                    )
                ]
                misplaced.sort(key=lambda x: (0 if "." in x[0] else 1))
                for iface_name, iface, owner_pos in misplaced:
                    try:
                        iface.delete()
                        del dev_ifaces[iface_name]
                        log.debug("  %-40s moved from Switch %d → Switch %d",
                                  iface_name, pos, owner_pos)
                    except Exception as exc:
                        log.error("  %-40s could not remove from wrong VSS member: %s",
                                  iface_name, exc)
        else:
            vss_ifaces = {}

        existing_ifaces = nb.fetch_interfaces(nb_device.id)

        nd_names = {
            port.get("port") or port.get("descr") for port in nd_ports
            if not (port.get("port") or port.get("descr") or "").lower().startswith(PORT_BLACKLIST_PREFIXES)
        }

        all_existing: dict = {}
        for d in vss_ifaces.values():
            all_existing.update(d)
        all_existing.update(existing_ifaces)
        orphaned_deleted = 0
        orphaned_errors = 0
        for name, iface in list(all_existing.items()):
            if name not in nd_names and not name.lower().startswith(PORT_BLACKLIST_PREFIXES):
                if housekeeping:
                    if iface_source_cf:
                        owner = (dict(getattr(iface, "custom_fields", {}) or {}).get(iface_source_cf) or "")
                        if owner and owner != iface_source_value:
                            log.debug("  %-40s skipping orphan delete: owned by %r", name, owner)
                            continue
                    try:
                        iface.delete()
                        orphaned_deleted += 1
                        log.debug("  %-40s deleted (not in Netdisco)", name)
                        existing_ifaces.pop(name, None)
                        for m in vss_ifaces.values():
                            m.pop(name, None)
                    except Exception as exc:
                        orphaned_errors += 1
                        log.error("  %-40s could not delete orphan: %s", name, exc)
        if housekeeping and (orphaned_deleted or orphaned_errors):
            log.debug("Orphaned interfaces: deleted=%d errors=%d", orphaned_deleted, orphaned_errors)

        # Stack cables: class=other entries with a real model and serial
        # (e.g. STACK-T1-50CM stackwise cables). One inventory item per StackPort entry.
        stack_cables = [
            m for m in nd_mods
            if m.get("class") == "other"
            and m.get("name")
            and m.get("model") and m.get("serial")
        ]
        if stack_cables:
            log.debug("StackCables  entries: %d", len(stack_cables))
            cable_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
            for cable in stack_cables:
                cable_name = cable.get("name") or ""
                cable_model = cable.get("model") or ""
                cable_serial = cable.get("serial") or ""
                # Route to correct VSS member if applicable; for traditional stacks all on nb_device
                cable_target = nb_device
                if slot_to_device:
                    sw_match = re.match(r"Switch\s+(\d+)", cable_name, re.IGNORECASE)
                    if not sw_match:
                        sw_match = re.match(r"\w+?(\d+)/", cable_name)
                    if sw_match:
                        cable_target = slot_to_device.get(int(sw_match.group(1)), nb_device)
                try:
                    action = nb.upsert_inventory_item(
                        cable_target, cable_name, manufacturer, cable_model, cable_serial,
                    )
                    cable_counts[action] += 1
                    if action != "unchanged":
                        log.debug("  Cable %-35s model=%s  serial=%s  %s",
                                  cable_name, cable_model, cable_serial, action)
                    else:
                        log.debug("  Cable %-35s unchanged", cable_name)
                except Exception as exc:
                    cable_counts["error"] += 1
                    log.error("  Cable %-35s error: %s", cable_name, exc)
            log.debug(
                "StackCables: created=%d updated=%d unchanged=%d errors=%d",
                cable_counts["created"], cable_counts["updated"],
                cable_counts["unchanged"], cable_counts["error"],
            )

        # Blades (linecards / supervisors / FRU uplink modules): module bay + module per slot.
        blades = [
            m for m in nd_mods
            if m.get("class") == "module"
            and m.get("name")
            and m.get("model") and m.get("model") != "Unknown PID"
            and m.get("serial")
            and "transceiver" not in (m.get("name") or "").lower()
        ]
        log.debug("Blades    entries: %d", len(blades))
        blade_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
        for blade in blades:
            blade_name = blade.get("name") or ""
            blade_model = blade.get("model") or ""
            blade_serial = blade.get("serial") or ""
            # Extract slot/position and route to the correct device.
            # VSS: each member is a separate Netbox device → route via "Switch N".
            # Stack: single Netbox device, so use the switch number AS the position so
            # each member's uplink module gets a unique slot and the module type
            # template generates distinct interface names (Te2/1/8 for Switch 2, etc.).
            target_device = nb_device
            sw_match = re.match(r"Switch\s+(\d+)", blade_name, re.IGNORECASE)
            if sw_match and slot_to_device:
                # VSS: route blade to the right member device; position within that device
                target_device = slot_to_device.get(int(sw_match.group(1)), nb_device)
                sw_match = None  # fall through to Slot/Module extraction below
            if sw_match:
                # Stack: switch number doubles as the module bay position
                position = sw_match.group(1)
            else:
                m = re.search(r"Slot\s+(\d+)", blade_name, re.IGNORECASE) \
                 or re.search(r"Module\s+(\d+)", blade_name, re.IGNORECASE)
                position = m.group(1) if m else ""
            try:
                vendor_name = vendor_from_chassis(blade)
                mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
                module_type = nb.get_or_create_module_type(mfr, blade_model)
                bay = nb.upsert_module_bay(target_device, blade_name, position)
                action, _ = nb.upsert_module(target_device, bay, module_type, blade_serial)
                blade_counts[action] += 1
                log.debug("  Blade %s  model=%s  serial=%s  %s",
                          blade_name, blade_model, blade_serial, action)
            except Exception as exc:
                blade_counts["error"] += 1
                log.error("  Blade %-35s error: %s", blade_name, exc)

        log.debug(
            "Blades: created=%d updated=%d unchanged=%d errors=%d",
            blade_counts["created"], blade_counts["updated"], blade_counts["unchanged"], blade_counts["error"],
        )

    # ── Interfaces ────────────────────────────────────────────────────────────────

    # When sync_modules was skipped the pre-blade block above didn't run, so
    # vss_ifaces / existing_ifaces / nd_names haven't been populated yet.
    # When sync_modules ran, blade sync may have created interfaces via module
    # type templates: refresh the cache so we update rather than re-create them.
    if sync_modules:
        existing_ifaces = nb.fetch_interfaces(nb_device.id)
        if slot_to_device:
            vss_ifaces = {pos: nb.fetch_interfaces(dev.id) for pos, dev in slot_to_device.items()}

    if not sync_modules:
        if slot_to_device:
            vss_ifaces = {pos: nb.fetch_interfaces(dev.id) for pos, dev in slot_to_device.items()}
        else:
            vss_ifaces = {}
        existing_ifaces = nb.fetch_interfaces(nb_device.id)
        nd_names = {
            port.get("port") or port.get("descr") for port in nd_ports
            if not (port.get("port") or port.get("descr") or "").lower().startswith(PORT_BLACKLIST_PREFIXES)
        }
        # Orphan pass for the no-module-sync path
        all_existing: dict = {}
        for d in vss_ifaces.values():
            all_existing.update(d)
        all_existing.update(existing_ifaces)
        orphaned_deleted = 0
        orphaned_errors = 0
        for name, iface in list(all_existing.items()):
            if name not in nd_names and not name.lower().startswith(PORT_BLACKLIST_PREFIXES):
                if housekeeping:
                    if iface_source_cf:
                        owner = (dict(getattr(iface, "custom_fields", {}) or {}).get(iface_source_cf) or "")
                        if owner and owner != iface_source_value:
                            log.debug("  %-40s skipping orphan delete: owned by %r", name, owner)
                            continue
                    try:
                        iface.delete()
                        orphaned_deleted += 1
                        log.debug("  %-40s deleted (not in Netdisco)", name)
                        existing_ifaces.pop(name, None)
                        for m in vss_ifaces.values():
                            m.pop(name, None)
                    except Exception as exc:
                        orphaned_errors += 1
                        log.error("  %-40s could not delete orphan: %s", name, exc)
        if housekeeping and (orphaned_deleted or orphaned_errors):
            log.debug("Orphaned interfaces: deleted=%d errors=%d", orphaned_deleted, orphaned_errors)

    # Sort: parent interfaces before subinterfaces (dot-notation) so parents exist when children are created
    nd_ports_sorted = sorted(nd_ports, key=lambda p: (1 if "." in (p.get("port") or p.get("descr") or "") else 0))
    log.debug("Interfaces  entries: %d  existing: %d", len(nd_ports_sorted), len(existing_ifaces))

    neighbors = neighbors_linked = 0
    seen_cable_iface_ids: set[int] = set()
    cable_counts: dict[str, int] = {"created": 0, "conflict": 0, "deleted": 0, "error": 0}
    for port in nd_ports_sorted:
        iface_name = port.get("port") or port.get("descr") or "?"
        if iface_name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            log.debug("  %-40s blacklisted: skipping", iface_name)
            continue
        try:
            nb_device_id, nb_iface_id = (
                _resolve_neighbor(nb, port.get("remote_ip", ""), port.get("remote_port", ""))
                if (cf_neighbor_device or cf_neighbor_iface or cable_scope) and port.get("remote_ip")
                else (None, None)
            )
            # For cabling, gate nb_iface_id on same-site check
            cable_iface_id = nb_iface_id
            if cable_scope == "site" and cable_iface_id and nb_device_id:
                remote_dev = nb.nb.dcim.devices.get(nb_device_id)
                local_site = getattr(getattr(nb_device, "site", None), "id", None)
                remote_site = getattr(getattr(remote_dev, "site", None), "id", None) if remote_dev else None
                if not local_site or local_site != remote_site:
                    cable_iface_id = None
            if port.get("remote_ip"):
                neighbors += 1
                if nb_device_id is not None:
                    neighbors_linked += 1
            nb_data = port_to_netbox(
                port,
                lldp_clear_stale=lldp_clear_stale,
                neighbor_device_id=nb_device_id,
                neighbor_iface_id=nb_iface_id,
                cf_neighbor_text=cf_neighbor_text,
                cf_neighbor_port=cf_neighbor_port,
                cf_neighbor_device=cf_neighbor_device,
                cf_neighbor_iface=cf_neighbor_iface,
            )
            if not sync_mac:
                nb_data.pop("mac_address", None)
            if slot_to_device:
                sw_pos = _slot_from_iface("vss", iface_name)
                target_id = slot_to_device[sw_pos].id if sw_pos in slot_to_device else nb_device.id
                target_existing = vss_ifaces.get(sw_pos, existing_ifaces)
            else:
                target_id = nb_device.id
                target_existing = existing_ifaces
            action, nb_iface = nb.upsert_interface(
                target_id, nb_data, target_existing.get(iface_name),
                source_cf=iface_source_cf, source_value=iface_source_value,
            )
            counts[action] += 1
            if action != "unchanged":
                log.debug("  %-40s %s", iface_name, action)
            else:
                log.debug("  %-40s unchanged", iface_name)

            # Cabling — fall back to the Netbox-side type when Netdisco has none,
            # so virtual/lag interfaces are still excluded and copper detection works
            _iface_type = nb_data.get("type") or (nb._nb_value(getattr(nb_iface, "type", None)) if nb_iface else None) or ""
            if cable_scope and cable_iface_id and nb_iface and _iface_type not in ("virtual", "lag", "bridge"):
                seen_cable_iface_ids.add(nb_iface.id)
                try:
                    c_action = nb.upsert_cable(
                        nb_iface.id, cable_iface_id,
                        cable_type=cable_type_from_iface_type(_iface_type),
                        source_cf=cable_source_cf,
                        source_value=cable_source_value,
                    )
                    if c_action == "created":
                        cable_counts["created"] += 1
                        log.debug("  %-40s cable → iface %d (%s)", iface_name, nb_iface_id, c_action)
                    elif c_action == "conflict":
                        cable_counts["conflict"] += 1
                except Exception as exc:
                    cable_counts["error"] += 1
                    log.warning("  %-40s cable error (not counted in errors): %s ↔ %s/%s: %s",
                                iface_name, iface_name, port.get("remote_ip", "?"), port.get("remote_port", "?"), exc)

        except Exception as exc:
            counts["error"] += 1
            log.error("  %-40s error: %s", iface_name, exc)

    if neighbors:
        log.debug("Neighbors  : found: %d  linked: %d  unresolved: %d",
                  neighbors, neighbors_linked, neighbors - neighbors_linked)

    # Delete stale cables: owned cables on interfaces that no longer have a neighbor
    if cable_scope and cable_source_cf:
        all_ifaces = dict(existing_ifaces)
        for d in vss_ifaces.values():
            all_ifaces.update(d)
        for iface_name, iface_obj in all_ifaces.items():
            if iface_obj.id in seen_cable_iface_ids:
                continue
            cable = getattr(iface_obj, "cable", None)
            if not cable:
                continue
            cf = dict(getattr(cable, "custom_fields", {}) or {})
            if cf.get(cable_source_cf) != cable_source_value:
                continue
            try:
                nb.delete_cable(cable.id)
                cable_counts["deleted"] += 1
                log.debug("  %-40s cable deleted (stale)", iface_name)
            except Exception as exc:
                log.error("  %-40s cable delete error: %s", iface_name, exc)

    if any(cable_counts.values()):
        log.debug("Cables     : %s", "  ".join(f"{k}: {v}" for k, v in cable_counts.items() if v))

    # Wire up parent links for subinterfaces (e.g. GigabitEthernet0/0/1.1132 → GigabitEthernet0/0/1)
    # For VSS, wire parents per member device (subinterface and parent are always on the same device).
    member_iface_maps = (
        [nb.fetch_interfaces(dev.id) for dev in slot_to_device.values()]
        if slot_to_device
        else [nb.fetch_interfaces(nb_device.id)]
    )
    parent_updated = 0
    for all_ifaces in member_iface_maps:
        for iface_name, iface in all_ifaces.items():
            if "." not in iface_name:
                continue
            parent_name = iface_name.rsplit(".", 1)[0]
            parent = all_ifaces.get(parent_name)
            if not parent:
                continue
            current_parent = nb._nb_value(getattr(iface, "parent", None))
            if current_parent != parent.id:
                try:
                    iface.update({"parent": parent.id})
                    parent_updated += 1
                    log.debug("  %s → parent %s", iface_name, parent_name)
                except Exception as exc:
                    log.error("  %s parent link error: %s", iface_name, exc)
    if parent_updated:
        log.debug("Subinterfaces: linked %d parent(s)", parent_updated)

    # Wire up LAG bonding from Netdisco "slave_of" (e.g. Fortinet: port3 slave_of LAG-ecn).
    # Physical members → Netbox `lag` field; virtual children (l2vlan, etc.) → `parent`.
    flat_ifaces: dict = {}
    for m in member_iface_maps:
        flat_ifaces.update(m)
    lag_linked = 0
    for port in nd_ports:
        parent_name = port.get("slave_of")
        if not parent_name:
            continue
        iface_name = port.get("port") or port.get("descr") or ""
        iface = flat_ifaces.get(iface_name)
        parent = flat_ifaces.get(parent_name)
        if not iface or not parent:
            continue
        field = _slave_link_field(iface_name, port.get("type"), nb._nb_value(getattr(iface, "type", None)))
        current = nb._nb_value(getattr(iface, field, None))
        if current == parent.id:
            continue
        try:
            iface.update({field: parent.id})
            lag_linked += 1
            log.debug("  %s → %s %s", iface_name, field, parent_name)
        except Exception as exc:
            log.error("  %s %s link error: %s", iface_name, field, exc)
    if lag_linked:
        log.debug("LAG members: linked %d parent(s)", lag_linked)

    # ── Interface → Module assignment ─────────────────────────────────────────────

    if slot_to_module:
        all_ifaces = nb.fetch_interfaces(nb_device.id)
        mod_iface_counts = {"updated": 0, "unchanged": 0, "skipped": 0}
        for iface_name, iface in all_ifaces.items():
            slot = _slot_from_iface(topo, iface_name)
            mod_id = slot_to_module.get(slot) if slot is not None else None
            if mod_id is None:
                mod_iface_counts["skipped"] += 1
                continue
            current = nb._nb_value(getattr(iface, "module", None))
            if current != mod_id:
                iface.update({"module": mod_id})
                mod_iface_counts["updated"] += 1
                log.debug("  %-40s → module slot %s", iface_name, slot)
            else:
                mod_iface_counts["unchanged"] += 1
        log.debug(
            "Interface→Module: updated=%d unchanged=%d skipped=%d",
            mod_iface_counts["updated"], mod_iface_counts["unchanged"], mod_iface_counts["skipped"],
        )

    # ── IPs ───────────────────────────────────────────────────────────────────────

    if sync_ip:
        # Re-fetch interfaces so newly created ones have IDs.
        # For VSS, merge all member devices' interfaces so IPs can be assigned across both switches.
        if slot_to_device:
            existing_ifaces = {}
            for pos, dev in slot_to_device.items():
                existing_ifaces.update(nb.fetch_interfaces(dev.id))
        else:
            existing_ifaces = nb.fetch_interfaces(nb_device.id)
        try:
            nd_ips = nd.get_device_ips(ip)
        except requests.HTTPError as exc:
            _reraise_if_gateway_error(exc)
            log.error("Could not fetch device IPs from Netdisco: %s", exc)
            nd_ips = []

        for entry in nd_ips:
            address = entry.get("alias") or entry.get("ip")
            subnet = entry.get("subnet")
            port_name = entry.get("port")
            if not address or not port_name:
                continue
            # Build CIDR if needed
            if "/" not in str(address) and subnet:
                try:
                    prefix = ipaddress.ip_network(subnet, strict=False).prefixlen
                    address = f"{address}/{prefix}"
                except ValueError:
                    pass
            iface = existing_ifaces.get(port_name)
            if not iface:
                log.warning("  IP %-20s skipped: interface %r not found in Netbox", address, port_name)
                ip_counts["skipped"] += 1
                continue
            try:
                action = nb.upsert_ip(address, iface)
                ip_counts[action] += 1
                if action in ("created", "fixed", "moved"):
                    log.debug("  IP %-20s → %s on %s", address, action, port_name)
                elif action == "unchanged":
                    log.debug("  IP %-20s → unchanged on %s", address, port_name)
            except Exception as exc:
                if "primary IP" in str(exc):
                    ip_counts["skipped"] += 1
                    log.warning(
                        "  IP %-20s on %-30s skipped: designated primary IP of another device"
                        " (enable housekeeping to remove the VIP device first)",
                        address, port_name,
                    )
                else:
                    ip_counts["error"] += 1
                    log.error("  IP %-20s on %-30s error: %s", address, port_name, exc)

        log.debug(
            "IPs: created=%d fixed=%d moved=%d unchanged=%d skipped=%d errors=%d",
            ip_counts["created"], ip_counts["fixed"], ip_counts["moved"],
            ip_counts["unchanged"], ip_counts["skipped"], ip_counts["error"],
        )

    # ── SFPs ──────────────────────────────────────────────────────────────────────

    if sync_sfp:
        # nd_mods already fetched if sync_modules ran; fetch only if needed
        if not sync_modules:
            try:
                nd_mods = nd.get_modules(ip)
            except requests.HTTPError as exc:
                _reraise_if_gateway_error(exc)
                log.error("Could not fetch modules from Netdisco: %s", exc)
                nd_mods = []
            _fill_module_names(nd_mods)

        sfps = [
            m for m in nd_mods
            if m.get("class") == "port" and m.get("model") and m.get("serial")
        ]
        log.debug("SFPs      entries: %d", len(sfps))

        # Re-fetch interfaces to include anything created this run.
        # For VSS, build a per-member interface cache so Switch 2 SFPs land on the right device.
        manufacturer = nb_device.device_type.manufacturer
        if slot_to_device:
            sfp_ifaces_by_device: dict[int, dict] = {
                pos: nb.fetch_interfaces(dev.id)
                for pos, dev in slot_to_device.items()
            }
        else:
            sfp_ifaces_by_device = {1: nb.fetch_interfaces(nb_device.id)}

        for sfp in sfps:
            name = sfp.get("name") or ""
            model = sfp.get("model") or ""
            serial = sfp.get("serial") or ""
            expanded = expand_iface_name(name)
            # Determine which VSS member owns this interface (first digit run before first '/').
            # e.g. GigabitEthernet2/0/1 → switch 2.  Falls back to pos 1 for standalone/stack.
            sw_match = re.match(r"[A-Za-z-]+(\d+)/", expanded)
            sw_pos = int(sw_match.group(1)) if sw_match and sw_match.group(1) in {
                str(k) for k in sfp_ifaces_by_device
            } else 1
            target_ifaces = sfp_ifaces_by_device.get(sw_pos) or sfp_ifaces_by_device.get(1, {})
            target_device = slot_to_device.get(sw_pos, nb_device) if slot_to_device else nb_device
            iface = target_ifaces.get(expanded)
            if not iface:
                log.debug("  SFP %-20s skipped: interface not in Netbox", name)
                continue
            try:
                action = nb.upsert_sfp(target_device, iface, manufacturer, name, model, serial)
                sfp_counts[action] += 1
                if action != "unchanged":
                    log.debug("  SFP %-20s model=%-20s serial=%s → %s", name, model, serial, action)
                else:
                    log.debug("  SFP %-20s unchanged", name)
            except Exception as exc:
                sfp_counts["error"] += 1
                log.error("  SFP %-20s error: %s", name, exc)

        log.debug(
            "SFPs: created=%d updated=%d unchanged=%d errors=%d",
            sfp_counts["created"], sfp_counts["updated"], sfp_counts["unchanged"], sfp_counts["error"],
        )

    # ── PoE ───────────────────────────────────────────────────────────────────────

    poe_counts: dict[str, int] = {"updated": 0, "unchanged": 0, "skipped": 0, "error": 0}

    if sync_poe:
        try:
            powered_ports = nd.get_powered_ports(ip)
        except requests.HTTPError as exc:
            _reraise_if_gateway_error(exc)
            log.warning("Could not fetch powered ports (device may not support PoE): %s", exc)
            powered_ports = []

        if powered_ports:
            existing_ifaces = nb.fetch_interfaces(nb_device.id)
            poe_iface_names = {p["port"] for p in powered_ports if p.get("port")}
            for port_name in poe_iface_names:
                iface = existing_ifaces.get(port_name)
                if not iface:
                    poe_counts["skipped"] += 1
                    continue
                try:
                    current = nb._nb_value(getattr(iface, "poe_mode", None))
                    if current != "pse":
                        iface.update({"poe_mode": "pse"})
                        poe_counts["updated"] += 1
                        log.debug("  PoE %s → pse", port_name)
                    else:
                        poe_counts["unchanged"] += 1
                except Exception as exc:
                    poe_counts["error"] += 1
                    log.error("  PoE %s error: %s", port_name, exc)

            log.debug(
                "PoE: updated=%d unchanged=%d skipped=%d errors=%d",
                poe_counts["updated"], poe_counts["unchanged"], poe_counts["skipped"], poe_counts["error"],
            )

    def _fmt(label: str, c: dict) -> Optional[str]:
        changed = c.get("created", 0) + c.get("updated", 0)
        errors = c.get("error", 0)
        if not changed and not errors:
            return None
        s = f"{label}=+{c.get('created', 0)}/~{c.get('updated', 0)}"
        if errors:
            s += f"/!{errors}"
        return s

    total_errors = sum(
        c.get("error", 0)
        for c in [counts, ip_counts, mod_counts, sfp_counts, poe_counts]
    )
    parts = list(filter(None, [
        _fmt("ifaces", counts),
        _fmt("ips", ip_counts) if sync_ip else None,
        _fmt("mods", mod_counts) if sync_modules else None,
        _fmt("sfps", sfp_counts) if sync_sfp else None,
    ]))
    summary = ("  " + "  ".join(parts)) if parts else "  no changes"
    log.info("sync done %s%s  errors=%d", nb_device.name, summary, total_errors)
    return {
        "ok": counts["error"] == 0,
        "hostname": nb_device.name,
        "interfaces": counts,
        "ips": ip_counts if sync_ip else {},
        "modules": mod_counts if sync_modules else {},
        "sfps": sfp_counts if sync_sfp else {},
        "ha_vip": vip_device is not None,
    }


