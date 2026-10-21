"""One device's sync from Netdisco into Netbox."""

import ipaddress
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

from .aps import (
    _WLC_NOTE_RE,
    AP_WIRED_IFACE,
    _ap_fingerprint,
    _ap_iface_fixups,
    _ap_note_block,
    _ap_radio_iface_type,
    _ap_radio_slots,
    _ap_radios,
    _ap_uplink_key,
    _ap_wired_iface_type,
    _is_ap_port,
    _is_wireless_type,
    _load_ap_state,
    _merge_ap_note,
    _merge_note_block,
    _parse_ap_description,
    _plan_ap_pass,
    _prune_ap_bays,
    _prune_ap_interfaces,
    _remove_wlc_radio_ports,
    _resolve_ap_uplinks,
    _save_ap_state,
    _tags_without,
    _wlc_note_block,
)
from .ha import (
    _HA_CREATED_LINE,
    _HA_NOTE_RE,
    _chassis_pos_of,
    _create_ha_peer,
    _ensure_ha_member_ip,
    _ensure_member_ip,
    _guess_prefix_len,
    _ha_node_info,
    _ha_note_block,
    _ha_peer_name,
    _handle_vip_device,
    _pick_primary_chassis,
    _resolve_ha_member_ip,
)
from .interfaces import (
    _apply_poe,
    _iface_lookup,
    _match_existing_iface,
    _remove_misplaced_vss_interfaces,
    _remove_orphaned_interfaces,
    port_to_netbox,
)
from .log import _DeviceLogAdapter, _sync_error_tls, logger
from .neighbors import _resolve_neighbor, _resolve_neighbor_by_fdb
from .netbox import NetboxClient
from .netdisco import NetdiscoClient
from .util import (
    DEVICE_LOCK_WAIT_S,
    DUMMY_INTERFACES,
    PORT_BLACKLIST_PREFIXES,
    STALE_DEVICE_BAY_PATTERNS,
    _cf_record_id,
    _device_lock,
    _merged_custom_fields,
    _reraise_if_gateway_error,
    _slave_link_field,
    _slot_from_iface,
    cable_type_from_iface_type,
    expand_iface_name,
    parse_sw_model,
    parse_sw_ver,
    vendor_from_chassis,
)


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


def _discovery_incomplete(nd_ports: list) -> bool:
    """
    True when every port name is a bare number — Netdisco fills in ifIndex
    as the port name until discovery has fetched the real ifDescr, so an
    all-numeric port list means the device's discovery hasn't finished yet.
    """
    names = [str(p.get("port") or p.get("descr") or "") for p in nd_ports]
    return bool(names) and all(n.isdigit() for n in names)


def _all_descriptions_missing(nd_ports: list) -> bool:
    """
    True when not a single port has an ifAlias set.

    Unlike _discovery_incomplete, this alone is NOT a reliable "discovery
    failed" signal — plenty of real, fully-discovered devices genuinely have
    no configured interface descriptions. It's only meaningful combined with
    Netbox already having real descriptions for this device (checked by the
    caller): a poll that suddenly reports every alias empty when Netbox
    already has some is more likely a degraded/partial Netdisco poll than
    every interface losing its label in the same instant.
    """
    aliases = [str(p.get("name") or "").strip() for p in nd_ports]
    return bool(aliases) and not any(aliases)


def _stack_member_count(topo: str, chassis: list) -> Optional[int]:
    """
    Total physical units for the stack_members custom field.

    Only meaningful for a traditional stack with more than one member. A
    count of 1 — whether from a standalone device, or a stack that's
    degraded down to its last member — carries no signal, so it's left
    unset, same as VSS (splits a pair across two separate Netbox devices)
    and FEX (satellites aren't stack members in the switch-stacking sense).
    """
    if topo == "stack" and len(chassis) > 1:
        return len(chassis)
    return None


def _should_update_stack_members(current_value: object, new_value: int, only_increase: bool) -> bool:
    """
    Whether to write a new stack_members value over the existing one.

    With only_increase (the default), a lower new count is rejected — a
    dead stack member must not silently ratchet the recorded value down.
    A non-numeric or missing current value (e.g. never set) always allows
    the write; only a genuine numeric decrease is blocked.
    """
    if current_value == new_value:
        return False
    if only_increase and isinstance(current_value, (int, float)) and new_value < current_value:
        return False
    return True


def _recently_touched(touch_value: object, cooldown_days: int, today: Optional[date] = None) -> bool:
    """
    Whether a device's touch field shows a real config change within cooldown_days.

    touch_value is a "YYYY-MM-DD"-ish string written to the touch custom field
    the last time sync_device made an actual change. Missing/unparseable
    values never block a sync (treated as "not recently touched").
    """
    if not touch_value or cooldown_days <= 0:
        return False
    try:
        touched = date.fromisoformat(str(touch_value)[:10])
    except ValueError:
        return False
    return (today or date.today()) - touched < timedelta(days=cooldown_days)


_UNCHANGED_ACTIONS = {"unchanged", "skipped", "error", "conflict"}


def _counts_changed(counts: Optional[dict]) -> bool:
    """True if a per-section counts dict recorded any real write (not unchanged/skipped/error)."""
    return bool(counts) and any(v for k, v in counts.items() if k not in _UNCHANGED_ACTIONS and v)


def _remove_legacy_chassis_modules(nb: "NetboxClient", device, chassis: list[dict], log) -> int:
    """
    Before HA pairs were modelled as a Virtual Chassis, each unit became a
    module bay + module on the controller device ("Chassis 1", "Chassis 2").
    Remove those, but only a bay named after a chassis whose module is empty
    or carries that chassis' serial. Interfaces still assigned to the module
    are detached first: deleting a module cascades to its components.
    """
    removed = 0
    for ch in chassis:
        bay_name = ch.get("name")
        if not bay_name:
            continue
        for bay in nb.nb.dcim.module_bays.filter(device_id=device.id, name=bay_name):
            installed = getattr(bay, "installed_module", None)
            if installed:
                module = nb.nb.dcim.modules.get(installed.id)
                if module and (module.serial or "") not in ("", ch.get("serial") or ""):
                    log.warning("  %s: module serial %r isn't chassis %r: left alone",
                                bay_name, module.serial, ch.get("serial"))
                    continue
                if module:
                    for iface in nb.nb.dcim.interfaces.filter(module_id=module.id):
                        iface.update({"module": None})
                    module.delete()
            bay.delete()
            removed += 1
            log.info("  Removed legacy chassis module bay %r (HA pair is a Virtual Chassis now)", bay_name)
    return removed


def sync_device(*args, **kwargs) -> dict:
    """
    Sync one device (see _sync_device), serialised per Netbox device: the
    per-device lock is taken inside once the device is resolved, and always
    released here.
    """
    held: list = []
    outer_count = getattr(_sync_error_tls, "count", None)     # a sync inside a sync: keep counting
    _sync_error_tls.count = 0
    try:
        return _sync_device(*args, _held_locks=held, **kwargs)
    finally:
        for lock in held:
            lock.release()
        inner = _sync_error_tls.count
        _sync_error_tls.count = None if outer_count is None else outer_count + inner


def _sync_device(
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
    vip_mode: str = "threenode",   # threenode | soft | hard | off; fallback when the device's
                                    # vendor isn't in vip_mode_by_vendor
    vip_mode_by_vendor: Optional[dict[str, str]] = None,  # e.g. {"fortinet": "threenode", "cisco": "soft"} —
                                                            # keyed by nd_device["vendor"] (already lowercased)
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
    cf_stack_members: Optional[str] = "stack_members",
    stack_members_only_increase: bool = True,
    cf_touch: Optional[str] = "netdisco_last_update",
    cf_inventory_rebuild: Optional[str] = None,  # datetime CF stamped when a rebuild (prune,
                                                 # not dry-run) ran, changed or not; None = off
    touch_cooldown_days: int = 1,
    ha_metrics_url: Optional[str] = None,
    ha_metrics_metric: str = "fgHaStatsSyncStatus_info",
    ha_metrics_hostname_label: str = "fgHaStatsHostname",
    ha_metrics_target_label: str = "netbox_primary_ip",
    ha_metrics_timeout: int = 15,
    ha_metrics_tls_verify: bool = True,
    ha_metrics_instance_label: str = "instance",
    ha_mgmt_iface_name: str = "mgmt1",
    cf_controller: Optional[str] = "controller",  # object CF (→ Device) set on devices a
                                                   # controller reports, e.g. WLC → AP
    ap_prune_interfaces: bool = True,  # APs keep only GigabitEthernet0 + Dot11Radio<n>, no empty bays
    remove_orphaned_interfaces: Optional[bool] = None,  # also delete source-less interfaces Netdisco
                                                        # doesn't report (own ones always go); None =
                                                        # follow housekeeping
    orphan_delete_max_percent: int = 25,  # mass-deletion brake for that
    ap_type_confirmed_untag: Optional[list[str]] = None,  # tag slugs removed once an AP's model
                                                           # resolves to a type (aps.type_confirmed_untag)
    ap_full: bool = False,  # WLC: update every AP even when the AP list's fingerprint is unchanged
    prune: bool = False,
    dry_run: bool = True,
    _held_locks: Optional[list] = None,
) -> dict:
    """
    Sync device fields, interfaces, MACs, IPs, modules, and SFPs.

    Returns a dict with:
      ok          bool  : True if no errors occurred
      interfaces  dict  : created/updated/unchanged/error counts
      ips         dict  : created/fixed/moved/unchanged/skipped/error counts
      modules     dict  : created/updated/unchanged/error counts
      prune       dict  : only present when prune=True — deletion counts (see below)

    prune=True additionally reconciles this device's interfaces, chassis/blade
    modules, fans/PSUs, and SFP inventory items to exactly what this Netdisco
    snapshot reports, deleting anything else, and bypasses the stack_members
    increase-only ratchet. Unlike the rest of sync_device this is destructive —
    it's meant for an explicit manual rebuild, never the regular background sync.
    dry_run (default True) reports what prune would delete without deleting it.
    """
    log = _DeviceLogAdapter(logger, {"ip": ip})

    # nd_ip is what every subsequent Netdisco call uses; ip (Netbox's primary_ip4)
    # stays the identity for Netbox-side lookups/logging regardless.
    nd_ip = ip
    try:
        nd_device = nd.get_device(nd_ip)
        nd_ports = nd.get_ports(nd_ip)
    except requests.HTTPError as exc:
        _reraise_if_gateway_error(exc)
        not_found = exc.response is not None and exc.response.status_code == 404
        resolved = nd.find_canonical_ip(ip) if not_found else None
        if not_found and not resolved:
            log.info("Not in Netdisco (it 404s the IP, no other IP of a device matches): nothing to sync")
            return {"ok": False, "reason": "not_in_netdisco",
                    "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}
        if not resolved or resolved == ip:
            log.error("Netdisco request failed: %s", exc)
            return {"ok": False, "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}
        log.warning(
            "%s isn't Netdisco's canonical IP for this device (likely a cluster "
            "floating address) — retrying via %s", ip, resolved,
        )
        nd_ip = resolved
        try:
            nd_device = nd.get_device(nd_ip)
            nd_ports = nd.get_ports(nd_ip)
        except requests.HTTPError as exc2:
            _reraise_if_gateway_error(exc2)
            log.error("Netdisco request failed even via canonical IP %s: %s", nd_ip, exc2)
            return {"ok": False, "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    nd_hostname = nd_device.get("name") or nd_device.get("dns") or ""
    nd_serial = nd_device.get("serial") or ""
    vip_mode = (vip_mode_by_vendor or {}).get((nd_device.get("vendor") or "").lower(), vip_mode)
    log.info("sync start%s %s", " (rebuild)" if prune else "", nd_hostname or ip)
    log.debug("Netdisco  hostname=%r  ports=%d", nd_hostname, len(nd_ports))

    remove_orphans = housekeeping if remove_orphaned_interfaces is None else remove_orphaned_interfaces
    orphans_removed = 0
    orphan_stats: dict = {"braked": 0}
    ap_radio_ports = [p for p in nd_ports if _is_ap_port(p)]
    ap_port_count = len(ap_radio_ports)
    if ap_port_count:
        nd_ports = [p for p in nd_ports if not _is_ap_port(p)]
        log.debug(
            "  Filtered %d AP radio port(s) (type=dot11* or <MAC>.<slot>) — these belong to associated "
            "APs, not to this device's own interfaces", ap_port_count,
        )

    if _discovery_incomplete(nd_ports):
        log.warning(
            "All %d interface names are bare numbers (ifIndex placeholders): "
            "discovery looks incomplete, skipping sync", len(nd_ports),
        )
        return {"ok": False, "reason": "discovery_incomplete", "hostname": nd_hostname,
                "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    nb_device = nb.find_device_by_ip(ip, hostname=nd_hostname, serial=nd_serial)
    if not nb_device:
        log.info("In Netdisco, not in Netbox (IP %s, hostname %r): nothing to sync", ip, nd_hostname)
        return {"ok": False, "reason": "device_not_found", "hostname": nd_hostname,
                "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    logger.debug("Netbox    device=%r  id=%s", nb_device.name, nb_device.id)

    if _held_locks is not None:
        lock = _device_lock(nb_device.id)
        if lock.acquire(blocking=False):
            _held_locks.append(lock)
        else:
            log.info("  %s is being synced already (another IP of the same device): waiting", nb_device.name)
            if lock.acquire(timeout=DEVICE_LOCK_WAIT_S):
                _held_locks.append(lock)
            else:
                log.warning("  %s still busy after %ds: syncing anyway", nb_device.name, DEVICE_LOCK_WAIT_S)

    if cf_touch:
        touch_value = dict(getattr(nb_device, "custom_fields", {}) or {}).get(cf_touch)
        if _recently_touched(touch_value, touch_cooldown_days):
            log.info(
                "  %s=%r within %dd cooldown: skipping sync",
                cf_touch, touch_value, touch_cooldown_days,
            )
            return {"ok": True, "reason": "recently_touched", "hostname": nd_hostname,
                    "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    device_changed = False

    # ── HA / VIP detection ────────────────────────────────────────────────────────
    # Signal: SNMP hostname belongs to a different Netbox device than the IP lookup found.
    # This happens when the management IP is a cluster VIP. The serial approach alone
    # is unreliable because previous syncs may have written the active node's serial
    # onto the VIP device. Hostname mismatch is the primary signal.
    vip_device = None
    vip_redirected = False
    if nd_hostname and nb_device.name:
        nd_short = nd_hostname.lower().split(".")[0]
        nb_short = nb_device.name.lower().split(".")[0]
        if nd_short != nb_short:
            # Try to find the real physical device by SNMP hostname
            real_device = None
            # 0. HA-metrics lookup: an exporter-provided label (e.g. Fortinet
            #    fgHaStatsSyncStatus_info) may already carry the correct Netbox
            #    primary IP for this vendor-reported hostname — trustworthy
            #    regardless of how unrelated the vendor's own hostname is to
            #    Netbox naming. Tried first; falls through on any failure.
            if ha_metrics_url:
                try:
                    resolved_ip = _resolve_ha_member_ip(
                        ha_metrics_url, nd_hostname,
                        metric=ha_metrics_metric,
                        hostname_label=ha_metrics_hostname_label,
                        target_label=ha_metrics_target_label,
                        timeout=ha_metrics_timeout,
                        verify_tls=ha_metrics_tls_verify,
                    )
                except Exception as exc:
                    log.warning("HA metrics lookup for %r failed: %s", nd_hostname, exc)
                    resolved_ip = None
                if resolved_ip:
                    real_device = nb.find_device_by_ip(resolved_ip)
            # 1. Exact match, then contains with short-name verification
            if not real_device:
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
                pre_redirect = nb_device
                nb_device = real_device
                vip_redirected = True
                log.info(
                    "HA VIP detected: %r → redirecting to %r",
                    pre_redirect.name, nb_device.name,
                )

                # Find partner node by swapping node indicator in hostname
                partner_dev = nb.find_ha_partner(nb_device.name)
                mgmt_prefix = _guess_prefix_len(nb_device, partner_dev, pre_redirect)

                if ha_metrics_url:
                    for member in (nb_device, partner_dev):
                        if _ensure_ha_member_ip(
                            nb, member, mgmt_prefix, ha_metrics_url, ha_metrics_instance_label,
                            ha_metrics_metric, ha_metrics_target_label, ha_metrics_timeout,
                            ha_metrics_tls_verify, ha_mgmt_iface_name, log,
                            source_cf=iface_source_cf, source_value=iface_source_value,
                        ):
                            device_changed = True

                real_short = nb_device.name.split(".")[0]
                domain = nb_device.name[len(real_short):]
                real_ha_info = _ha_node_info(real_short)
                vip_short = real_ha_info[2] if real_ha_info else None

                pre_short = pre_redirect.name.split(".")[0] if pre_redirect.name else ""
                if _ha_node_info(pre_short):
                    # pre_redirect (found by the synced IP) is itself an already-identified
                    # cluster member, not a VIP placeholder — its address must have drifted
                    # onto it (e.g. a prior `soft` vip_mode run freed the real VIP's address
                    # and a later interface sync claimed it for this member). Never
                    # mirror/rename a real member as if it were the VIP: look up the
                    # (possibly orphaned) VIP device by its expected name instead.
                    found_vip = None
                    if vip_short:
                        for candidate in (vip_short + domain, vip_short):
                            results = list(nb.nb.dcim.devices.filter(name__ie=candidate))
                            if results:
                                found_vip = results[0]
                                break
                    if found_vip:
                        log.warning(
                            "%s resolved to %r, but that's already a real cluster member, not "
                            "the VIP — its address must have drifted; using orphaned VIP %r instead",
                            ip, pre_redirect.name, found_vip.name,
                        )
                    else:
                        log.warning(
                            "%s resolved to %r, but that's already a real cluster member, not "
                            "the VIP, and no orphaned VIP device (%s) was found in NetBox — "
                            "skipping VIP handling to avoid corrupting %r",
                            ip, pre_redirect.name, vip_short or "?", pre_redirect.name,
                        )
                    vip_device = found_vip
                else:
                    vip_device = pre_redirect

                # Create / update Virtual Chassis, named after the VIP's own FQDN when we
                # have a device record for it, otherwise its expected name pattern.
                if vip_device:
                    vc_name = vip_device.name
                elif vip_short:
                    vc_name = vip_short + domain
                else:
                    vc_name = pre_redirect.name
                active_m = re.search(r"p(\d+)[a-z]?", nb_device.name, re.IGNORECASE)
                active_pos = int(active_m.group(1)) if active_m else 1
                vc_members: list[tuple] = [(nb_device, active_pos)]
                if partner_dev:
                    partner_m = re.search(r"p(\d+)[a-z]?", partner_dev.name, re.IGNORECASE)
                    partner_pos = int(partner_m.group(1)) if partner_m else (2 if active_pos == 1 else 1)
                    vc_members.append((partner_dev, partner_pos))
                    log.info("HA partner found: %r", partner_dev.name)
                else:
                    log.warning("HA partner not found for %r: VC will have one member", nb_device.name)
                if vip_mode == "threenode" and vip_device:
                    vc_members.append((vip_device, 0))
                    log.info("HA VIP device %r added as VC member pos=0", vip_device.name)
                    # ip is the address currently being synced — by construction (a
                    # hostname mismatch redirected us here) it's the floating/cluster
                    # address, so it belongs on the VIP device, not wherever a prior
                    # vip_mode may have left it.
                    if _ensure_member_ip(nb, vip_device, ip, mgmt_prefix, ha_mgmt_iface_name, log,
                                         iface_source_cf, iface_source_value):
                        device_changed = True
                try:
                    vc_action, _ = nb.upsert_virtual_chassis(vc_name, vc_members)
                    log.info("HA VirtualChassis %r: %s", vc_name, vc_action)
                    if vc_action != "unchanged":
                        device_changed = True
                except Exception as exc:
                    log.error("HA VirtualChassis error: %s", exc)

                if vip_device:
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
    if not vip_redirected:
        short = nb_device.name.split(".")[0]
        ha_info = _ha_node_info(short)
        if ha_info:
            partner_dev = nb.find_ha_partner(nb_device.name)
            if partner_dev:
                if ha_metrics_url:
                    mgmt_prefix = _guess_prefix_len(nb_device, partner_dev)
                    for member in (nb_device, partner_dev):
                        if _ensure_ha_member_ip(
                            nb, member, mgmt_prefix, ha_metrics_url, ha_metrics_instance_label,
                            ha_metrics_metric, ha_metrics_target_label, ha_metrics_timeout,
                            ha_metrics_tls_verify, ha_mgmt_iface_name, log,
                            source_cf=iface_source_cf, source_value=iface_source_value,
                        ):
                            device_changed = True

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

                # VC identity: prefer the VIP's FQDN so this path and the
                # VIP-hook path (which names the VC after the VIP device) agree
                # on the same VC — otherwise each path creates its own VC and
                # fights over the members. Fall back to the short hostname with
                # the node indicator stripped, plus this device's own domain
                # suffix, when no VIP device exists.
                vc_name = found_vip_dev.name if found_vip_dev else (vc_base + domain)

                if vip_mode == "threenode" and found_vip_dev:
                    vc_members.append((found_vip_dev, 0))
                    log.info("HA VIP device %r added as VC member pos=0", found_vip_dev.name)

                try:
                    vc_action, _ = nb.upsert_virtual_chassis(vc_name, vc_members)
                    log.info(
                        "HA peer VirtualChassis %r: %s  (topology=standalone per Netdisco; VC is Netbox-side)",
                        vc_name, vc_action,
                    )
                    if vc_action != "unchanged":
                        device_changed = True
                except Exception as exc:
                    log.error("HA peer VirtualChassis error: %s", exc)

                if found_vip_dev:
                    _handle_vip_device(nb, found_vip_dev, vip_mode, log, active_device=nb_device)

    if nb.update_device_fields(nb_device, nd_device, log=log,
                               cf_os_version=cf_os_version, cf_os_name=cf_os_name, cf_os_release=cf_os_release):
        device_changed = True

    # Hoisted above the housekeeping guard: prune (below) needs it too, and it's
    # cheap — just a comprehension over ports already fetched.
    nd_port_names = {
        p.get("port") or p.get("descr") for p in nd_ports
        if p.get("port") or p.get("descr")
    }

    if housekeeping:
        deleted_bays = nb.remove_stale_device_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        deleted_mod_bays = nb.remove_stale_module_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        deleted_ifaces = nb.remove_empty_dummy_interfaces(nb_device, DUMMY_INTERFACES, nd_port_names)
        if deleted_bays or deleted_mod_bays or deleted_ifaces:
            device_changed = True
        log.debug(
            "Housekeeping: deleted %d stale device bay(s), %d stale module bay(s), %d empty dummy interface(s)",
            deleted_bays, deleted_mod_bays, deleted_ifaces,
        )

    # A WLC (it reports AP radio ports): drop the radio pseudo-ports older versions
    # created on it. Not gated on housekeeping: they're never real interfaces.
    if ap_port_count and _remove_wlc_radio_ports(nb, nb_device, iface_source_cf, iface_source_value, log):
        device_changed = True

    counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "skipped": 0, "error": 0}
    ip_counts: dict[str, int] = {"created": 0, "fixed": 0, "moved": 0, "unchanged": 0, "skipped": 0, "error": 0}
    mod_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    sfp_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    ap_counts: dict[str, int] = {"updated": 0, "unchanged": 0, "not_found": 0, "error": 0, "skipped": 0}

    # slot_to_module populated during module sync; consumed by interface→module pass.
    slot_to_module: dict[int, int] = {}  # slot key (stack pos / FEX ID) → nb module id
    module_patch_batch: list = []
    module_names_by_id: dict[int, str] = {}
    # slot_to_device populated for VSS; used to route blades to the correct member device.
    slot_to_device: dict[int, object] = {}  # VSS pos → nb device record
    topo = "standalone"                   # updated inside sync_modules block

    # prune-only bookkeeping: names/ids claimed by this run, per target device —
    # anything already in Netbox but not in these sets is a candidate for deletion.
    current_module_names_by_device: dict[int, set[str]] = {}
    current_bare_inventory_names_by_device: dict[int, set[str]] = {}
    current_sfp_iface_ids: set[int] = set()
    stack_members_prune_result: dict = {}

    # ── Modules (before interfaces so bays exist when interfaces are assigned) ────

    if sync_modules:
        try:
            nd_mods = nd.get_modules(nd_ip)
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
        # Cisco 9800 HA SSO pairs: ENTITY-MIB root "Multi Chassis System" with a
        # chassis per unit; modelled like StackWise Virtual (one Netbox device
        # per unit, joined in a Virtual Chassis), peers created when missing
        is_ha_pair = has_stack and "multi chassis system" in (stack_root.get("name") or "").lower()
        _mods_by_index = {m.get("index"): m for m in nd_mods if m.get("index") is not None}

        def _ha_member(entry: dict):
            """HA pair: the member device owning entry, by its ENTITY chassis ancestor (None = unknown)."""
            pos = _chassis_pos_of(entry, _mods_by_index)
            return slot_to_device.get(pos) if pos is not None else None
        is_vss = has_stack and (
            "virtualstack" in (stack_root.get("type") or "").lower()
            or "virtual stack" in (stack_root.get("name") or "").lower()
            or is_ha_pair
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

        member_count = _stack_member_count(topo, chassis)
        if cf_stack_members:
            current_cf = dict(getattr(nb_device, "custom_fields", {}) or {})
            current_value = current_cf.get(cf_stack_members)
            if member_count is not None:
                # A rebuild (prune) force-corrects the count, including decreases —
                # the regular background sync never bypasses the ratchet.
                effective_only_increase = stack_members_only_increase and not prune
                if _should_update_stack_members(current_value, member_count, effective_only_increase):
                    if prune and dry_run:
                        log.info(
                            "  [dry-run] would set %s=%d (was %r)",
                            cf_stack_members, member_count, current_value,
                        )
                        stack_members_prune_result = {"was": current_value, "now": member_count}
                    else:
                        if nb_device.update({"custom_fields": _merged_custom_fields(nb_device, {cf_stack_members: member_count})}):
                            device_changed = True
                        log.debug("  %s=%d updated", cf_stack_members, member_count)
                        if prune:
                            stack_members_prune_result = {"was": current_value, "now": member_count}
                elif current_value != member_count:
                    log.debug(
                        "  %s: keeping %s (new count %d is lower — a dead member shouldn't reduce it)",
                        cf_stack_members, current_value, member_count,
                    )
            elif prune and current_value not in (None, ""):
                # _stack_member_count() only recognizes a genuine multi-member "stack"
                # topology — a stack that's degraded to its last member (or was
                # reclassified as standalone/VSS/FEX) reports None here on purpose, so
                # the regular sync leaves a stale count alone rather than guessing. A
                # rebuild instead clears it: the current snapshot no longer supports
                # any specific number, so an unsupported leftover count is worse than
                # no value at all.
                if dry_run:
                    log.info(
                        "  [dry-run] would clear %s (was %r — topology now %s)",
                        cf_stack_members, current_value, topo,
                    )
                    stack_members_prune_result = {"was": current_value, "now": None}
                else:
                    if nb_device.update({"custom_fields": _merged_custom_fields(nb_device, {cf_stack_members: None})}):
                        device_changed = True
                    log.debug("  %s cleared (topology now %s)", cf_stack_members, topo)
                    stack_members_prune_result = {"was": current_value, "now": None}

        manufacturer = nb_device.device_type.manufacturer

        def _update_device_type(ch: dict) -> None:
            """Update DeviceType (and serial) on nb_device from a chassis entry."""
            part_number = ch.get("model", "") or ""
            serial = ch.get("serial") or ""
            # An OID fragment model (e.g. ".112.100.1003") still goes through
            # get_or_create_device_type: a type alias may resolve it, and
            # otherwise it returns None and the current DeviceType stays.
            model = parse_sw_model(ch.get("sw_ver", "")) or part_number
            if not model:
                if serial and (nb_device.serial or "") != serial:
                    nb_device.update({"serial": serial})
                    log.debug("  serial=%s updated (no valid model from Netdisco)", serial)
                else:
                    log.debug("  DeviceType skipped: no valid model from Netdisco")
                mod_counts["unchanged"] += 1
                return
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            device_type = nb.get_or_create_device_type(mfr, model, part_number=part_number, vendor=nd_device.get("vendor"))
            patch = {}
            if device_type and nb_device.device_type.id != device_type.id:
                patch["device_type"] = device_type.id
            if serial and (nb_device.serial or "") != serial:
                patch["serial"] = serial
            if patch:
                nb_device.update(patch)
                # pynetbox replaces device_type with a plain int after update: restore the object
                if device_type:
                    nb_device.device_type = device_type
                log.debug("  DeviceType → %s  serial=%s  updated", getattr(device_type, "model", model), serial)
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
            module_type = nb.get_or_create_module_type(mfr, model, vendor=nd_device.get("vendor"))
            if not module_type:
                mod_counts["unchanged"] += 1
                return
            bay = nb.upsert_module_bay(nb_device, name, position)
            action, module = nb.upsert_module(nb_device, bay, module_type, serial, batch=module_patch_batch)
            mod_counts[action] += 1
            if action == "updated" and module:
                module_names_by_id[module.id] = name
            if slot_key is not None and module:
                slot_to_module[slot_key] = module.id
            current_module_names_by_device.setdefault(nb_device.id, set()).add(name)
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
            primary_ch = _pick_primary_chassis(chassis, device_serial)
            partner_chs = [c for c in chassis if c is not primary_ch]
            if is_ha_pair:
                _remove_legacy_chassis_modules(nb, nb_device, chassis, log)

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

                partner_created = False
                if is_ha_pair:
                    peer_name = _ha_peer_name(nb_device.name, partner_pos)
                    if not partner_dev:
                        results = list(nb.nb.dcim.devices.filter(name__ie=peer_name))
                        partner_dev = results[0] if results else None
                    if not partner_dev:
                        try:
                            partner_dev = _create_ha_peer(nb, nb_device, peer_name, partner_serial,
                                                          iface_source_cf, iface_source_value)
                            partner_created = True
                            log.info("  HA peer %r created (chassis %s, serial %s)", peer_name, partner_pos, partner_serial)
                        except Exception as exc:
                            log.error("  HA peer %r could not be created: %s", peer_name, exc)
                    if partner_dev:
                        existing = getattr(partner_dev, "comments", "") or ""
                        new_comments = _merge_note_block(
                            existing,
                            _ha_note_block(nb_device.name, partner_pos,
                                           created=partner_created or _HA_CREATED_LINE in existing,
                                           location_missing=not getattr(partner_dev, "location", None)),
                            _HA_NOTE_RE,
                        )
                        if new_comments is not None:
                            try:
                                partner_dev.update({"comments": new_comments})
                            except Exception as exc:
                                log.error("  HA peer %r note update error: %s", partner_dev.name, exc)

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
                    partner_dt = nb.get_or_create_device_type(
                        mfr, partner_model, part_number=partner_part_number, vendor=nd_device.get("vendor"),
                    )
                    patch: dict = {}
                    if partner_dt and partner_dev.device_type.id != partner_dt.id:
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

            # Create/update Virtual Chassis linking both physical devices, named
            # after the primary device's own FQDN (Netbox-sourced, so it's stable
            # even when Netdisco's reported hostname is bare/short).
            vc_name = nb_device.name or nd_hostname or f"vc-{ip}"
            try:
                vc_action, _vc = nb.upsert_virtual_chassis(vc_name, vc_members)
                log.info("  VirtualChassis %r: %s", vc_name, vc_action)
            except Exception as exc:
                log.error("  VirtualChassis error: %s", exc)

            # Build pos → device map so blades can be routed to the right member
            for _dev, _pos in vc_members:
                slot_to_device[_pos] = _dev

        else:
            # Traditional stack: correct the top-level device's DeviceType from the
            # stack master (matched by serial, same as the FEX/VSS primary lookup
            # above) — otherwise a device's DeviceType is only ever set once, at
            # creation, and never revisited once it's syncing as a multi-member
            # stack (e.g. hardware swapped from a standalone 3850 to a 9300L
            # stack: nothing below this point would ever correct it).
            device_serial = nd_device.get("serial", "")
            master = next((c for c in chassis if c.get("serial") == device_serial), chassis[0] if chassis else None)
            if master is None:
                # No ENTITY-MIB chassis: Netdisco's device model is then often just
                # the sysObjectID (e.g. "enterprises.2440"), so only an explicit
                # type alias may move the DeviceType, never the raw string itself
                current_dt = nb_device.device_type
                aliased = nb.resolve_device_type_alias(
                    nd_device.get("model"),
                    getattr(current_dt, "part_number", None),
                    getattr(current_dt, "model", None),
                    vendor=nd_device.get("vendor"),
                )
                if aliased and aliased.id != current_dt.id:
                    nb_device.update({"device_type": aliased.id})
                    nb_device.device_type = aliased
                    log.info("  DeviceType → %s (type alias, no chassis modules)", aliased.model)
                    mod_counts["updated"] += 1
                else:
                    log.warning("  No chassis modules reported by Netdisco: skipping DeviceType/module sync")
            else:
                try:
                    _update_device_type(master)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  DeviceType update error: %s", exc)

            # Create a module bay + module per chassis member.
            # Netdisco pos is 0-indexed; Cisco interface names are 1-indexed (Gi1/0/1 = member 1).
            for ch in chassis:
                try:
                    pos = ch.get("pos")
                    _upsert_chassis_bay(ch, slot_key=pos + 1 if pos is not None else None)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  %-30s error: %s", ch.get("name", ""), exc)

        if module_patch_batch:
            failed_ids = nb.flush_module_updates(module_patch_batch, log=log)
            for failed_id in failed_ids:
                mod_counts["updated"] -= 1
                mod_counts["error"] += 1
                log.error("  %-30s failed to apply (see above)", module_names_by_id.get(failed_id, failed_id))

        log.debug(
            "Modules: updated=%d unchanged=%d errors=%d",
            mod_counts.get("updated", 0) + mod_counts.get("created", 0),
            mod_counts["unchanged"], mod_counts["error"],
        )

        # WLC junction: ap-class module entries describe *other* devices (the
        # associated APs), each already present in Netbox under its own record.
        # They're never module bays of this device — they update the AP's own
        # record: DeviceType/serial, OS version, controller link, a marker-
        # delimited comments block, its wired interface and radios + MACs.
        ap_modules = [m for m in nd_mods if m.get("class") == "ap"]
        ap_untyped: dict[str, int] = {}   # AP model → count of APs no existing DeviceType matched
        ap_untag = {t.lower() for t in (ap_type_confirmed_untag or [])}

        ap_uplink_ids: dict[tuple[str, str], int] = {}

        def _update_ap_device(ap_dev, ch: dict, parsed: dict) -> str:
            model = ch.get("model") or ""
            serial = ch.get("serial") or ""
            patch: dict = {}
            if model:
                vendor_name = vendor_from_chassis(ch)
                mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
                # Lookup only: an AP model without a matching type (no alias,
                # part_number, model or slug hit) keeps its current type. Minting
                # a bare type here would move every AP of that model onto it.
                device_type = nb.get_or_create_device_type(
                    mfr, model, part_number=model, vendor=nd_device.get("vendor"), create=False,
                )
                if device_type is None:
                    ap_untyped[model] = ap_untyped.get(model, 0) + 1
                else:
                    if ap_dev.device_type.id != device_type.id:
                        patch["device_type"] = device_type.id
                    # the WLC-reported model resolved to a type: the model is confirmed,
                    # so a "model needs fixing" tag no longer applies
                    kept_tags = _tags_without(getattr(ap_dev, "tags", None), ap_untag)
                    if kept_tags is not None:
                        patch["tags"] = kept_tags
            if serial and (ap_dev.serial or "") != serial:
                patch["serial"] = serial

            # Only fields that exist on Device are written (Netbox lists every
            # defined custom field, null or not); others are silently skipped.
            cf = dict(getattr(ap_dev, "custom_fields", {}) or {})
            cf_updates: dict = {}
            sw_ver = ch.get("sw_ver") or ""
            if sw_ver and cf_os_version and cf_os_version in cf and cf.get(cf_os_version) != sw_ver:
                cf_updates[cf_os_version] = sw_ver
            # Netdisco has no AP OS name/release: clear imported (switch) values
            # rather than keep a wrong one next to the AP's real version.
            for name in (cf_os_name, cf_os_release):
                if name and name in cf and cf.get(name):
                    cf_updates[name] = None
            if cf_controller and cf_controller in cf and _cf_record_id(cf.get(cf_controller)) != nb_device.id:
                cf_updates[cf_controller] = nb_device.id
            if cf_updates:
                patch["custom_fields"] = cf_updates

            radios = _ap_radios(ap_radio_ports, parsed.get("dot3_mac") or "")
            new_comments = _merge_ap_note(
                getattr(ap_dev, "comments", "") or "",
                _ap_note_block(parsed, nb_device.name, radios, controller_id=nb_device.id,
                               uplink_id=ap_uplink_ids.get(_ap_uplink_key(parsed))),
            )
            if new_comments is not None:
                patch["comments"] = new_comments

            changed = False
            if patch:
                ap_dev.update(patch)
                changed = True

            ifaces = nb.fetch_interfaces(ap_dev.id)
            eth_mac = parsed.get("ethernet_mac")
            if eth_mac:
                existing = next((i for n, i in ifaces.items() if n.lower() == AP_WIRED_IFACE.lower()), None)
                old_mac = str(getattr(getattr(existing, "primary_mac_address", None), "mac_address", "") or "")
                wired_type = _ap_wired_iface_type(model)
                data = {"name": AP_WIRED_IFACE, "type": wired_type, "mac_address": eth_mac,
                        **_ap_iface_fixups(existing, wired_type)}
                try:
                    action, _ = nb.upsert_interface(
                        ap_dev.id, data, existing, source_cf=iface_source_cf, source_value=iface_source_value,
                    )
                    if action in ("created", "updated") or (action == "unchanged" and old_mac.lower() != eth_mac.lower()):
                        changed = True
                except Exception as exc:
                    log.error("  AP %s: %s update error: %s", ap_dev.name, AP_WIRED_IFACE, exc)
            # One Dot11Radio<slot> per radio the WLC reports (devicetype-library
            # naming, so template interfaces are adopted). The WLC only exposes
            # the radio base MAC, the same on every slot (per-slot BSSIDs aren't
            # known): it goes on the first radio only, so a MAC search finds one
            # interface; the comments block lists it too.
            radio_mac = parsed.get("dot3_mac") or ""
            for n_radio, (slot, _) in enumerate(_ap_radio_slots(ap_radio_ports, radio_mac)):
                name = f"Dot11Radio{slot}"
                existing = next((i for n, i in ifaces.items() if n.lower() == name.lower()), None)
                data: dict = {"name": name}
                mac = radio_mac if n_radio == 0 else ""
                old_mac = str(getattr(getattr(existing, "primary_mac_address", None), "mac_address", "") or "")
                if mac:
                    data["mac_address"] = mac
                current_type = getattr(getattr(existing, "type", None), "value", None) if existing else None
                if existing is None or not _is_wireless_type(current_type):
                    # a radio is a radio: fix a template/legacy non-wireless type
                    # (a template's own wireless type is kept)
                    data["type"] = _ap_radio_iface_type(model)
                data.update(_ap_iface_fixups(existing, data.get("type") or current_type))
                try:
                    action, _ = nb.upsert_interface(
                        ap_dev.id, data, existing, source_cf=iface_source_cf, source_value=iface_source_value,
                    )
                    if action in ("created", "updated") or (mac and old_mac.lower() != mac.lower()):
                        changed = True
                except Exception as exc:
                    log.error("  AP %s: %s update error: %s", ap_dev.name, name, exc)
            if ap_prune_interfaces:
                removed = _prune_ap_interfaces(nb, ap_dev, ifaces, iface_source_cf, iface_source_value, log)
                removed += _prune_ap_bays(nb, ap_dev, log)
                if removed:
                    changed = True
            return "updated" if changed else "unchanged"

        ap_state: dict = _load_ap_state(nb_device.id) if ap_modules else {}
        ap_fp = _ap_fingerprint(ap_modules, ap_radio_ports, [
            nb_device.id, nb_device.name, nd_device.get("vendor"), cf_controller, cf_os_version,
            cf_os_name, cf_os_release, ap_prune_interfaces, iface_source_cf, iface_source_value, sorted(ap_untag),
        ]) if ap_modules else ""
        ap_now = time.time()
        ap_full_pass, ap_retry = _plan_ap_pass(ap_state, ap_fp, ap_full, ap_now) if ap_modules else (True, set())
        ap_pending: list[str] = []      # not found / failed this pass: retried on the next skipped one
        # uplink switch ids for the note links: once per pass, in bulk, only for the APs it touches
        ap_parsed = [_parse_ap_description(m.get("description") or "") for m in ap_modules]
        ap_parsed = [p for p in ap_parsed if p.get("hostname") and (ap_full_pass or p["hostname"] in ap_retry)]
        try:
            ap_uplink_ids.update(_resolve_ap_uplinks(nb, ap_parsed, log))
        except Exception as exc:
            log.warning("  AP uplink lookup failed (notes keep the search links): %s", exc)
        for ap_mod in ap_modules:
            parsed = _parse_ap_description(ap_mod.get("description") or "")
            hostname = parsed.get("hostname")
            if not ap_full_pass and (hostname or "") not in ap_retry:
                ap_counts["skipped"] += 1
                continue
            if not hostname:
                log.debug(
                    "  AP serial=%s: description didn't parse, skipping", ap_mod.get("serial", ""),
                )
                ap_counts["error"] += 1
                continue
            try:
                ap_dev = nb.find_ap_device(
                    hostname, serial=ap_mod.get("serial", ""), ip=parsed.get("ip") or "",
                )
            except Exception as exc:
                ap_counts["error"] += 1
                ap_pending.append(hostname)
                log.error("  AP %-20s lookup error: %s", hostname, exc)
                continue
            if not ap_dev:
                ap_counts["not_found"] += 1
                ap_pending.append(hostname)
                log.debug("  AP %-20s not found in Netbox: skipping", hostname)
                continue
            try:
                action = _update_ap_device(ap_dev, ap_mod, parsed)
                ap_counts[action] += 1
                if action == "updated":
                    log.debug(
                        "  AP %-20s → %s serial=%s updated (device=%s)",
                        hostname, ap_mod.get("model", ""), ap_mod.get("serial", ""), ap_dev.name,
                    )
            except Exception as exc:
                ap_counts["error"] += 1
                ap_pending.append(hostname)
                log.error("  AP %-20s update error: %s", hostname, exc)
        if ap_modules:
            _save_ap_state(nb_device.id, {
                "fingerprint": ap_fp,
                "full_at": ap_now if ap_full_pass else ap_state.get("full_at"),
                "pending": sorted(set(ap_pending)),
            })
            if not ap_full_pass:
                log.debug("APs: list unchanged since the last pass: skipped %d, retried %d",
                          ap_counts["skipped"], len(ap_modules) - ap_counts["skipped"])

        if ap_modules:
            log.debug(
                "APs: updated=%d unchanged=%d not_found=%d errors=%d",
                ap_counts["updated"], ap_counts["unchanged"], ap_counts["not_found"], ap_counts["error"],
            )
        # Controller side of the link: only when the controller CF exists on Device
        # (Netbox lists every defined custom field), otherwise the link would be empty.
        if ap_modules and cf_controller and cf_controller in (getattr(nb_device, "custom_fields", {}) or {}):
            new_comments = _merge_note_block(
                getattr(nb_device, "comments", "") or "", _wlc_note_block(cf_controller, nb_device.id), _WLC_NOTE_RE,
            )
            if new_comments is not None:
                try:
                    nb_device.update({"comments": new_comments})
                    device_changed = True
                except Exception as exc:
                    log.error("  WLC note update error: %s", exc)
        if ap_untyped:
            log.info(
                "APs: no DeviceType for model(s) %s: kept their current type; set the part_number on the "
                "right DeviceType in Netbox (or add a types.device_aliases entry)",
                ", ".join(f"{m} ({n})" for m, n in sorted(ap_untyped.items())),
            )

        # Supplement os_version from chassis sw_ver when the device field was empty
        # (e.g. Fortinet: "FortiGate-600F v7.4.8,build2795,250523 (GA.M)" → "7.4.8")
        if cf_os_version and not nd_device.get("os_ver"):
            for ch in chassis:
                ver = parse_sw_ver(ch.get("sw_ver", ""))
                if ver:
                    try:
                        if nb_device.update({"custom_fields": _merged_custom_fields(nb_device, {cf_os_version: ver})}):
                            device_changed = True
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
                if slot_to_device and is_ha_pair:
                    fan_target = _ha_member(fan) or nb_device
                elif slot_to_device:
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
                    current_bare_inventory_names_by_device.setdefault(fan_target.id, set()).add(fan_name)
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
            if slot_to_device and is_ha_pair:
                psu_target = _ha_member(psu) or nb_device
            elif slot_to_device:
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
                current_bare_inventory_names_by_device.setdefault(psu_target.id, set()).add(psu_name)
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
            _remove_misplaced_vss_interfaces(
                nb, vss_ifaces, set(slot_to_device), iface_source_cf, iface_source_value, log,
            )
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
        # own orphans always; source-less ones only with remove_orphans
        for name in _remove_orphaned_interfaces(
            nb, all_existing, nd_names, len(nd_ports), iface_source_cf, iface_source_value,
            orphan_delete_max_percent, log, include_unowned=remove_orphans, stats=orphan_stats,
        ):
            orphans_removed += 1
            device_changed = True
            existing_ifaces.pop(name, None)
            for m in vss_ifaces.values():
                m.pop(name, None)

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
            stack_cable_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
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
                    stack_cable_counts[action] += 1
                    current_bare_inventory_names_by_device.setdefault(cable_target.id, set()).add(cable_name)
                    if action != "unchanged":
                        log.debug("  Cable %-35s model=%s  serial=%s  %s",
                                  cable_name, cable_model, cable_serial, action)
                    else:
                        log.debug("  Cable %-35s unchanged", cable_name)
                except Exception as exc:
                    stack_cable_counts["error"] += 1
                    log.error("  Cable %-35s error: %s", cable_name, exc)
            log.debug(
                "StackCables: created=%d updated=%d unchanged=%d errors=%d",
                stack_cable_counts["created"], stack_cable_counts["updated"],
                stack_cable_counts["unchanged"], stack_cable_counts["error"],
            )
            if _counts_changed(stack_cable_counts):
                device_changed = True

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
        blade_patch_batch: list = []
        blade_names_by_id: dict[int, str] = {}
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
            if slot_to_device and is_ha_pair:
                # HA pair: the unit the blade sits in, from the ENTITY tree
                target_device = _ha_member(blade) or nb_device
                sw_match = None
            elif sw_match and slot_to_device:
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
                module_type = nb.get_or_create_module_type(mfr, blade_model, vendor=nd_device.get("vendor"))
                if not module_type:
                    blade_counts["unchanged"] += 1
                    continue
                bay = nb.upsert_module_bay(target_device, blade_name, position)
                action, blade_module = nb.upsert_module(
                    target_device, bay, module_type, blade_serial, batch=blade_patch_batch,
                )
                blade_counts[action] += 1
                if action == "updated" and blade_module:
                    blade_names_by_id[blade_module.id] = blade_name
                current_module_names_by_device.setdefault(target_device.id, set()).add(blade_name)
                log.debug("  Blade %s  model=%s  serial=%s  %s",
                          blade_name, blade_model, blade_serial, action)
            except Exception as exc:
                blade_counts["error"] += 1
                log.error("  Blade %-35s error: %s", blade_name, exc)

        if blade_patch_batch:
            failed_ids = nb.flush_module_updates(blade_patch_batch, log=log)
            for failed_id in failed_ids:
                blade_counts["updated"] -= 1
                blade_counts["error"] += 1
                log.error("  Blade %-35s failed to apply (see above)", blade_names_by_id.get(failed_id, failed_id))

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
        # own orphans always; source-less ones only with remove_orphans
        for name in _remove_orphaned_interfaces(
            nb, all_existing, nd_names, len(nd_ports), iface_source_cf, iface_source_value,
            orphan_delete_max_percent, log, include_unowned=remove_orphans, stats=orphan_stats,
        ):
            orphans_removed += 1
            device_changed = True
            existing_ifaces.pop(name, None)
            for m in vss_ifaces.values():
                m.pop(name, None)

    # Sort: parent interfaces before subinterfaces (dot-notation) so parents exist when children are created
    nd_ports_sorted = sorted(nd_ports, key=lambda p: (1 if "." in (p.get("port") or p.get("descr") or "") else 0))
    log.debug("Interfaces  entries: %d  existing: %d", len(nd_ports_sorted), len(existing_ifaces))

    suppress_description = False
    if _all_descriptions_missing(nd_ports):
        all_existing_ifaces = dict(existing_ifaces)
        for m in vss_ifaces.values():
            all_existing_ifaces.update(m)
        if any((getattr(iface, "description", None) or "").strip() for iface in all_existing_ifaces.values()):
            suppress_description = True
            log.warning(
                "All %d interface descriptions are empty in this Netdisco poll, but "
                "Netbox already has some set: looks like a degraded poll rather than "
                "a real config change — leaving existing descriptions untouched",
                len(nd_ports),
            )

    neighbors = neighbors_linked = 0
    seen_cable_iface_ids: set[int] = set()
    cable_counts: dict[str, int] = {"created": 0, "conflict": 0, "deleted": 0, "error": 0}
    iface_patch_batch: list = []
    iface_names_by_id: dict[int, str] = {}
    _fdb_cache: dict = {}

    def _port_fdb() -> dict:
        """port → active MACs from Netdisco's FDB, fetched once, only when a port needs it."""
        if "ports" not in _fdb_cache:
            by_port: dict[str, list[str]] = {}
            try:
                for node in nd.get_nodes(nd_ip) or []:
                    if node.get("active") and node.get("port") and node.get("mac"):
                        by_port.setdefault(node["port"], []).append(node["mac"])
            except Exception as exc:
                log.debug("  FDB fetch failed: %s", exc)
            _fdb_cache["ports"] = by_port
        return _fdb_cache["ports"]

    for port in nd_ports_sorted:
        iface_name = port.get("port") or port.get("descr") or "?"
        if iface_name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            log.debug("  %-40s blacklisted: skipping", iface_name)
            continue
        try:
            has_neighbor = bool(port.get("remote_ip") or port.get("remote_id"))
            nb_device_id, nb_iface_id = (
                _resolve_neighbor(nb, port.get("remote_ip", ""), port.get("remote_port", ""), port.get("remote_id") or "")
                if (cf_neighbor_device or cf_neighbor_iface or cable_scope) and has_neighbor
                else (None, None)
            )
            # A neighbor whose port couldn't be resolved (e.g. an AP on DHCP announcing
            # only its name via CDP): the switch's own FDB for this port may hold the
            # neighbor's MAC, which Netbox knows (an AP's Ethernet MAC on GigabitEthernet0).
            if nb_iface_id is None and has_neighbor and (cable_scope or cf_neighbor_iface):
                f_dev, f_iface = _resolve_neighbor_by_fdb(nb, _port_fdb().get(iface_name, []))
                if f_iface is not None and nb_device_id in (None, f_dev):
                    nb_device_id, nb_iface_id = f_dev, f_iface
                    log.debug("  %-40s neighbor resolved via FDB MAC", iface_name)
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
                suppress_description=suppress_description,
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
                target_id, nb_data,
                _match_existing_iface(
                    nb, target_existing, iface_name,
                    source_cf=iface_source_cf, source_value=iface_source_value,
                ),
                source_cf=iface_source_cf, source_value=iface_source_value,
                batch=iface_patch_batch,
            )
            counts[action] += 1
            if action == "updated" and nb_iface:
                iface_names_by_id[nb_iface.id] = iface_name
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
            elif cable_scope and nb_iface and (
                port.get("remote_ip") or port.get("remote_id") or port.get("remote_port")
            ):
                # The port reports a neighbor but it could not be resolved or
                # cabled this run (no mgmt IP announced, resolution failure,
                # cross-site link): failing to re-prove the neighbor is not
                # proof it is gone — never treat the existing cable as stale.
                seen_cable_iface_ids.add(nb_iface.id)

        except Exception as exc:
            counts["error"] += 1
            log.error("  %-40s error: %s", iface_name, exc)

    if iface_patch_batch:
        failed_ids = nb.flush_interface_updates(iface_patch_batch, log=log)
        for failed_id in failed_ids:
            counts["updated"] -= 1
            counts["error"] += 1
            log.error("  %-40s failed to apply (see above)", iface_names_by_id.get(failed_id, failed_id))

    if neighbors:
        log.debug("Neighbors  : found: %d  linked: %d  unresolved: %d",
                  neighbors, neighbors_linked, neighbors - neighbors_linked)

    # Delete stale cables: owned cables on interfaces whose port reports no
    # neighbor at all. Gated on lldp_clear_stale like the neighbor custom
    # fields: without it, absent LLDP data never destroys existing state —
    # crucial for asymmetric LLDP visibility, where the far side's sync would
    # otherwise delete the cable the near side just created.
    if cable_scope and cable_source_cf and lldp_clear_stale:
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
    if _counts_changed(cable_counts):
        device_changed = True

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
        iface = _iface_lookup(flat_ifaces, iface_name)
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

    # The management IP (the address we poll the device by) is always synced:
    # it anchors reachability, drives the dummy-interface move ("main" → real
    # port), and repairs a missing primary_ip4. sync_ip=False (no_ip) only
    # skips the remaining interface addresses.
    # Re-fetch interfaces so newly created ones have IDs.
    # For VSS, merge all member devices' interfaces so IPs can be assigned across both switches.
    if slot_to_device:
        existing_ifaces = {}
        for pos, dev in slot_to_device.items():
            existing_ifaces.update(nb.fetch_interfaces(dev.id))
    else:
        existing_ifaces = nb.fetch_interfaces(nb_device.id)
    try:
        nd_ips = nd.get_device_ips(nd_ip)
    except requests.HTTPError as exc:
        _reraise_if_gateway_error(exc)
        log.error("Could not fetch device IPs from Netdisco: %s", exc)
        nd_ips = []

    if not sync_ip:
        nd_ips = [
            e for e in nd_ips
            if str(e.get("alias") or e.get("ip") or "").split("/")[0] == ip
        ]

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
        iface = _iface_lookup(existing_ifaces, port_name)
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

    # Repair a missing primary IP: if the device has none (e.g. an interface
    # holding it was deleted in the past), claim the management IP just synced —
    # it must be assigned to one of this device's interfaces for Netbox to accept.
    if not getattr(nb_device, "primary_ip4", None):
        try:
            for ip_obj in nb.nb.ipam.ip_addresses.filter(address=ip):
                if str(ip_obj).split("/")[0] != ip:
                    continue
                if getattr(ip_obj, "assigned_object_type", None) != "dcim.interface":
                    continue
                dev_id = getattr(getattr(ip_obj.assigned_object, "device", None), "id", None)
                if dev_id != nb_device.id:
                    continue
                nb_device.update({"primary_ip4": ip_obj.id})
                log.info("  Primary IP restored: %s on %r", ip_obj.address, nb_device.name)
                break
        except Exception as exc:
            log.warning("  Could not restore primary IP %s: %s", ip, exc)

    # ── SFPs ──────────────────────────────────────────────────────────────────────

    if sync_sfp:
        # nd_mods already fetched if sync_modules ran; fetch only if needed
        if not sync_modules:
            try:
                nd_mods = nd.get_modules(nd_ip)
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
                current_sfp_iface_ids.add(iface.id)
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
            powered_ports = nd.get_powered_ports(nd_ip)
        except requests.HTTPError as exc:
            _reraise_if_gateway_error(exc)
            log.warning("Could not fetch powered ports (device may not support PoE): %s", exc)
            powered_ports = []

        if powered_ports:
            existing_ifaces = nb.fetch_interfaces(nb_device.id)
            _apply_poe(nb, existing_ifaces, powered_ports, poe_counts, log)

            log.debug(
                "PoE: updated=%d unchanged=%d skipped=%d errors=%d",
                poe_counts["updated"], poe_counts["unchanged"], poe_counts["skipped"], poe_counts["error"],
            )

    if not device_changed:
        for _name in ("counts", "ip_counts", "mod_counts", "sfp_counts",
                      "fan_counts", "psu_counts", "blade_counts",
                      "mod_iface_counts", "poe_counts"):
            if _counts_changed(locals().get(_name)):
                device_changed = True
                break

    prune_counts: dict = {}
    if prune:
        prune_counts = {
            "interfaces_deleted": 0,
            "modules_deleted": 0,
            "modules_skipped_foreign": 0,
            "inventory_deleted": 0,
            "sfps_deleted": 0,
            "stack_members": stack_members_prune_result,
        }
        devices_to_prune = {nb_device.id: nb_device}
        for dev in slot_to_device.values():
            devices_to_prune[dev.id] = dev
        for dev in devices_to_prune.values():
            prune_counts["interfaces_deleted"] += nb.remove_stale_interfaces(
                dev, nd_port_names, iface_source_cf, iface_source_value, dry_run=dry_run,
            )
            # Modules/fans/PSUs/SFPs are only pruned when this run actually synced
            # them — current_*_by_device would otherwise be empty (nothing "current"
            # was recorded this run) and every existing item would look stale.
            if sync_modules:
                mod_deleted, mod_skipped = nb.remove_stale_modules(
                    dev, current_module_names_by_device.get(dev.id, set()),
                    iface_source_cf, iface_source_value, dry_run=dry_run,
                )
                prune_counts["modules_deleted"] += mod_deleted
                prune_counts["modules_skipped_foreign"] += mod_skipped
                prune_counts["inventory_deleted"] += nb.remove_stale_bare_inventory_items(
                    dev, current_bare_inventory_names_by_device.get(dev.id, set()), dry_run=dry_run,
                )
            if sync_sfp:
                prune_counts["sfps_deleted"] += nb.remove_stale_sfps(
                    dev, current_sfp_iface_ids, dry_run=dry_run,
                )
        log.info(
            "Prune%s: interfaces=%d modules=%d (skipped_foreign=%d) inventory=%d sfps=%d",
            " [dry-run]" if dry_run else "",
            prune_counts["interfaces_deleted"], prune_counts["modules_deleted"],
            prune_counts["modules_skipped_foreign"], prune_counts["inventory_deleted"],
            prune_counts["sfps_deleted"],
        )
        if not dry_run and any(
            prune_counts[k] for k in ("interfaces_deleted", "modules_deleted", "inventory_deleted", "sfps_deleted")
        ):
            device_changed = True

    stamps: dict = {}
    if cf_touch and device_changed:
        stamps[cf_touch] = date.today().isoformat()
    # Rebuild stamp: only when the CF exists on Device (Netbox lists every defined CF)
    if (prune and not dry_run and cf_inventory_rebuild
            and cf_inventory_rebuild in (getattr(nb_device, "custom_fields", {}) or {})):
        stamps[cf_inventory_rebuild] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if stamps:
        try:
            nb_device.update({"custom_fields": _merged_custom_fields(nb_device, stamps)})
            log.debug("  %s updated", ", ".join(stamps))
        except Exception as exc:
            log.error("  %s update error: %s", ", ".join(stamps), exc)

    def _fmt(label: str, c: dict) -> Optional[str]:
        changed = c.get("created", 0) + c.get("updated", 0)
        errors = c.get("error", 0)
        if not changed and not errors:
            return None
        s = f"{label}=+{c.get('created', 0)}/~{c.get('updated', 0)}"
        if errors:
            s += f"/!{errors}"
        return s

    def _fmt_cables(c: dict) -> Optional[str]:
        # neighbor cables (created incl. replaced stale ones, deleted, conflicts left alone);
        # cable errors stay out of errors= (logged as "not counted in errors")
        if not (c["created"] or c["deleted"] or c["conflict"]):
            return None
        s = f"cables=+{c['created']}/-{c['deleted']}"
        return s + (f"/conflict={c['conflict']}" if c["conflict"] else "")

    # stack_cable_counts only exists in locals() when this run's chassis data
    # actually included any (gated behind `if stack_cables:` above).
    _stack_cable_counts = locals().get("stack_cable_counts")
    total_errors = sum(
        c.get("error", 0)
        for c in [counts, ip_counts, mod_counts, sfp_counts, poe_counts, ap_counts,
                  locals().get("fan_counts") or {}, locals().get("psu_counts") or {}]
    ) + (_stack_cable_counts.get("error", 0) if _stack_cable_counts else 0)
    # plus errors only logged (note updates, …): the higher of the two, as most
    # counted errors are logged too
    total_errors = max(total_errors, getattr(_sync_error_tls, "count", None) or 0)
    def _fmt_aps(c: dict) -> Optional[str]:
        # Always shown for a WLC: an all-unchanged AP run is still worth seeing.
        total = sum(c.values())
        if not total:
            return None
        s = f"aps={total}/~{c['updated']}"
        if c["not_found"]:
            s += f"/?{c['not_found']}"
        if c["error"]:
            s += f"/!{c['error']}"
        if c["skipped"]:
            s += f" (unchanged: {c['skipped']} skipped)"
        return s

    parts = list(filter(None, [
        _fmt("ifaces", counts),
        _fmt("ips", ip_counts),
        _fmt("mods", mod_counts) if sync_modules else None,
        _fmt("sfps", sfp_counts) if sync_sfp else None,
        _fmt_aps(ap_counts) if sync_modules else None,
        _fmt("stackcables", _stack_cable_counts) if _stack_cable_counts else None,
        _fmt_cables(cable_counts),
        f"orphans=-{orphans_removed}" if orphans_removed else None,
    ]))
    summary = ("  " + "  ".join(parts)) if parts else "  no changes"
    errors_suffix = f"  errors={total_errors}" if total_errors else ""
    log.info(
        "sync done%s %s%s%s", " (rebuild)" if prune else "", nb_device.name, summary, errors_suffix,
    )
    return {
        "ok": counts["error"] == 0,
        "errors": total_errors,   # every part (interfaces, IPs, modules, SFPs, PoE, APs, stack cables)
        "hostname": nb_device.name,
        "interfaces": counts,
        "ips": ip_counts,
        "modules": mod_counts if sync_modules else {},
        "sfps": sfp_counts if sync_sfp else {},
        "aps": ap_counts if sync_modules else {},
        "ha_vip": vip_device is not None,
        "orphans": {"removed": orphans_removed, "braked": orphan_stats["braked"]},
        **({"prune": prune_counts} if prune else {}),
    }
