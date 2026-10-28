"""Interfaces: Netdisco port → Netbox interface, matching, PoE, orphan removal."""

import functools
import logging
import re
from typing import Optional

from .netbox import NetboxClient
from .util import (
    NO_POE_IFACE_TYPES,
    PORT_BLACKLIST_PREFIXES,
    _delete_with_retry,
    _slot_from_iface,
    clean_mac,
    map_iftype,
    parse_speed_kbps,
)

ORPHAN_DELETE_MIN_ALLOWED = 5    # small devices: this many orphans are always fine


def _remove_orphaned_interfaces(
    nb, existing: dict, nd_names: set, nd_port_count: int,
    source_cf: Optional[str], source_value: str, max_percent: int, log,
    include_unowned: bool = False, stats: Optional[dict] = None,
) -> list[str]:
    """
    Delete Netbox interfaces Netdisco doesn't report ("orphans"), returning
    the deleted names. By source_cf: discobox's own (source_value, i.e.
    created or adopted by discobox) always; unset (imported/manual) only with
    include_unowned (sync.remove_orphaned_interfaces); another source's never.
    Without source_cf, ownership can't be told apart: include_unowned decides
    alone. Never one with a cable or an IP.
    Mass-deletion brake: nothing is deleted when Netdisco reports no ports at
    all, or when the deletable orphans exceed max_percent of the device's
    interfaces (and ORPHAN_DELETE_MIN_ALLOWED) — a failed or partial discovery
    must not wipe a device.
    Exempt from the brake: discobox's own digit-only names ("17", an ifIndex
    from a failed ifName walk) when Netdisco reports ports and none of them
    digit-only — on devices that really name ports "1".."48" (HP/Aruba) the
    brake still holds.
    stats, if given, gets "braked": the orphans the brake kept.
    """
    nd_lower = {str(n).lower() for n in nd_names if n}
    nd_digit_names = any(str(n).strip().isdigit() for n in nd_names if n)
    candidates = []
    ifindex_junk = []
    unowned_kept: list[str] = []
    for name, iface in existing.items():
        lname = name.lower()
        if lname in nd_lower or lname.startswith(PORT_BLACKLIST_PREFIXES):
            continue
        if source_cf:
            owner = dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) or ""
            if owner and owner != source_value:
                log.debug("  %-40s orphan kept: owned by %r", name, owner)
                continue
            if not owner and not include_unowned:
                if not getattr(iface, "cable", None):
                    unowned_kept.append(name)
                continue
        elif not include_unowned:
            continue
        if getattr(iface, "cable", None):
            log.debug("  %-40s orphan kept: has a cable", name)
            continue
        if (source_cf and nd_port_count and not nd_digit_names and name.strip().isdigit()
                and (dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) or "") == source_value):
            ifindex_junk.append((name, iface))
            continue
        candidates.append((name, iface))
    if unowned_kept:
        # preview for sync.remove_orphaned_interfaces: what it would remove here
        # (before the IP check and the brake, so an upper bound)
        log.info(
            "  %d source-less orphan(s) kept (sync.remove_orphaned_interfaces would remove them): %s",
            len(unowned_kept), ", ".join(sorted(unowned_kept)[:10]) + (" …" if len(unowned_kept) > 10 else ""),
        )
    limit = max(ORPHAN_DELETE_MIN_ALLOWED, len(existing) * max_percent // 100)
    if candidates and (nd_port_count == 0 or len(candidates) > limit):
        log.warning(
            "  Orphan cleanup skipped: %d of %d interfaces would go (Netdisco ports: %d, limit %d) — "
            "discovery incomplete? Check the device, or run a rebuild",
            len(candidates), len(existing), nd_port_count, limit,
        )
        if stats is not None:
            stats["braked"] = stats.get("braked", 0) + len(candidates)
        candidates = []
    if ifindex_junk:
        log.info("  %d digit-only orphan(s) (ifIndex as name?), not braked: %s", len(ifindex_junk),
                 ", ".join(sorted((n for n, _ in ifindex_junk), key=lambda n: int(n))[:10])
                 + (" …" if len(ifindex_junk) > 10 else ""))
    deleted: list[str] = []
    for name, iface in ifindex_junk + candidates:
        # Netbox cascades an interface delete to its IPs: the device would lose them
        if list(nb.nb.ipam.ip_addresses.filter(assigned_object_type="dcim.interface", assigned_object_id=iface.id)):
            log.warning("  %-40s not in Netdisco but has IP(s) assigned: keeping", name)
            continue
        if _delete_with_retry(iface, f"orphan {name!r}", log):
            deleted.append(name)
            log.debug("  %-40s deleted (not in Netdisco)", name)
    if deleted:
        log.info("  Removed %d orphaned interface(s) (not in Netdisco)", len(deleted))
    return deleted


def _apply_poe(nb, existing_ifaces: dict, powered_ports: list[dict], poe_counts: dict, log) -> None:
    """poe_mode=pse on every interface Netdisco reports as powered (PSE)."""
    poe_iface_names = {p["port"] for p in powered_ports if p.get("port")}
    for port_name in poe_iface_names:
        iface = _iface_lookup(existing_ifaces, port_name)
        if not iface:
            poe_counts["skipped"] += 1
            continue
        # Netbox rejects PoE on virtual/bridge/LAG interfaces; Netdisco can still
        # list one as powered (a PSE port index mapped to the wrong ifIndex)
        if nb._nb_value(getattr(iface, "type", None)) in NO_POE_IFACE_TYPES:
            poe_counts["skipped"] += 1
            log.debug("  PoE %s skipped: Netdisco reports power on a %s interface",
                      port_name, nb._nb_value(iface.type))
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


def _remove_misplaced_vss_interfaces(
    nb, vss_ifaces: dict[int, dict], positions: set, source_cf: Optional[str], source_value: str, log,
) -> int:
    """
    Delete interfaces sitting on the wrong VSS member device (the switch number
    in the name says another member), so the sync recreates them on the right
    one. Unlike orphans these aren't gone from Netdisco, only misplaced, so a
    source-less one goes too. Kept (with a warning): one owned by another
    source, one with a cable or an IP, which the delete would lose. Subinterfaces
    first so Netbox's cascade doesn't 404 the child. Returns the number deleted.
    """
    removed = 0
    for pos, dev_ifaces in vss_ifaces.items():
        misplaced = [
            (name, iface, owner_pos)
            for name, iface in dev_ifaces.items()
            if (owner_pos := _slot_from_iface("vss", name)) is not None
            and owner_pos != pos and owner_pos in positions
        ]
        misplaced.sort(key=lambda x: (0 if "." in x[0] else 1))
        for name, iface, owner_pos in misplaced:
            owner = dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) if source_cf else None
            if owner and owner != source_value:
                log.warning("  %-40s on Switch %d, belongs on Switch %d: kept (owned by %r)",
                            name, pos, owner_pos, owner)
                continue
            if getattr(iface, "cable", None):
                log.warning("  %-40s on Switch %d, belongs on Switch %d: kept (has a cable, move it by hand)",
                            name, pos, owner_pos)
                continue
            if list(nb.nb.ipam.ip_addresses.filter(assigned_object_type="dcim.interface", assigned_object_id=iface.id)):
                log.warning("  %-40s on Switch %d, belongs on Switch %d: kept (has IPs, move them by hand)",
                            name, pos, owner_pos)
                continue
            if _delete_with_retry(iface, f"misplaced {name!r} on Switch {pos}", log):
                del dev_ifaces[name]
                removed += 1
                log.debug("  %-40s moved from Switch %d → Switch %d", name, pos, owner_pos)
    return removed


def port_to_netbox(
    port: dict,
    lldp_clear_stale: bool = False,
    neighbor_device_id: Optional[int] = None,
    neighbor_iface_id: Optional[int] = None,
    cf_neighbor_text: Optional[str] = "neighbor",
    cf_neighbor_port: Optional[str] = "neighbor_port",
    cf_neighbor_device: Optional[str] = "neighbor_device",
    cf_neighbor_iface: Optional[str] = "neighbor_iface",
    suppress_description: bool = False,
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
      up                operational state; only gates speed (never written to Netbox
                         itself) — ifSpeed is unreliable while a link is down (some
                         platforms report 0 rather than the nominal speed), so a flap
                         must not clobber the last known-good value.

    suppress_description (set device-wide by sync_device via _all_descriptions_missing,
    when this poll reports every alias empty despite Netbox already having real
    descriptions — a likely degraded Netdisco poll) skips writing description at
    all rather than wiping every interface's description to empty.
    """
    speed_kbps = None if (port.get("up") or "").lower() == "down" else parse_speed_kbps(port.get("speed"))

    raw_duplex = port.get("duplex") or port.get("duplex_admin")
    duplex = raw_duplex if raw_duplex in ("full", "half", "auto") else None

    # Use ifAlias as description only when it differs from the interface name
    full_name = port.get("port") or port.get("descr") or ""
    alias = port.get("name", "")
    description = (
        None if suppress_description
        else (alias if alias and alias.lower() != full_name.lower() else "")
    )

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


def _iface_lookup(existing: dict, name: str):
    """
    Get an interface by name with a case-insensitive fallback: Netdisco's
    ports, device_ips and powered_ports tables can disagree on the case of
    the same port name (e.g. 'loopback3899' vs 'Loopback3899').
    """
    if name in existing:
        return existing[name]
    lower = str(name).lower()
    for k, v in existing.items():
        if k.lower() == lower:
            return v
    return None


def _match_existing_iface(
    nb: "NetboxClient",
    existing: dict,
    name: str,
    source_cf: Optional[str] = None,
    source_value: str = "netdisco",
):
    """
    Pick the existing Netbox interface for a Netdisco port name.

    Exact match wins; a case-variant (manually created 'loopback3899' vs
    Netdisco's 'Loopback3899') is adopted — the upsert diff then renames it
    to the canonical spelling and claims it via the source CF. When both an
    exact and a case-variant exist (duplicate created before case-insensitive
    matching), the one holding IPs is kept and empty non-foreign duplicates
    are deleted.

    Returns the record to update, or None to create fresh. `existing` is
    re-keyed under the canonical name when a match is found.
    """
    log = logging.getLogger("discobox.sync")
    exact = existing.get(name)
    variant_keys = [k for k in existing if k.lower() == name.lower() and k != name]
    if not variant_keys:
        return exact
    candidates = ([exact] if exact is not None else []) + [existing[k] for k in variant_keys]

    def _has_ips(iface) -> bool:
        return bool(list(nb.nb.ipam.ip_addresses.filter(
            assigned_object_type="dcim.interface", assigned_object_id=iface.id,
        )))

    with_ips = [c for c in candidates if _has_ips(c)]
    if len(with_ips) > 1:
        keeper = exact if exact is not None else candidates[0]
        log.warning(
            "  %-40s %d case-duplicates hold IPs: keeping %r, merging nothing",
            name, len(with_ips), keeper.name,
        )
        return keeper
    keeper = with_ips[0] if with_ips else (exact if exact is not None else candidates[0])

    for c in candidates:
        if c.id == keeper.id:
            continue
        if source_cf:
            owner = (dict(getattr(c, "custom_fields", {}) or {}).get(source_cf) or "")
            if owner and owner != source_value:
                log.debug("  %-40s keeping case-duplicate %r: owned by %r", name, c.name, owner)
                continue
        try:
            c.delete()
            existing.pop(c.name, None)
            log.info("  %-40s merged: deleted empty case-duplicate %r", name, c.name)
        except Exception as exc:
            log.warning("  %-40s could not delete case-duplicate %r: %s", name, c.name, exc)

    for k in variant_keys:
        if k in existing and existing[k].id == keeper.id:
            existing.pop(k)
    existing[name] = keeper
    return keeper


# Netbox interface types that aren't a physical port: interface defaults with
# physical_only skip them (virtual covers SVIs, tunnels, loopbacks, subinterfaces).
NON_PHYSICAL_IFACE_TYPES = frozenset({"virtual", "bridge", "lag"})


def _is_physical_iface(iface) -> bool:
    t = getattr(getattr(iface, "type", None), "value", None) or ""
    return t not in NON_PHYSICAL_IFACE_TYPES and not t.startswith("ieee802.11") and t != "other-wireless"


def _get_path(obj, dotted: str):
    """A pynetbox Record or nested dict by dot-notation: status.value, role.slug, custom_fields.team."""
    for part in dotted.split("."):
        if obj is None:
            return None
        obj = obj.get(part) if isinstance(obj, dict) else getattr(obj, part, None)
    return obj


def _match_strings(raw) -> list[str]:
    """The value(s) a pattern is matched against; list fields (tags, multi-select) by element, slug first."""
    if isinstance(raw, (list, tuple)):
        return [str((item.get("slug") if isinstance(item, dict) else getattr(item, "slug", None)) or item)
                for item in raw]
    return ["" if raw is None else str(raw)]


@functools.lru_cache(maxsize=256)
def _pattern(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.IGNORECASE)


def interface_defaults_for(device, rules: list[dict], log=None) -> list[dict]:
    """
    The rules whose match all fit this device (the snmp-modulator semantics):
    every dotted path's value fullmatches its pattern, case-insensitive; a list
    field (tags, multi-select) matches when any element does.
    """
    matching = []
    for i, rule in enumerate(rules):
        label = rule.get("name") or f"#{i + 1}"
        ok = True
        for path, pattern in rule["match"].items():
            values = _match_strings(_get_path(device, path))
            hit = any(_pattern(pattern).fullmatch(v) for v in values)
            if log:
                log.debug("Interface defaults: rule [%s] %s %s pattern=%r value=%r",
                          label, "MATCH" if hit else "MISS ", path, pattern,
                          values if len(values) != 1 else values[0])
            if not hit:
                ok = False
                break
        if ok:
            matching.append(rule)
    return matching


def apply_interface_defaults(nb, ifaces: dict, rules: list[dict], log) -> dict[str, int]:
    """
    Fill each rule's set fields where they're empty (None or ""), never
    overwriting a value (a deliberate "none" included), on every interface of
    the device regardless of who owns it; per field the first rule wins;
    physical_only skips virtual, LAG, bridge and wireless ones. One bulk PATCH
    per device. Returns {field: interfaces set}.
    """
    counts: dict[str, int] = {}
    batch: list[dict] = []
    for iface in ifaces.values():
        current = dict(getattr(iface, "custom_fields", {}) or {})
        patch = {}
        decided: set[str] = set()
        for rule in rules:
            if rule.get("physical_only", True) and not _is_physical_iface(iface):
                continue
            for field, value in rule["set"].items():
                if field in decided:
                    continue
                decided.add(field)            # the first rule that covers the interface owns the field
                if current.get(field) in (None, ""):
                    patch[field] = value
        if patch:
            batch.append({"id": iface.id, "custom_fields": {**current, **patch}})
            for field in patch:
                counts[field] = counts.get(field, 0) + 1
    if batch:
        failed = nb.flush_interface_updates(batch, log)
        if failed:
            log.error("Interface defaults: %d interface(s) failed", len(failed))
        log.info("Interface defaults set: %s", "  ".join(f"{f}={n}" for f, n in sorted(counts.items())))
    return counts
