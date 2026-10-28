"""HA pairs and VIP devices (Fortinet FGCP, VSS, ...): member resolution, notes, peers."""

import logging
import re
from typing import Optional

import pynetbox
import requests

from .netbox import NetboxClient

HA_NOTE_BEGIN = "<!-- discobox:ha -->"
HA_NOTE_END = "<!-- /discobox:ha -->"
_HA_NOTE_RE = re.compile(re.escape(HA_NOTE_BEGIN) + r".*?" + re.escape(HA_NOTE_END), re.DOTALL)
_HA_CREATED_LINE = " - Created by discobox from the controller's ENTITY-MIB chassis list"


def _ha_note_block(primary_name: str, pos: int, created: bool, location_missing: bool) -> str:
    """discobox-owned comments block on an HA peer device (chassis <pos> of primary_name)."""
    lines = [HA_NOTE_BEGIN, "## HA peer", f" - Chassis {pos} of {primary_name} (HA pair, Virtual Chassis)"]
    if created:
        lines.append(_HA_CREATED_LINE)
    if location_missing:
        lines.append(" - ⚠ Location/rack not known to discobox: set them manually")
    lines.append(HA_NOTE_END)
    return "\n".join(lines)


def _chassis_pos_of(entry: dict, by_index: dict) -> Optional[int]:
    """Position of the chassis an ENTITY-MIB entry sits under (walking parents), or None."""
    seen: set = set()
    cur: Optional[dict] = entry
    while cur is not None and cur.get("index") not in seen:
        if cur.get("class") == "chassis":
            return cur.get("pos")
        seen.add(cur.get("index"))
        cur = by_index.get(cur.get("parent"))
    return None


def _ha_peer_name(primary_name: str, pos: int) -> str:
    """ "wlc1.example.com", 2 → "wlc1-2.example.com" (the "-<pos>" peer naming convention)."""
    short, dot, domain = primary_name.partition(".")
    return f"{short}-{pos}{dot}{domain}"


def _pick_primary_chassis(chassis: list[dict], device_serial: str) -> dict:
    """
    The chassis entry that is the synced device itself. A single matching serial
    decides; an HA pair's logical device reports every member's serial ("A B"),
    so then the lowest position wins: chassis 1 stays the same Netbox device
    across switchovers (roles move, positions don't).
    """
    wanted = set((device_serial or "").split())
    own = [c for c in chassis if c.get("serial") and c.get("serial") in wanted]
    if len(own) == 1:
        return own[0]
    return min(chassis, key=lambda c: c.get("pos") if isinstance(c.get("pos"), int) else 1 << 30)


def _ha_node_info(short: str) -> Optional[tuple[int, str, str]]:
    """Parse an HA node indicator from a short hostname.

    Returns (node_num, vc_base, vip_short) or None when no indicator matches:
      "fwha0001p1h" → (1, "fwha0001", "fwha0001p0h")
      "fw-node2"      → (2, "fw", "fw-node0")
      "mgmtha0001p1v"  → (1, "mgmtha0001", "mgmtha0001p0v")
    """
    m = re.search(r"(p|node|-)([12])([a-z]?)", short, re.IGNORECASE)
    if not m:
        return None
    node_num = int(m.group(2))
    vc_base = (short[: m.start(1)] + short[m.end(3):]).rstrip("-_") or short
    vip_short = short[: m.start(2)] + "0" + short[m.end(2):]
    return node_num, vc_base, vip_short


def _is_own_vip(member_name: str, other_name: str) -> bool:
    """True when other_name is the VIP of cluster member member_name ("x0001p1h" → "x0001p0h")."""
    info = _ha_node_info((member_name or "").split(".")[0])
    return bool(info) and (other_name or "").split(".")[0].lower() == info[2].lower()


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


def _query_ha_metric(
    url: str,
    label: str,
    value: str,
    metric: str = "fgHaStatsSyncStatus_info",
    target_label: str = "netbox_primary_ip",
    timeout: int = 15,
    verify_tls: bool = True,
) -> Optional[str]:
    """
    Query a Prometheus-compatible API for `metric{label="value"}` and return
    the first matching series' target_label — e.g. resolving either a
    vendor-reported hostname (fgHaStatsHostname) or an already-known NetBox
    device FQDN (instance) to the correct netbox_primary_ip, via a metric
    whose labels already carry that resolved identity (attached by the
    scrape/relabeling pipeline).

    Returns None when nothing matches. Raises on HTTP or payload errors —
    the caller decides fail-open behavior.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    query = f'{metric}{{{label}="{escaped}"}}'
    resp = requests.get(
        f"{url.rstrip('/')}/api/v1/query",
        params={"query": query},
        timeout=timeout,
        verify=verify_tls,
        headers={"Accept": "application/json", "User-Agent": "discobox"},
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"HA metric query failed: {payload.get('error') or 'unknown error'}")
    for series in payload.get("data", {}).get("result", []):
        result = series.get("metric", {}).get(target_label)
        if result:
            return str(result).split("/")[0]
    return None


def _resolve_ha_member_ip(
    url: str,
    hostname: str,
    metric: str = "fgHaStatsSyncStatus_info",
    hostname_label: str = "fgHaStatsHostname",
    target_label: str = "netbox_primary_ip",
    timeout: int = 15,
    verify_tls: bool = True,
) -> Optional[str]:
    """
    Resolve a vendor-reported HA cluster member hostname (e.g. a FortiGate's
    own `hostname`, which usually has no relation to its Netbox device name)
    to the correct Netbox device IP. See _query_ha_metric for details.
    """
    return _query_ha_metric(
        url, hostname_label, hostname,
        metric=metric, target_label=target_label, timeout=timeout, verify_tls=verify_tls,
    )


def _guess_prefix_len(*devices: object) -> str:
    """Borrow a prefix length from the first device with an existing primary_ip4 (HA
    members share a subnet); "32" (a bare host route) when none is known."""
    for device in devices:
        primary = getattr(device, "primary_ip4", None) if device else None
        if primary and "/" in str(primary):
            return str(primary).split("/")[-1]
    return "32"


def _ensure_member_ip(
    nb: "NetboxClient",
    device: Optional[pynetbox.core.response.Record],
    target_ip: Optional[str],
    prefix_len: str,
    iface_name: str,
    log: logging.Logger,
    source_cf: Optional[str] = None,
    source_value: str = "netdisco",
) -> bool:
    """
    Ensure `device` has target_ip assigned to one of its interfaces and set
    as primary_ip4. Creates `iface_name` if the device has none yet.

    No-op if the device already has any primary_ip4 (never second-guesses an
    existing assignment) or target_ip is falsy. Never steals an IP already
    claimed by a different device's interface — upsert_ip's own safeguard
    (marks it role=vip and skips instead).
    """
    if not device or not target_ip or getattr(device, "primary_ip4", None):
        return False

    ifaces = list(nb.nb.dcim.interfaces.filter(device_id=device.id))
    iface = ifaces[0] if ifaces else None
    if not iface:
        iface = nb.nb.dcim.interfaces.create(device=device.id, name=iface_name, type="other")
        if source_cf:
            iface.update({"custom_fields": {source_cf: source_value}})
        log.info("  Created management interface %r on %r", iface_name, device.name)

    action = nb.upsert_ip(f"{target_ip}/{prefix_len}", iface)
    if action not in ("created", "moved", "fixed"):
        return False  # already elsewhere / unchanged — upsert_ip already logged why

    ip_obj = next(iter(nb.nb.ipam.ip_addresses.filter(address=target_ip)), None)
    if not ip_obj or nb._nb_value(getattr(ip_obj, "assigned_object_id", None)) != iface.id:
        return False
    device.update({"primary_ip4": ip_obj.id})
    log.info("  Management IP %s set as primary for %r", target_ip, device.name)
    return True


def _ensure_ha_member_ip(
    nb: "NetboxClient",
    device: Optional[pynetbox.core.response.Record],
    prefix_len: str,
    ha_metrics_url: str,
    instance_label: str,
    metric: str,
    target_label: str,
    timeout: int,
    verify_tls: bool,
    mgmt_iface_name: str,
    log: logging.Logger,
    source_cf: Optional[str] = None,
    source_value: str = "netdisco",
) -> bool:
    """
    Ensure `device` has its own management IP — resolved by its NetBox FQDN
    via the same HA-metrics endpoint used for member identification — assigned
    to one of its interfaces and set as primary_ip4. See _ensure_member_ip.

    No-op if the device already has a primary_ip4, the metric has no row for
    it, or the lookup fails.
    """
    if not device or not device.name or getattr(device, "primary_ip4", None):
        return False
    try:
        target_ip = _query_ha_metric(
            ha_metrics_url, instance_label, device.name,
            metric=metric, target_label=target_label, timeout=timeout, verify_tls=verify_tls,
        )
    except Exception as exc:
        log.warning("HA management-IP lookup for %r failed: %s", device.name, exc)
        return False
    return _ensure_member_ip(nb, device, target_ip, prefix_len, mgmt_iface_name, log, source_cf, source_value)


def _create_ha_peer(
    nb: "NetboxClient", primary, name: str, serial: str,
    source_cf: Optional[str], source_value: str,
):
    """
    Create the Netbox device for another unit of an HA pair, copying role,
    site, tenant, status and DeviceType from the primary. Location and rack
    stay empty: discobox can't know them (the HA note says so).
    """
    payload: dict = {
        "name": name,
        "device_type": primary.device_type.id,
        "role": primary.role.id,
        "site": primary.site.id,
        "status": getattr(getattr(primary, "status", None), "value", None) or "active",
        "serial": serial or "",
    }
    tenant = getattr(primary, "tenant", None)
    if tenant:
        payload["tenant"] = tenant.id
    cf = dict(getattr(primary, "custom_fields", {}) or {})
    if source_cf and source_cf in cf:
        payload["custom_fields"] = {source_cf: source_value}
    return nb.nb.dcim.devices.create(**payload)
