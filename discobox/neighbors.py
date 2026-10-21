"""Neighbor resolution (LLDP/CDP, FDB, MAC, IP) for cables and neighbor fields."""

import logging
from typing import Optional

from .netbox import NetboxClient
from .util import _MAC_RE, clean_mac, expand_iface_name


def _resolve_neighbor_by_mac(nb: "NetboxClient", remote_id: str) -> tuple[Optional[int], Optional[int]]:
    """
    (device_id, interface_id) of the one Netbox interface carrying remote_id as
    a MAC address, else (None, None). LLDP neighbors whose chassis ID is a MAC
    (e.g. Cisco APs: their Ethernet MAC, which the WLC sync puts on
    GigabitEthernet0) resolve this way even without any IP in Netbox, and
    without matching their abbreviated port name ("Gi0").
    """
    hits = _resolve_mac_hits(nb, remote_id)
    if len(hits) != 1:
        logging.getLogger("discobox.sync").debug("  neighbor resolve  mac=%s  interface hits=%d", remote_id, len(hits))
        return None, None
    return hits[0]


def _resolve_neighbor(
    nb: "NetboxClient", remote_ip: str, remote_port: str, remote_id: str = "",
) -> tuple[Optional[int], Optional[int]]:
    """
    Resolve a neighbor (remote_ip, remote_port, remote_id) to (device_id, interface_id).

    Strategy:
    1. Find device by primary_ip4 match (common case).
    2. Fall back to any IP address assignment (covers VLAN/loopback IPs on L3 devices).
    Once the device is found, look up the interface by name.
    3. When that yields no interface, the LLDP chassis ID (remote_id) as a MAC
       on exactly one Netbox interface (see _resolve_neighbor_by_mac); it only
       replaces an IP hit when it lands on the same device.
    4. Otherwise, a non-MAC chassis/device ID (CDP device ID, LLDP system
       name, e.g. an AP on DHCP announcing "BERN-W32") as the name of exactly
       one Netbox device, full or short name (see _resolve_neighbor_by_name).
    Interface names also match in abbreviated form ("Gi0" → GigabitEthernet0).
    Either value may be None independently.
    """
    dev_id, iface_id = _resolve_neighbor_by_ip(nb, remote_ip, remote_port)
    if iface_id is None and remote_id:
        mac_dev, mac_iface = _resolve_neighbor_by_mac(nb, remote_id)
        if mac_iface is not None and dev_id in (None, mac_dev):
            return mac_dev, mac_iface
        if not _MAC_RE.match(remote_id.strip()):
            name_dev, name_iface = _resolve_neighbor_by_name(nb, remote_id, remote_port)
            if name_dev is not None and dev_id in (None, name_dev):
                return name_dev, name_iface
    return dev_id, iface_id


FDB_MAX_MACS_PER_PORT = 4


def _resolve_neighbor_by_fdb(nb: "NetboxClient", macs: list[str]) -> tuple[Optional[int], Optional[int]]:
    """
    (device_id, interface_id) from the MACs a switch learned on a port: exactly
    one of them must sit on exactly one Netbox interface (an AP's Ethernet MAC
    on its GigabitEthernet0). Ports with more than FDB_MAX_MACS_PER_PORT MACs
    (uplinks, hosts behind a switch) are never used.
    """
    macs = list(dict.fromkeys(m for m in macs if m))
    if not macs or len(macs) > FDB_MAX_MACS_PER_PORT:
        return None, None
    found: dict[int, int] = {}      # interface id → device id
    for mac in macs:
        for hit in _resolve_mac_hits(nb, mac):
            found[hit[1]] = hit[0]
    if len(found) != 1:
        return None, None
    iface_id, dev_id = next(iter(found.items()))
    return dev_id, iface_id


def _resolve_mac_hits(nb: "NetboxClient", mac: str) -> list[tuple[int, int]]:
    """(device_id, interface_id) of every Netbox interface carrying mac."""
    mac = clean_mac(mac) if mac and _MAC_RE.match(mac.strip()) else None
    if not mac:
        return []
    out = []
    for m in nb.nb.dcim.mac_addresses.filter(mac_address=mac):
        iface = getattr(m, "assigned_object", None)
        dev_id = getattr(getattr(iface, "device", None), "id", None)
        if getattr(m, "assigned_object_type", None) == "dcim.interface" and iface and dev_id:
            out.append((dev_id, iface.id))
    return out


def _neighbor_iface_on_device(nb: "NetboxClient", device_id: int, remote_port: str) -> Optional[int]:
    """Interface id on device_id named remote_port, as reported or expanded ("Gi0" → "GigabitEthernet0")."""
    if not remote_port:
        return None
    for name in dict.fromkeys((remote_port, expand_iface_name(remote_port))):
        ifaces = list(nb.nb.dcim.interfaces.filter(device_id=device_id, name=name))
        if ifaces:
            return ifaces[0].id
    return None


def _resolve_neighbor_by_name(nb: "NetboxClient", remote_id: str, remote_port: str) -> tuple[Optional[int], Optional[int]]:
    """
    (device_id, interface_id) of the one Netbox device named remote_id: an exact
    (case-insensitive) name first, then the short name ("bern-w32" matches
    "bern-w32.example.com"). More than one candidate: ambiguous, (None, None).
    """
    name = (remote_id or "").strip()
    if not name:
        return None, None
    hits = list(nb.nb.dcim.devices.filter(name__ie=name))
    if not hits:
        short = name.split(".", 1)[0].lower()
        hits = [d for d in nb.nb.dcim.devices.filter(name__isw=short)
                if (d.name or "").split(".", 1)[0].lower() == short]
    if len(hits) != 1:
        logging.getLogger("discobox.sync").debug("  neighbor resolve  name=%s  device hits=%d", name, len(hits))
        return None, None
    return hits[0].id, _neighbor_iface_on_device(nb, hits[0].id, remote_port)


def _resolve_neighbor_by_ip(
    nb: "NetboxClient", remote_ip: str, remote_port: str
) -> tuple[Optional[int], Optional[int]]:
    """Passes 1 and 2 of _resolve_neighbor."""
    log = logging.getLogger("discobox.sync")
    if not remote_ip:
        return None, None

    def _iface_on_device(device_id: int) -> Optional[int]:
        return _neighbor_iface_on_device(nb, device_id, remote_port)

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
