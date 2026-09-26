"""_resolve_neighbor: IP passes first, then the LLDP chassis ID as a MAC.

Shape from a real Cisco AP neighbor on an access switch (values fake):
remote_ip = the AP's DHCP address (not in Netbox), remote_id = the AP's
Ethernet MAC, remote_port = "Gi0" (abbreviated).
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import _resolve_neighbor  # noqa: E402

AP_MAC = "02:00:00:00:52:01"


def _dev_filter(devices, q=None, name__ie=None, name__isw=None):
    if q is not None:
        return [d for d in devices if q in str(d.primary_ip4 or "")]
    if name__ie is not None:
        return [d for d in devices if d.name.lower() == name__ie.lower()]
    return [d for d in devices if d.name.lower().startswith(name__isw.lower())]


def _nb(devices=(), ifaces=(), macs=()):
    def _mac_filter(mac_address):
        return [m for m in macs if m.mac_address.upper() == mac_address.upper()]

    dcim = SimpleNamespace(
        devices=SimpleNamespace(filter=lambda **kw: _dev_filter(devices, **kw)),
        interfaces=SimpleNamespace(filter=lambda device_id, name: [
            i for i in ifaces if i.device.id == device_id and i.name == name
        ]),
        mac_addresses=SimpleNamespace(filter=_mac_filter),
    )
    ipam = SimpleNamespace(ip_addresses=SimpleNamespace(filter=lambda q: []))
    return SimpleNamespace(nb=SimpleNamespace(dcim=dcim, ipam=ipam))


def _ap(dev_id=52, iface_id=520, mac=AP_MAC):
    dev = SimpleNamespace(id=dev_id, name="ap-w052", primary_ip4=None)
    iface = SimpleNamespace(id=iface_id, name="GigabitEthernet0", device=dev)
    return dev, iface, SimpleNamespace(mac_address=mac.upper(), assigned_object_type="dcim.interface", assigned_object=iface)


def test_ap_without_ip_resolves_by_mac() -> None:
    dev, iface, mac = _ap()
    nb = _nb(devices=[dev], ifaces=[iface], macs=[mac])
    assert _resolve_neighbor(nb, "192.0.2.39", "Gi0", AP_MAC) == (52, 520)
    # case / separator of the LLDP chassis ID don't matter
    assert _resolve_neighbor(nb, "192.0.2.39", "Gi0", AP_MAC.upper().replace(":", "-")) == (52, 520)


def test_mac_only_neighbor_without_ip() -> None:
    dev, iface, mac = _ap()
    assert _resolve_neighbor(_nb(macs=[mac]), "", "Gi0", AP_MAC) == (52, 520)


def test_name_chassis_id_resolves_by_short_name_and_abbreviated_port() -> None:
    # CDP/LLDP device ID is the AP's hostname (DHCP IP not in Netbox, no MAC match)
    dev, iface, mac = _ap()
    dev.name = "bern-w052.example.com"
    nb = _nb(devices=[dev], ifaces=[iface])
    assert _resolve_neighbor(nb, "192.0.2.39", "Gi0", "BERN-W052") == (52, 520)
    assert _resolve_neighbor(nb, "192.0.2.39", "GigabitEthernet0", "bern-w052.example.com") == (52, 520)
    # device found but port unknown → device only (no cable, neighbor_device still set)
    assert _resolve_neighbor(nb, "", "eth7", "BERN-W052") == (52, None)
    assert _resolve_neighbor(nb, "", "Gi0", "OTHER-W1") == (None, None)


def test_ambiguous_name_is_not_used() -> None:
    a = SimpleNamespace(id=1, name="bern-w052.example.com", primary_ip4=None)
    b = SimpleNamespace(id=2, name="bern-w052.other.example", primary_ip4=None)
    assert _resolve_neighbor(_nb(devices=[a, b]), "", "Gi0", "BERN-W052") == (None, None)


def test_ambiguous_mac_is_not_used() -> None:
    _, _, mac1 = _ap(52, 520)
    _, _, mac2 = _ap(53, 530)
    assert _resolve_neighbor(_nb(macs=[mac1, mac2]), "192.0.2.39", "Gi0", AP_MAC) == (None, None)


def test_ip_hit_with_interface_wins_over_mac() -> None:
    sw = SimpleNamespace(id=9, name="sw", primary_ip4="192.0.2.9/24")
    sw_if = SimpleNamespace(id=90, name="Twe1/0/1", device=sw)
    _, _, mac = _ap()          # MAC points elsewhere: must not override a full IP hit
    nb = _nb(devices=[sw], ifaces=[sw_if], macs=[mac])
    assert _resolve_neighbor(nb, "192.0.2.9", "Twe1/0/1", AP_MAC) == (9, 90)


def test_mac_on_another_device_does_not_replace_ip_device() -> None:
    sw = SimpleNamespace(id=9, name="sw", primary_ip4="192.0.2.9/24")
    _, _, mac = _ap()          # IP found device 9 but not the port; MAC says device 52
    nb = _nb(devices=[sw], macs=[mac])
    assert _resolve_neighbor(nb, "192.0.2.9", "Gi0", AP_MAC) == (9, None)


def test_fdb_resolves_ap_by_learned_mac() -> None:
    from discobox import _resolve_neighbor_by_fdb
    dev, iface, mac = _ap()
    nb = _nb(devices=[dev], ifaces=[iface], macs=[mac])
    # the AP's Ethernet MAC learned on the port, plus a client MAC unknown to Netbox
    assert _resolve_neighbor_by_fdb(nb, [AP_MAC.lower(), "02:00:00:aa:aa:aa"]) == (52, 520)
    assert _resolve_neighbor_by_fdb(nb, []) == (None, None)
    assert _resolve_neighbor_by_fdb(nb, ["02:00:00:aa:aa:aa"]) == (None, None)


def test_fdb_ignores_busy_ports_and_ambiguity() -> None:
    from discobox import FDB_MAX_MACS_PER_PORT, _resolve_neighbor_by_fdb
    dev, iface, mac = _ap()
    dev2, iface2, mac2 = _ap(53, 530, mac="02:00:00:00:53:01")
    nb = _nb(devices=[dev, dev2], ifaces=[iface, iface2], macs=[mac, mac2])
    many = [AP_MAC] + [f"02:00:00:bb:bb:{i:02x}" for i in range(FDB_MAX_MACS_PER_PORT)]
    assert _resolve_neighbor_by_fdb(nb, many) == (None, None)                          # uplink-ish port
    assert _resolve_neighbor_by_fdb(nb, [AP_MAC, "02:00:00:00:53:01"]) == (None, None)   # two known devices
