"""_prune_ap_interfaces: APs keep only GigabitEthernet0 + Dot11Radio<n>, never anything protected."""
from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import _is_ap_managed_iface, _prune_ap_interfaces  # noqa: E402


class _If:
    def __init__(self, id_, name, cable=None, source=None):
        self.id, self.name, self.cable = id_, name, cable
        self.custom_fields = {"source": source}
        self.deleted = False

    def delete(self):
        self.deleted = True


def _nb(ips_on=()):
    return SimpleNamespace(nb=SimpleNamespace(ipam=SimpleNamespace(ip_addresses=SimpleNamespace(
        filter=lambda assigned_object_type, assigned_object_id: ["ip"] if assigned_object_id in ips_on else []
    ))))


def test_managed_names() -> None:
    for n in ("GigabitEthernet0", "gigabitethernet0", "Dot11Radio0", "Dot11Radio2"):
        assert _is_ap_managed_iface(n)
    for n in ("main", "Vlan2", "GigabitEthernet1/0/1", "GigabitEthernet0/1", "WAN", "br0"):
        assert not _is_ap_managed_iface(n)


def test_prune_removes_placeholders_and_wrong_template_ports_only() -> None:
    ifaces = {i.name: i for i in (
        _If(1, "GigabitEthernet0"), _If(2, "Dot11Radio0"), _If(3, "Dot11Radio5"),     # managed: kept
        _If(4, "main"), _If(5, "Vlan2"), _If(6, "GigabitEthernet1/0/1"),              # stale: removed
        _If(7, "GigabitEthernet1/0/2", cable=SimpleNamespace(id=9)),                   # cabled: kept
        _If(8, "GigabitEthernet1/0/3"),                                                # has IP: kept
        _If(9, "mgmt0", source="bossy"),                                               # foreign: kept
        _If(10, "GigabitEthernet1/0/4", source="netdisco"),                            # ours: removed
    )}
    ap = SimpleNamespace(name="ap-1")
    n = _prune_ap_interfaces(_nb(ips_on={8}), ap, ifaces, "source", "netdisco", logging.getLogger())
    assert n == 4
    assert sorted(i.name for i in ifaces.values() if i.deleted) == [
        "GigabitEthernet1/0/1", "GigabitEthernet1/0/4", "Vlan2", "main",
    ]


def test_prune_nothing_to_do() -> None:
    ifaces = {"GigabitEthernet0": _If(1, "GigabitEthernet0")}
    assert _prune_ap_interfaces(_nb(), SimpleNamespace(name="ap"), ifaces, "source", "netdisco", logging.getLogger()) == 0
