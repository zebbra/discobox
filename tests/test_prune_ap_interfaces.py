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


def test_prune_ap_bays_only_empty_and_only_when_counted() -> None:
    from discobox import _prune_ap_bays

    class _Bay(_If):
        def __init__(self, id_, name, installed=None, attr="installed_device"):
            super().__init__(id_, name)
            setattr(self, attr, installed)

    dbays = [_Bay(1, "Slot 1"), _Bay(2, "Slot 2", installed=SimpleNamespace(id=5))]
    mbays = [_Bay(3, "Network Module", attr="installed_module")]
    queried: list = []

    def _ep(items, name):
        return SimpleNamespace(filter=lambda device_id: queried.append(name) or items)

    nb = SimpleNamespace(nb=SimpleNamespace(dcim=SimpleNamespace(
        device_bays=_ep(dbays, "device_bays"), module_bays=_ep(mbays, "module_bays"))))
    ap = SimpleNamespace(name="ap", id=1, device_bay_count=2, module_bay_count=1)
    assert _prune_ap_bays(nb, ap, logging.getLogger()) == 2
    assert [b.name for b in dbays + mbays if b.deleted] == ["Slot 1", "Network Module"]   # occupied bay kept
    queried.clear()
    assert _prune_ap_bays(nb, SimpleNamespace(name="ap", id=1, device_bay_count=0, module_bay_count=0),
                          logging.getLogger()) == 0
    assert queried == []                                      # no bays counted → no queries


def test_tags_without() -> None:
    from discobox import _tags_without
    t = [SimpleNamespace(id=1, slug="fixme-model"), SimpleNamespace(id=2, slug="wifi")]
    assert _tags_without(t, {"fixme-model"}) == [2]
    assert _tags_without([SimpleNamespace(id=2, slug="wifi")], {"fixme-model"}) is None    # nothing to do
    assert _tags_without(None, {"fixme-model"}) is None
    assert _tags_without([SimpleNamespace(id=1, slug="FIXME-Model")], {"fixme-model"}) == []
