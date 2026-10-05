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


def test_remove_wlc_radio_ports_only_unprotected_pseudo_ports() -> None:
    from discobox import _remove_wlc_radio_ports
    ifaces = [
        _If(1, "02:00:00:aa:bb:cc.0"), _If(2, "02:00:00:aa:bb:cc.1"),               # pseudo-ports: removed
        _If(3, "02:00:00:aa:bb:dd.0", cable=SimpleNamespace(id=1)),                  # cabled: kept
        _If(4, "02:00:00:aa:bb:ee.0", source="bossy"),                               # foreign: kept
        _If(5, "02:00:00:aa:bb:ff.0"),                                               # has IP: kept
        _If(6, "TenGigabitEthernet0/0/0"), _If(7, "02:00:00:aa:bb:cc"),              # real / no slot: kept
    ]
    nb = _nb(ips_on={5})
    nb.nb.dcim = SimpleNamespace(interfaces=SimpleNamespace(filter=lambda device_id: ifaces))
    assert _remove_wlc_radio_ports(nb, SimpleNamespace(id=9), "source", "netdisco", logging.getLogger()) == 2
    assert [i.name for i in ifaces if i.deleted] == ["02:00:00:aa:bb:cc.0", "02:00:00:aa:bb:cc.1"]


def test_ap_iface_fixups_clear_rf_role_on_non_wireless() -> None:
    from discobox import _ap_iface_fixups
    ch = lambda v: SimpleNamespace(value=v)  # noqa: E731  (pynetbox ChoiceItem)
    assert _ap_iface_fixups(SimpleNamespace(rf_role=ch("ap")), "2.5gbase-t") == {"rf_role": ""}
    assert _ap_iface_fixups(SimpleNamespace(rf_role=ch("ap")), "ieee802.11ax") == {}
    assert _ap_iface_fixups(SimpleNamespace(rf_role=None), "2.5gbase-t") == {}
    assert _ap_iface_fixups(None, "2.5gbase-t") == {}


# ── orphaned interfaces (switch side) ──

def _orphan_setup():
    existing = {i.name: i for i in (
        _If(1, "Gi1/0/1", source="netdisco"),                      # own orphan: removed
        _If(2, "Gi1/0/2"),                                         # source-less: only with the flag
        _If(3, "Gi1/0/3", source="bossy"),                         # foreign: never
        _If(4, "Gi1/0/4", source="netdisco", cable=SimpleNamespace(id=1)),   # cabled: never
        _If(5, "Gi1/0/5", source="netdisco"),                      # has IP: never
    )}
    existing.update({f"Te1/1/{n}": _If(10 + n, f"Te1/1/{n}", source="netdisco") for n in range(1, 16)})
    nd_names = {f"Te1/1/{n}" for n in range(1, 16)}                # still reported by Netdisco
    return existing, nd_names


def test_orphans_own_always_unowned_only_with_flag_foreign_never() -> None:
    from discobox import _remove_orphaned_interfaces
    existing, nd = _orphan_setup()
    assert _remove_orphaned_interfaces(_nb(ips_on={5}), existing, nd, 15, "source", "netdisco", 25,
                                       logging.getLogger()) == ["Gi1/0/1"]
    existing, nd = _orphan_setup()
    assert sorted(_remove_orphaned_interfaces(_nb(ips_on={5}), existing, nd, 15, "source", "netdisco", 25,
                                              logging.getLogger(), include_unowned=True)) == ["Gi1/0/1", "Gi1/0/2"]


def test_orphan_brake_on_empty_or_mass_deletion() -> None:
    from discobox import _remove_orphaned_interfaces
    existing, _ = _orphan_setup()
    # Netdisco reports no ports at all: nothing goes, whatever the ownership
    assert _remove_orphaned_interfaces(_nb(), existing, set(), 0, "source", "netdisco", 25, logging.getLogger()) == []
    assert not any(i.deleted for i in existing.values())
    # 16 own orphans of 20 interfaces (80% > 25%, > the 5 always allowed): brake
    stats: dict = {}
    assert _remove_orphaned_interfaces(_nb(), existing, {"Gi1/0/2"}, 1, "source", "netdisco", 25,
                                       logging.getLogger(), stats=stats) == []
    assert stats == {"braked": 17}   # own uncabled orphans: Gi1/0/1, Gi1/0/5, 15 Te


def test_orphan_digit_names_bypass_brake_unless_netdisco_has_digit_ports() -> None:
    from discobox import _remove_orphaned_interfaces
    def setup():
        existing = {f"Gi1/0/{n}": _If(n, f"Gi1/0/{n}", source="netdisco") for n in range(1, 11)}
        existing.update({str(n): _If(100 + n, str(n), source="netdisco") for n in range(1, 21)})   # ifIndex junk
        existing.update({f"Te1/1/{n}": _If(200 + n, f"Te1/1/{n}", source="netdisco") for n in range(1, 5)})
        existing["21"] = _If(121, "21")                                       # source-less: kept
        existing["22"] = _If(122, "22", source="netdisco", cable=SimpleNamespace(id=1))   # cabled: kept
        existing["23"] = _If(123, "23", source="netdisco")                    # has IP: kept
        return existing
    nd = {f"Gi1/0/{n}" for n in range(1, 11)}
    # 20 digit orphans (not braked) + 4 Te orphans (<= the 5 always allowed): all go
    existing = setup()
    deleted = _remove_orphaned_interfaces(_nb(ips_on={123}), existing, nd, 10, "source", "netdisco", 25,
                                          logging.getLogger())
    assert sorted(deleted) == sorted([str(n) for n in range(1, 21)] + [f"Te1/1/{n}" for n in range(1, 5)])
    # the brake on the rest still holds: 13 non-digit orphans > limit 9, only the digit ones go
    existing = setup()
    deleted = _remove_orphaned_interfaces(_nb(ips_on={123}), existing, {"Gi1/0/1"}, 1, "source", "netdisco", 25,
                                          logging.getLogger())
    assert sorted(deleted) == sorted(str(n) for n in range(1, 21))
    # Netdisco itself reports digit-only ports (HP/Aruba): no exemption, brake applies to all
    existing = setup()
    assert _remove_orphaned_interfaces(_nb(), existing, {"1"}, 1, "source", "netdisco", 25,
                                       logging.getLogger()) == []
    # no Netdisco ports at all: nothing
    existing = setup()
    assert _remove_orphaned_interfaces(_nb(), existing, set(), 0, "source", "netdisco", 25,
                                       logging.getLogger()) == []
