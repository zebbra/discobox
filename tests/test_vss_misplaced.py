"""_remove_misplaced_vss_interfaces: interfaces on the wrong VSS member go, unless owned elsewhere or holding a cable/IP."""
from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import _remove_misplaced_vss_interfaces  # noqa: E402


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


def test_misplaced_policy() -> None:
    sw1 = {i.name: i for i in (
        _If(1, "TenGigabitEthernet1/1/1"),                          # right member: kept
        _If(2, "TenGigabitEthernet2/1/1", source="netdisco"),       # own, misplaced: removed
        _If(3, "TenGigabitEthernet2/1/2"),                          # source-less, misplaced: removed
        _If(4, "TenGigabitEthernet2/1/3", source="manual"),         # foreign: kept
        _If(5, "TenGigabitEthernet2/1/4", cable=7),                 # cabled: kept
        _If(6, "TenGigabitEthernet2/1/5"),                          # has an IP: kept
        _If(7, "TenGigabitEthernet3/1/1"),                          # no member 3: kept
        _If(8, "TenGigabitEthernet2/1/1.100"),                      # subinterface: removed (first)
    )}
    vss = {1: sw1, 2: {}}
    gone = dict(sw1)
    n = _remove_misplaced_vss_interfaces(_nb(ips_on={6}), vss, {1, 2}, "source", "netdisco",
                                         logging.getLogger("t"))
    assert n == 3
    assert sorted(i.name for i in gone.values() if i.deleted) == [
        "TenGigabitEthernet2/1/1", "TenGigabitEthernet2/1/1.100", "TenGigabitEthernet2/1/2"]
    assert set(sw1) == {"TenGigabitEthernet1/1/1", "TenGigabitEthernet2/1/3", "TenGigabitEthernet2/1/4",
                        "TenGigabitEthernet2/1/5", "TenGigabitEthernet3/1/1"}


def test_without_source_cf_still_guards_cable_and_ip() -> None:
    sw2 = {i.name: i for i in (_If(1, "Gi1/0/1", cable=3), _If(2, "Gi1/0/2"), _If(3, "Gi1/0/3"))}
    n = _remove_misplaced_vss_interfaces(_nb(ips_on={3}), {1: {}, 2: sw2}, {1, 2}, None, "netdisco",
                                         logging.getLogger("t"))
    assert n == 1 and set(sw2) == {"Gi1/0/1", "Gi1/0/3"}
