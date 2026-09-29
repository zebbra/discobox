"""_resolve_ap_uplinks: AP uplink switch → Netbox device id, in bulk (IP, exact name, unique FQDN prefix)."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import _ap_note_block, _resolve_ap_uplinks  # noqa: E402

DEVICES = [
    NS(id=1, name="sw-a.example.net", primary_ip4=NS(address="192.0.2.1/24")),
    NS(id=2, name="SW-B", primary_ip4=None),
    NS(id=3, name="sw-c.example.net", primary_ip4=None),
    NS(id=4, name="sw-d.one.example", primary_ip4=None),
    NS(id=5, name="sw-d.two.example", primary_ip4=None),      # sw-d ambiguous
    NS(id=6, name="sw-e10", primary_ip4=None),                # not a prefix match for sw-e
]
IPS = [NS(id=101, address="192.0.2.1/24")]


class _Q:
    def __init__(self):
        self.calls: list = []

    def devices(self, **kw):
        self.calls.append(("devices", kw))
        if "primary_ip4_id" in kw:
            return [d for d in DEVICES if d.primary_ip4 and 101 in kw["primary_ip4_id"]]
        if "name__ie" in kw:
            return [d for d in DEVICES if d.name.lower() in kw["name__ie"]]
        if "name__isw" in kw:
            return [d for d in DEVICES if any(d.name.lower().startswith(p) for p in kw["name__isw"])]
        raise AssertionError(kw)

    def ips(self, **kw):
        self.calls.append(("ips", kw))
        return [a for a in IPS if a.address.split("/")[0] in kw["address"]]


def _nb(q):
    return NS(nb=NS(dcim=NS(devices=NS(filter=q.devices)), ipam=NS(ip_addresses=NS(filter=q.ips))))


def test_resolution_order_and_bulk() -> None:
    q = _Q()
    aps = [
        {"hostname": "AP1", "uplink_name": "wrong-name", "uplink_ip": "192.0.2.1"},   # IP wins
        {"hostname": "AP2", "uplink_name": "sw-b.example.net"},                        # short name exact
        {"hostname": "AP3", "uplink_name": "sw-c"},                                    # unique prefix
        {"hostname": "AP4", "uplink_name": "sw-c"},                                    # same uplink: once
        {"hostname": "AP5", "uplink_name": "sw-d"},                                    # ambiguous: none
        {"hostname": "AP6", "uplink_name": "sw-e"},                                    # sw-e10 isn't sw-e.
        {"hostname": "AP7"},                                                           # no uplink
    ]
    out = _resolve_ap_uplinks(_nb(q), aps)
    assert out == {("wrong-name", "192.0.2.1"): 1, ("sw-b.example.net", ""): 2, ("sw-c", ""): 3}
    assert len(q.calls) == 4            # ips, devices by ip, by name, by prefix: not per AP


def test_note_links_by_id_when_resolved() -> None:
    parsed = {"uplink_name": "sw-c.example.net", "uplink_ip": "192.0.2.9"}
    assert "[sw-c.example.net](/dcim/devices/3/) (192.0.2.9)" in _ap_note_block(parsed, "wlc", [], uplink_id=3)
    assert "[sw-c.example.net](/dcim/devices/?q=sw-c)" in _ap_note_block(parsed, "wlc", [])
