"""Unit tests for discobox.map_iftype.

Run with `pytest tests/` or directly: `python tests/test_iftype.py`.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import map_iftype


# (interface_name, netdisco_iftype, expected Netbox type slug)
CASES: list[tuple[str, str, str]] = [
    # FortiProxy/FortiGate physical ports — must NOT be classified as lag.
    # Regression: "po" prefix used to false-match "port1".
    ("port1",            "ethernetCsmacd",  "1000base-t"),
    ("port2",            "ethernetCsmacd",  "1000base-t"),
    ("port4",            "ethernetCsmacd",  "1000base-t"),

    # FortiProxy LAGs — caught via SNMP ifType.
    ("LAG-ecn",          "ieee8023adLag",   "lag"),
    ("LAG-dcn",          "ieee8023adLag",   "lag"),

    # FortiProxy l2vlan sub-interfaces and SSL VPN tunnels.
    ("if-pro-ecn-1628",  "l2vlan",          "virtual"),
    ("ssl.root",         "tunnel",          "virtual"),

    # Cisco port-channel shorthand and full form — still classify as lag.
    ("Po1",              "ieee8023adLag",   "lag"),
    ("Po10",             "ieee8023adLag",   "lag"),
    ("Po123",            "ieee8023adLag",   "lag"),
    ("Port-channel1",    "ieee8023adLag",   "lag"),

    # Cisco physical name-prefix matches.
    ("GigabitEthernet0/1",      "ethernetCsmacd", "1000base-t"),
    ("Gi1/0/1",                 "ethernetCsmacd", "1000base-t"),
    ("TenGigabitEthernet1/1",   "ethernetCsmacd", "10gbase-x-sfpp"),
    ("Te1/1",                   "ethernetCsmacd", "10gbase-x-sfpp"),
    ("HundredGigE1/0/1",        "ethernetCsmacd", "100gbase-x-qsfp28"),
    ("FortyGigabitEthernet1/1", "ethernetCsmacd", "40gbase-x-qsfpp"),

    # Virtuals.
    ("Loopback0",        "softwareLoopback", "virtual"),
    ("Vlan100",          "l3ipvlan",         "virtual"),
    ("Tunnel0",          "tunnel",           "virtual"),
]


def test_map_iftype() -> None:
    failures = []
    for name, nd_type, want in CASES:
        got = map_iftype(nd_type, name)
        if got != want:
            failures.append(f"{name!r} ({nd_type}) -> {got!r}, want {want!r}")
    assert not failures, "map_iftype regressions:\n  " + "\n  ".join(failures)


if __name__ == "__main__":
    test_map_iftype()
    print(f"OK — {len(CASES)} cases passed")
