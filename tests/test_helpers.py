"""Unit tests for discobox helper functions.

Run with `pytest tests/` or directly:
    python tests/test_helpers.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import (
    NetboxClient,
    _fill_module_names,
    _ha_node_info,
    _slot_from_iface,
    expand_iface_name,
    map_iftype,
    parse_speed_kbps,
    parse_sw_model,
    parse_sw_ver,
    port_to_netbox,
    slugify,
    vendor_from_chassis,
)

SAMPLES = Path(__file__).resolve().parent / "samples"


# ── map_iftype ─────────────────────────────────────────────────────────────────

IFTYPE_CASES: list[tuple[str, str, str]] = [
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

    # Dot-notation subinterfaces — must be virtual regardless of the parent's
    # physical name prefix, or Netbox rejects the `parent` assignment.
    # Regression: "TwentyFiveGigE1/1/8.2802" was classified as 25gbase-x-sfp28.
    ("TwentyFiveGigE1/1/8.2802",     "ethernetCsmacd", "virtual"),
    ("GigabitEthernet0/0/1.1132",    "ethernetCsmacd", "virtual"),
    ("TenGigabitEthernet1/1.100",    "ethernetCsmacd", "virtual"),
    ("Po1.100",                      "propVirtual",    "virtual"),
    ("Port-channel1.200",            "ieee8023adLag",  "virtual"),
    ("eth0.100",                     "l2vlan",         "virtual"),
    # Dots not followed by trailing digits are NOT subinterfaces.
    ("ssl.root",                     "tunnel",         "virtual"),   # ifType, not the dot rule
]


def test_map_iftype() -> None:
    failures = []
    for name, nd_type, want in IFTYPE_CASES:
        got = map_iftype(nd_type, name)
        if got != want:
            failures.append(f"{name!r} ({nd_type}) -> {got!r}, want {want!r}")
    assert not failures, "map_iftype regressions:\n  " + "\n  ".join(failures)


# ── parse_sw_ver / parse_sw_model ──────────────────────────────────────────────

def test_parse_sw_ver() -> None:
    assert parse_sw_ver("FortiGate-600F v7.4.8,build2795,250523 (GA.M)") == "7.4.8"
    assert parse_sw_ver("FortiProxy-400G v7.6.6,build8279,260323 (GA)") == "7.6.6"
    assert parse_sw_ver("Version 17.3.4") == "17.3.4"
    assert parse_sw_ver("17.03.04") == "17.03.04"
    assert parse_sw_ver("") is None
    assert parse_sw_ver(None) is None
    assert parse_sw_ver("no version here") is None


def test_parse_sw_model() -> None:
    assert parse_sw_model("FortiGate-600F v7.4.8,build2795,250523 (GA.M)") == "FortiGate-600F"
    assert parse_sw_model("FortiProxy-400G v7.6.6,build8279,260323 (GA)") == "FortiProxy-400G"
    assert parse_sw_model("") is None
    assert parse_sw_model(None) is None
    # No "v<digit>" pattern → no model
    assert parse_sw_model("Cisco IOS Software, Version 17.3.4") is None


# ── slugify ────────────────────────────────────────────────────────────────────

def test_slugify() -> None:
    assert slugify("FortiProxy-400G") == "fortiproxy-400g"
    assert slugify("FortiGate-600F") == "fortigate-600f"
    assert slugify("FPX_400G") == "fpx_400g"          # underscore preserved
    assert slugify("C9300-48P") == "c9300-48p"
    assert slugify("N9K-C93180YC-EX") == "n9k-c93180yc-ex"
    assert slugify("  Some Model  ") == "some-model"
    assert slugify("Model.10408") == "model-10408"
    assert slugify("multiple---dashes") == "multiple-dashes"


# ── vendor_from_chassis ────────────────────────────────────────────────────────

def test_vendor_from_chassis() -> None:
    assert vendor_from_chassis({"type": "fortinet.10408.10408.0"}) == "Fortinet"
    assert vendor_from_chassis({"type": "fortinet.6007.6007.0"}) == "Fortinet"
    # Cisco ENTITY-MIB OID names — defer to caller's existing manufacturer
    assert vendor_from_chassis({"type": "cevChassisN9KC93600CDGX"}) is None
    assert vendor_from_chassis({"type": "cevChassisCAT9300"}) is None
    # No type / unparseable
    assert vendor_from_chassis({"type": ""}) is None
    assert vendor_from_chassis({}) is None


# ── parse_speed_kbps ───────────────────────────────────────────────────────────

def test_parse_speed_kbps() -> None:
    assert parse_speed_kbps("10 Mbps") == 10_000
    assert parse_speed_kbps("100 Mbps") == 100_000
    assert parse_speed_kbps("1 Gbps") == 1_000_000
    assert parse_speed_kbps("1.0 Gbps") == 1_000_000      # fortiproxy uses decimal form
    assert parse_speed_kbps("10 Gbps") == 10_000_000
    assert parse_speed_kbps("100 kbps") == 100
    assert parse_speed_kbps("0") is None
    assert parse_speed_kbps("auto") is None
    assert parse_speed_kbps("") is None
    assert parse_speed_kbps(None) is None


# ── _slot_from_iface ───────────────────────────────────────────────────────────

def test_slot_from_iface() -> None:
    # Stack: first number is the stack member
    assert _slot_from_iface("stack", "GigabitEthernet2/0/1") == 2
    assert _slot_from_iface("stack", "Te1/1/1") == 1
    # FEX: only numbers >= 100 count
    assert _slot_from_iface("fex", "Ethernet101/1/1") == 101
    assert _slot_from_iface("fex", "Ethernet1/1/1") is None
    # VSS: same as stack
    assert _slot_from_iface("vss", "TenGigabitEthernet5/0/1") == 5
    # Standalone topology: never returns a slot
    assert _slot_from_iface("standalone", "Gi1/0/1") is None
    # Names without N/N/N pattern
    assert _slot_from_iface("stack", "Po1") is None
    assert _slot_from_iface("stack", "port1") is None
    assert _slot_from_iface("stack", "LAG-ecn") is None


# ── _ha_node_info ──────────────────────────────────────────────────────────────

def test_ha_node_info() -> None:
    # Fortinet pNh convention
    assert _ha_node_info("zcgate0005p1h") == (1, "zcgate0005", "zcgate0005p0h")
    assert _ha_node_info("zcgate0005p2h") == (2, "zcgate0005", "zcgate0005p0h")
    # nodeN and -N conventions
    assert _ha_node_info("fw-node2") == (2, "fw", "fw-node0")
    assert _ha_node_info("gw-1") == (1, "gw", "gw-0")
    # No indicator
    assert _ha_node_info("switch01") is None


# ── upsert_virtual_chassis: rejected member update must not poison the record ──

class _FakeVC:
    def __init__(self, id: int):
        self.id = id


class _FakeVCEndpoint:
    def __init__(self, existing: list):
        self.existing = existing

    def filter(self, name: str):
        return list(self.existing)

    def create(self, **kwargs):
        raise AssertionError("create should not be called when VC exists")


class _FakeDevice:
    """Mimics pynetbox Record.update(): assigns attributes, then saves."""

    def __init__(self, name: str, virtual_chassis, vc_position, fail_save: bool = False):
        self.name = name
        self.virtual_chassis = virtual_chassis
        self.vc_position = vc_position
        self._fail_save = fail_save

    def update(self, patch: dict):
        for k, v in patch.items():
            setattr(self, k, v)     # pynetbox assigns before saving
        if self._fail_save:
            raise RuntimeError("400 Bad Request: cannot be removed from virtual chassis")


def _make_client(vcs: list) -> NetboxClient:
    client = NetboxClient.__new__(NetboxClient)

    class _NB:
        class dcim:
            virtual_chassis = _FakeVCEndpoint(vcs)

    client.nb = _NB()
    return client


def test_upsert_virtual_chassis_restores_record_on_rejected_update() -> None:
    old_vc = _FakeVC(id=3)      # device currently master of another VC
    target_vc = _FakeVC(id=5)
    dev = _FakeDevice("fw-p1h", virtual_chassis=old_vc, vc_position=1, fail_save=True)
    client = _make_client([target_vc])
    try:
        client.upsert_virtual_chassis("fw-p0h", [(dev, 1)])
        raise AssertionError("expected the rejected update to raise")
    except RuntimeError:
        pass
    # Local record must be back on the original VC, or the caller's next
    # save() on this device re-sends the rejected change and fails the sync.
    assert dev.virtual_chassis is old_vc
    assert dev.vc_position == 1


def test_upsert_virtual_chassis_updates_member() -> None:
    target_vc = _FakeVC(id=5)
    dev = _FakeDevice("fw-p1h", virtual_chassis=None, vc_position=None)
    client = _make_client([target_vc])
    action, vc = client.upsert_virtual_chassis("fw-p0h", [(dev, 1)])
    assert action == "updated"
    assert vc is target_vc
    assert dev.virtual_chassis == 5
    assert dev.vc_position == 1


# ── _fill_module_names ─────────────────────────────────────────────────────────

def test_fill_module_names_cat9300x() -> None:
    # IOS-XE 17.15 on C9300X reports entPhysicalName as null for EVERY entity;
    # names must be synthesized from description + parent chain.
    with open(SAMPLES / "cat9300x-modules.json") as f:
        mods = json.load(f)
    assert all(m.get("name") is None for m in mods), "fixture expected to have all-null names"

    _fill_module_names(mods)
    by_index = {m["index"]: m for m in mods}

    # Chassis member: pos 0 → "Switch 1" (matches pre-17.15 naming)
    assert by_index[1000]["name"] == "Switch 1"
    # Stack root: plain description, no Switch prefix (parent chain ends at 0)
    assert by_index[1]["name"] == "c93xx Stack"
    # Uplink module blade: description prefixed with owning switch so the
    # "Switch N" position/routing extraction keeps working
    assert by_index[1086]["name"] == "Switch 1 8x25G Uplink Module"
    # PSU / fan: description already carries "Switch 1" → no double prefix
    assert by_index[1015]["name"] == "Switch 1 - Power Supply A"
    assert by_index[1018]["name"] == "Switch 1 - C9300X-24HX - FAN 2"
    # SFPs (class=port): name = parent SFP container description minus " Container",
    # which is the abbreviated interface name the SFP sync maps to Netbox
    assert by_index[1104]["name"] == "Twe1/1/1"
    assert by_index[1110]["name"] == "Twe1/1/2"
    assert by_index[1116]["name"] == "Twe1/1/3"
    assert by_index[1122]["name"] == "Twe1/1/8"
    assert expand_iface_name(by_index[1110]["name"]) == "TwentyFiveGigE1/1/2"
    # Nothing is left nameless as None (empty string at worst)
    assert all(m["name"] is not None for m in mods)


def test_fill_module_names_keeps_existing() -> None:
    # Pre-17.15 dumps have real names — must pass through untouched.
    with open(SAMPLES / "cat9300-modules.json") as f:
        mods = json.load(f)
    before = {m["index"]: m.get("name") for m in mods if m.get("name")}
    assert before, "fixture expected to have named entries"
    _fill_module_names(mods)
    after = {m["index"]: m.get("name") for m in mods}
    for idx, name in before.items():
        assert after[idx] == name


# ── port_to_netbox (covers map_iftype + parse_speed_kbps + clean_mac) ──────────

def test_port_to_netbox_fortiproxy_physical() -> None:
    # port1 (OoB) — physical port with descriptive ifAlias.
    port = {
        "port": "port1", "name": "OoB", "descr": "",
        "type": "ethernetCsmacd", "up_admin": "up",
        "speed": "1.0 Gbps", "speed_admin": None,
        "duplex": None, "duplex_admin": None,
        "mac": "10:ff:e0:3d:63:c2", "mtu": 1500,
    }
    out = port_to_netbox(port)
    assert out["name"] == "port1"
    assert out["type"] == "1000base-t"   # NOT "lag" — regression guard
    assert out["enabled"] is True
    assert out["description"] == "OoB"   # name differs from port → used as description
    assert out["mac_address"] == "10:FF:E0:3D:63:C2"
    assert out["speed"] == 1_000_000
    assert out["mtu"] == 1500


def test_port_to_netbox_fortiproxy_lag() -> None:
    port = {
        "port": "LAG-ecn", "name": "LAG-ecn", "descr": "",
        "type": "ieee8023adLag", "up_admin": "up",
        "speed": "1.0 Gbps", "duplex": None, "duplex_admin": None,
        "mac": "10:ff:e0:3d:63:c4", "mtu": 1500,
    }
    out = port_to_netbox(port)
    assert out["name"] == "LAG-ecn"
    assert out["type"] == "lag"
    assert out["description"] == ""        # name == port → no description
    assert out["mac_address"] == "10:FF:E0:3D:63:C4"


def test_port_to_netbox_fortiproxy_l2vlan() -> None:
    # if-pro-ecn-1628 — VLAN sub-interface stacked on a LAG (slave_of is wired
    # up later by the LAG-bonding pass; type comes from l2vlan ifType).
    port = {
        "port": "if-pro-ecn-1628", "name": "External to ECN", "descr": "",
        "type": "l2vlan", "up_admin": "up",
        "speed": "1.0 Gbps", "duplex": None, "duplex_admin": None,
        "mac": "10:ff:e0:3d:63:c4", "mtu": 1500,
    }
    out = port_to_netbox(port)
    assert out["name"] == "if-pro-ecn-1628"
    assert out["type"] == "virtual"
    assert out["description"] == "External to ECN"


def test_port_to_netbox_disabled_and_zero_speed() -> None:
    # SSL VPN tunnel — admin-down, no MAC, speed "0".
    port = {
        "port": "ssl.root", "name": "SSL VPN interface", "descr": "",
        "type": "tunnel", "up_admin": "down",
        "speed": "0", "duplex": None, "duplex_admin": None,
        "mac": None, "mtu": 1500,
    }
    out = port_to_netbox(port)
    assert out["name"] == "ssl.root"
    assert out["type"] == "virtual"
    assert out["enabled"] is False
    assert out["mac_address"] is None
    assert out["speed"] is None             # "0" → None
    assert out["description"] == "SSL VPN interface"


def test_port_to_netbox_null_mac() -> None:
    # All-zero MAC must be cleaned to None.
    port = {
        "port": "Gi1/0/1", "name": "Gi1/0/1", "descr": "",
        "type": "ethernetCsmacd", "up_admin": "up",
        "speed": "1 Gbps", "duplex": "full", "duplex_admin": None,
        "mac": "00:00:00:00:00:00", "mtu": 1500,
    }
    out = port_to_netbox(port)
    assert out["mac_address"] is None
    assert out["duplex"] == "full"


# ── Sample-driven smoke tests ──────────────────────────────────────────────────

# Valid Netbox interface type slugs the mapper may produce. Update when
# adding new branches to map_iftype.
VALID_IFTYPES: set[str] = {
    "lag", "virtual", "bridge", "other",
    "100base-tx", "1000base-t", "2.5gbase-t", "5gbase-t",
    "10gbase-x-sfpp", "25gbase-x-sfp28", "40gbase-x-qsfpp",
    "50gbase-x-sfp28", "100gbase-x-qsfp28",
}


def test_port_to_netbox_over_fortiproxy_sample() -> None:
    """Run port_to_netbox over every entry of the anonymized fortiproxy ports
    fixture and assert basic invariants (no crash, well-formed output, type slug
    in the known set, no fortiproxy port resolved as 'lag' — regression guard
    for the old 'po' prefix bug)."""
    path = SAMPLES / "fortiproxy-ports.json"
    if not path.exists():
        # Fixture not generated yet — skip rather than fail.
        return
    ports = json.loads(path.read_text())
    assert ports, "fortiproxy-ports.json is empty"
    for p in ports:
        out = port_to_netbox(p)
        assert out["name"], f"empty name for port {p!r}"
        assert out["type"] in VALID_IFTYPES, f"unknown type {out['type']!r} for {out['name']!r}"
        # Every fortiproxy `port[1-9]` entry must be physical, not lag.
        if out["name"].startswith("port") and out["name"][4:].isdigit():
            assert out["type"] != "lag", f"{out['name']} regressed to lag"


def test_vendor_from_chassis_over_fortinet_samples() -> None:
    """vendor_from_chassis must yield 'Fortinet' for every chassis entry in the
    fortinet/fortiproxy modules fixtures."""
    for fname in ("fortigate-modules.json", "fortiproxy-modules.json"):
        path = SAMPLES / fname
        if not path.exists():
            continue
        mods = json.loads(path.read_text())
        chassis = [m for m in mods if m.get("class") == "chassis"]
        assert chassis, f"no chassis entries in {fname}"
        for ch in chassis:
            assert vendor_from_chassis(ch) == "Fortinet", \
                f"{fname}: chassis {ch.get('name')!r} → {vendor_from_chassis(ch)!r}"


# ── Driver ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_map_iftype()
    test_parse_sw_ver()
    test_parse_sw_model()
    test_slugify()
    test_vendor_from_chassis()
    test_parse_speed_kbps()
    test_slot_from_iface()
    test_port_to_netbox_fortiproxy_physical()
    test_port_to_netbox_fortiproxy_lag()
    test_port_to_netbox_fortiproxy_l2vlan()
    test_port_to_netbox_disabled_and_zero_speed()
    test_port_to_netbox_null_mac()
    test_port_to_netbox_over_fortiproxy_sample()
    test_vendor_from_chassis_over_fortinet_samples()
    print("OK — all tests passed")
