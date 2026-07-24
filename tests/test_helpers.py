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
    _discovery_incomplete,
    _fill_module_names,
    _ha_node_info,
    _should_update_stack_members,
    _slave_link_field,
    _slot_from_iface,
    _stack_member_count,
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


def test_map_iftype_null_iftype_has_no_opinion() -> None:
    # FortiGate devices can report ifType=null on every port. With default=None
    # the mapping must express "no opinion" so the existing Netbox type is kept.
    assert map_iftype(None, "if-roo-sbc-164", default=None) is None
    assert map_iftype(None, "port1", default=None) is None
    assert map_iftype(None, "LAG-internal", default=None) is None
    # Name rules still win over a missing ifType
    assert map_iftype(None, "mgmt", default=None) == "1000base-t"
    assert map_iftype(None, "Loopback0", default=None) == "virtual"
    assert map_iftype(None, "GigabitEthernet0/0/1.100", default=None) == "virtual"
    # A real-but-unknown ifType still maps to "other"
    assert map_iftype("ppp", "weird0", default=None) == "other"
    # 2-arg call keeps the historical fallback
    assert map_iftype(None, "port1") == "other"


def test_slave_link_field() -> None:
    # Dot-notation subinterface → parent, regardless of types
    assert _slave_link_field("Gi0/0/1.100", "ethernetCsmacd", None) == "parent"
    # Netdisco says virtual (FortiProxy l2vlan) → parent
    assert _slave_link_field("if-pro-ecn-1628", "l2vlan", None) == "parent"
    # Netdisco has no ifType but Netbox already knows it's virtual → parent
    # (regression: was mis-wired as `lag` and rejected with 400)
    assert _slave_link_field("if-roo-sbc-164", None, "virtual") == "parent"
    # Physical LAG members stay lag, with or without ifType
    assert _slave_link_field("port3", "ethernetCsmacd", "1000base-t") == "lag"
    assert _slave_link_field("port11", None, "1000base-t") == "lag"
    assert _slave_link_field("port11", None, None) == "lag"


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
    def __init__(self, id: int, name: str = "vc", master=None):
        self.id = id
        self.name = name
        self.master = master
        self.deleted = False
        self.updates: list = []

    def update(self, patch: dict):
        self.updates.append(patch)
        for k, v in patch.items():
            setattr(self, k, v)

    def delete(self):
        self.deleted = True


class _FakeVCEndpoint:
    def __init__(self, by_name: dict):
        self.by_name = by_name
        self.created: list = []

    def filter(self, name: str = None, master_id: int = None):
        vcs = self.by_name.values()
        if name is not None:
            return [vc for vc in vcs if vc.name == name]
        if master_id is not None:
            return [vc for vc in vcs if vc.master is not None and vc.master.id == master_id]
        return list(vcs)

    def get(self, id: int):
        return next((vc for vc in self.by_name.values() if vc.id == id), None)

    def create(self, **kwargs):
        vc = _FakeVC(id=1000 + len(self.created), name=kwargs["name"], master=kwargs.get("master"))
        self.created.append(vc)
        self.by_name[vc.id] = vc
        return vc


class _FakeDeviceEndpoint:
    def __init__(self, devices: list):
        self.devices = devices

    def filter(self, virtual_chassis_id: int):
        return [
            d for d in self.devices
            if d.virtual_chassis is not None and d.virtual_chassis.id == virtual_chassis_id
        ]


class _FakeDevice:
    """Mimics pynetbox Record.update(): assigns attributes, then saves."""

    _next_id = 100

    def __init__(self, name: str, virtual_chassis, vc_position, fail_save: bool = False):
        _FakeDevice._next_id += 1
        self.id = _FakeDevice._next_id
        self.name = name
        self.virtual_chassis = virtual_chassis
        self.vc_position = vc_position
        self._fail_save = fail_save

    def update(self, patch: dict):
        for k, v in patch.items():
            setattr(self, k, v)     # pynetbox assigns before saving
        if self._fail_save:
            raise RuntimeError("400 Bad Request: cannot be removed from virtual chassis")


def _make_client(vcs: list, devices: list = ()) -> NetboxClient:
    from types import SimpleNamespace

    client = NetboxClient.__new__(NetboxClient)
    client.nb = SimpleNamespace(dcim=SimpleNamespace(
        virtual_chassis=_FakeVCEndpoint({vc.id: vc for vc in vcs}),
        devices=_FakeDeviceEndpoint(list(devices)),
    ))
    return client


def test_upsert_virtual_chassis_restores_record_on_rejected_update() -> None:
    target_vc = _FakeVC(id=5, name="fw-p0h")
    old_vc = _FakeVC(id=3, name="fw-legit")
    dev = _FakeDevice("fw-p1h", virtual_chassis=old_vc, vc_position=1, fail_save=True)
    partner = _FakeDevice("fw-p2h", virtual_chassis=old_vc, vc_position=2)
    old_vc.master = dev
    client = _make_client([target_vc, old_vc], [dev, partner])
    try:
        client.upsert_virtual_chassis("fw-p0h", [(dev, 1)])
        raise AssertionError("expected the rejected update to raise")
    except RuntimeError:
        pass
    # The old VC has another member, so it is NOT stale: master untouched.
    assert old_vc.master is dev
    assert not old_vc.deleted
    # Local record must be back on the original VC, or the caller's next
    # save() on this device re-sends the rejected change and fails the sync.
    assert dev.virtual_chassis is old_vc
    assert dev.vc_position == 1


def test_upsert_virtual_chassis_updates_member() -> None:
    target_vc = _FakeVC(id=5, name="fw-p0h")
    dev = _FakeDevice("fw-p1h", virtual_chassis=None, vc_position=None)
    client = _make_client([target_vc], [dev])
    action, vc = client.upsert_virtual_chassis("fw-p0h", [(dev, 1)])
    assert action == "updated"
    assert vc is target_vc
    assert dev.virtual_chassis == 5
    assert dev.vc_position == 1


def test_upsert_virtual_chassis_adopts_legacy_named_vc() -> None:
    # Pair lives in a base-named VC ("zwgate0089") from the old naming scheme,
    # partner is its master; target name doesn't exist. Creating a second VC
    # with the same master 400s in Netbox — the legacy VC must be adopted and
    # renamed in place, with no member moves.
    legacy_vc = _FakeVC(id=7, name="zwgate0089")
    dev = _FakeDevice("zwgate0089p2h", virtual_chassis=legacy_vc, vc_position=2)
    partner = _FakeDevice("zwgate0089p1h", virtual_chassis=legacy_vc, vc_position=1)
    legacy_vc.master = dev
    client = _make_client([legacy_vc], [dev, partner])
    action, vc = client.upsert_virtual_chassis(
        "zwgate0089p0h", [(dev, 2), (partner, 1)],
    )
    assert vc is legacy_vc
    assert legacy_vc.name == "zwgate0089p0h"
    assert action == "renamed"
    assert not legacy_vc.deleted
    # No member churn
    assert dev.virtual_chassis is legacy_vc
    assert partner.virtual_chassis is legacy_vc
    assert client.nb.dcim.virtual_chassis.created == []


def test_upsert_virtual_chassis_creates_when_no_adoptable_vc() -> None:
    dev = _FakeDevice("fw-p1h", virtual_chassis=None, vc_position=None)
    client = _make_client([], [dev])
    action, vc = client.upsert_virtual_chassis("fw-p0h", [(dev, 1)])
    assert action in ("created", "updated")
    assert vc.name == "fw-p0h"
    assert client.nb.dcim.virtual_chassis.created == [vc]
    assert dev.virtual_chassis == vc.id


def test_upsert_virtual_chassis_absorbs_stale_single_member_vc() -> None:
    # Residue from the earlier VC-name mismatch: the partner sits alone in a
    # junk VC ("zcgate0005") as its master. Moving it must release the master,
    # move the device, and delete the empty shell.
    target_vc = _FakeVC(id=5, name="zcgate0005p0h")
    stale_vc = _FakeVC(id=9, name="zcgate0005")
    dev = _FakeDevice("zcgate0005p2h", virtual_chassis=stale_vc, vc_position=2)
    stale_vc.master = dev
    client = _make_client([target_vc, stale_vc], [dev])
    action, _ = client.upsert_virtual_chassis("zcgate0005p0h", [(dev, 2)])
    assert action == "updated"
    assert dev.virtual_chassis == 5
    assert stale_vc.updates == [{"master": None}]
    assert stale_vc.deleted


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


# ── port_to_netbox: FortiGate with null ifType on every port ───────────────────

def test_port_to_netbox_fortigate_null_types() -> None:
    with open(SAMPLES / "fortigate-ha-ports.json") as f:
        ports = json.load(f)
    assert all(p.get("type") is None for p in ports), "fixture expected to have all-null types"
    by_name = {p["port"]: port_to_netbox(p) for p in ports}
    # VLAN subif on a LAG, physical member, and the LAG itself: no opinion on
    # type (None) so the existing Netbox type survives the diff.
    assert by_name["if-roo-sbc-164"]["type"] is None
    assert by_name["port11"]["type"] is None
    assert by_name["LAG-internal"]["type"] is None
    # Name rule still applies without ifType
    assert by_name["mgmt"]["type"] == "1000base-t"
    # Everything else still parses
    assert by_name["if-roo-sbc-164"]["speed"] == 2_000_000
    assert by_name["if-roo-sbc-164"]["enabled"] is True


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


def test_stack_member_count() -> None:
    """Only a traditional stack with >1 member gets a value; a degraded
    stack down to its last member, standalone, VSS, and FEX are all left
    unset (None) since a count of 1 carries no signal."""
    chassis = [{"name": "Switch 1"}, {"name": "Switch 2"}, {"name": "Switch 3"}]
    assert _stack_member_count("stack", chassis) == 3
    assert _stack_member_count("stack", chassis[:1]) is None  # degraded to 1 member
    assert _stack_member_count("standalone", chassis[:1]) is None
    assert _stack_member_count("vss", chassis[:2]) is None
    assert _stack_member_count("fex", chassis) is None


def test_should_update_stack_members() -> None:
    # No change: never "update" (would be a no-op write anyway).
    assert _should_update_stack_members(3, 3, only_increase=True) is False
    # Never written before (None) or non-numeric: always allow.
    assert _should_update_stack_members(None, 3, only_increase=True) is True
    # Increase: always allowed regardless of only_increase.
    assert _should_update_stack_members(2, 3, only_increase=True) is True
    assert _should_update_stack_members(2, 3, only_increase=False) is True
    # Decrease: blocked by default (a dead member shouldn't ratchet it down)...
    assert _should_update_stack_members(3, 2, only_increase=True) is False
    # ...but allowed when the operator explicitly disables the guard.
    assert _should_update_stack_members(3, 2, only_increase=False) is True


def test_discovery_incomplete() -> None:
    """All-numeric port names = ifIndex placeholders from an unfinished
    Netdisco discovery → sync must be skipped. Anything else syncs."""
    numeric = [{"port": "1"}, {"port": "2"}, {"port": "10105"}]
    assert _discovery_incomplete(numeric) is True
    # descr fallback when port is missing
    assert _discovery_incomplete([{"descr": "3"}, {"port": "4"}]) is True
    # mixed: one real name is enough to proceed
    assert _discovery_incomplete([{"port": "1"}, {"port": "GigabitEthernet1/0/1"}]) is False
    assert _discovery_incomplete([{"port": "port1"}, {"port": "port2"}]) is False
    # dot/slash notation is not a bare number
    assert _discovery_incomplete([{"port": "1/1"}, {"port": "1.100"}]) is False
    # no ports at all is a different situation — do not skip
    assert _discovery_incomplete([]) is False


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
