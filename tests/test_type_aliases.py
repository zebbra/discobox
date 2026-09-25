"""Unit tests for DeviceType/ModuleType alias resolution (types.* config).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_type_aliases.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient, _compile_type_aliases, _split_aliases


class FakeType(SimpleNamespace):
    def update(self, data):
        for k, v in data.items():
            setattr(self, k, v)
        return True


class FakeEndpoint:
    """Minimal pynetbox endpoint: exact-match filters, cf_<name>__ic substring filter."""

    def __init__(self, records):
        self.records = list(records)
        self.created = []

    def filter(self, **kw):
        out = []
        for r in self.records:
            ok = True
            for k, v in kw.items():
                if k.startswith("cf_") and k.endswith("__ic"):
                    cf = k[3:-4]
                    val = (r.custom_fields or {}).get(cf) or ""
                    ok = ok and str(v).lower() in str(val).lower()
                elif k == "manufacturer_id":
                    ok = ok and r.manufacturer.id == v
                else:
                    ok = ok and getattr(r, k, None) == v
            if ok:
                out.append(r)
        return out

    def create(self, **kw):
        rec = FakeType(id=1000 + len(self.created), custom_fields={}, **kw)
        self.created.append(rec)
        return rec


def _client(device_types=(), module_types=(), cf_types=("dcim.devicetype", "dcim.moduletype"), **kw):
    nb = NetboxClient.__new__(NetboxClient)
    mfrs = [SimpleNamespace(id=1, name="Unknown")]
    nb.nb = SimpleNamespace(
        dcim=SimpleNamespace(
            device_types=FakeEndpoint(device_types),
            module_types=FakeEndpoint(module_types),
            manufacturers=SimpleNamespace(
                filter=lambda **f: [m for m in mfrs if m.name.lower() == f["name__ie"].lower()],
                create=lambda **c: SimpleNamespace(id=2, **c),
            ),
        ),
        extras=SimpleNamespace(
            custom_fields=SimpleNamespace(
                filter=lambda **f: [SimpleNamespace(name="snmp_models", object_types=list(cf_types))]
            ),
        ),
    )
    nb.type_alias_cf = kw.get("type_alias_cf", "snmp_models")
    nb.device_type_aliases = _compile_type_aliases(kw.get("device_type_aliases"))
    nb.module_type_aliases = _compile_type_aliases(kw.get("module_type_aliases"))
    nb.create_missing_types = kw.get("create_missing_types", True)
    nb._alias_cf_enabled = {}
    return nb


EIP_MFR = SimpleNamespace(id=5, name="EfficientIP")
EIP = FakeType(id=10, model="EfficientIP Generic", part_number="efficientip-generic",
               slug="efficientip-generic", manufacturer=EIP_MFR, custom_fields={})


def test_config_alias_maps_raw_model_to_part_number():
    nb = _client([EIP], device_type_aliases={"enterprises.2440": "efficientip-generic"})
    assert nb.get_or_create_device_type("Unknown", "enterprises.2440", part_number="enterprises.2440") is EIP
    assert nb.nb.dcim.device_types.created == []


def test_config_alias_is_case_insensitive_and_supports_regex():
    nb = _client([EIP], device_type_aliases={"/^enterprises\\.2440(\\.|$)/": "efficientip-generic"})
    assert nb.get_or_create_device_type("Unknown", "Enterprises.2440.1.2") is EIP
    nb = _client([EIP], device_type_aliases={"ENTERPRISES.2440": "efficientip-generic"})
    assert nb.get_or_create_device_type("Unknown", "enterprises.2440") is EIP


def test_config_alias_wins_over_existing_bogus_type():
    bogus = FakeType(id=11, model="enterprises.2440", part_number="enterprises.2440",
                     slug="enterprises-2440", manufacturer=SimpleNamespace(id=1, name="Unknown"),
                     custom_fields={})
    nb = _client([bogus, EIP], device_type_aliases={"enterprises.2440": "efficientip-generic"})
    assert nb.get_or_create_device_type("Unknown", "enterprises.2440", part_number="enterprises.2440") is EIP


def test_alias_to_missing_target_never_creates_raw_type():
    nb = _client([], device_type_aliases={"enterprises.2440": "efficientip-generic"})
    assert nb.get_or_create_device_type("Unknown", "enterprises.2440") is None
    assert nb.nb.dcim.device_types.created == []


def test_custom_field_alias():
    eip = FakeType(**{**vars(EIP), "custom_fields": {"snmp_models": "enterprises.2440, SOLIDserver-550"}})
    nb = _client([eip])
    assert nb.get_or_create_device_type("Unknown", "enterprises.2440") is eip
    assert nb.get_or_create_device_type("Unknown", "solidserver-550") is eip
    # substring hit from __ic must not count as an alias match
    assert nb.get_or_create_device_type("Unknown", "SOLIDserver-5").model == "SOLIDserver-5"


def test_custom_field_ignored_when_not_assigned_to_object_type():
    eip = FakeType(**{**vars(EIP), "custom_fields": {"snmp_models": "SOLIDserver-550"}})
    nb = _client([eip], cf_types=("dcim.device",))
    assert nb.get_or_create_device_type("Unknown", "SOLIDserver-550").id != eip.id


def test_create_missing_off_returns_none():
    nb = _client([], create_missing_types=False)
    assert nb.get_or_create_device_type("Unknown", "WS-C9999") is None
    assert nb.get_or_create_module_type(SimpleNamespace(id=1, name="Unknown"), "GLC-X") is None
    assert nb.nb.dcim.device_types.created == []
    assert nb.nb.dcim.module_types.created == []


def test_create_false_is_lookup_only():
    nb = _client([])
    assert nb.get_or_create_device_type("Unknown", "C9120AXE-E", part_number="C9120AXE-E", create=False) is None
    assert nb.nb.dcim.device_types.created == []


def test_alias_to_model_of_type_without_part_number():
    # prod AP types: model "9120AX" / "Catalyst 9120AXI-E", part_number null
    cisco = SimpleNamespace(id=1, name="Cisco")
    axe = FakeType(id=131, model="9120AX", part_number=None, slug="9120ax", manufacturer=cisco, custom_fields={})
    axi = FakeType(id=132, model="Catalyst 9120AXI-E", part_number=None, slug="catalyst-9120axi-e",
                   manufacturer=cisco, custom_fields={})
    nb = _client([axe, axi], device_type_aliases={"C9120AXE-E": "9120AX", "C9120AXI-E": "Catalyst 9120AXI-E"})
    assert nb.get_or_create_device_type("Cisco", "C9120AXE-E", part_number="C9120AXE-E", create=False) is axe
    assert nb.get_or_create_device_type("Cisco", "C9120AXI-E", part_number="C9120AXI-E", create=False) is axi
    assert nb.nb.dcim.device_types.created == []


def test_default_behaviour_still_creates():
    nb = _client([])
    dt = nb.get_or_create_device_type("Unknown", "WS-C9999", part_number="WS-C9999")
    assert dt.model == "WS-C9999" and dt.manufacturer == 1


def test_module_alias():
    mt = FakeType(id=20, model="GLC-SX-MM", part_number="GLC-SX-MM", slug="glc-sx-mm",
                  manufacturer=SimpleNamespace(id=3, name="Cisco"), custom_fields={})
    nb = _client(module_types=[mt], module_type_aliases={"/^GLC-SX-MMD?$/": "GLC-SX-MM"})
    assert nb.get_or_create_module_type(SimpleNamespace(id=3, name="Cisco"), "GLC-SX-MMD") is mt


def test_resolve_device_type_alias_never_creates():
    nb = _client([EIP], device_type_aliases={"enterprises.2440": "efficientip-generic"})
    assert nb.resolve_device_type_alias(None, "enterprises.2440") is EIP
    assert nb.resolve_device_type_alias("something-else") is None
    assert nb.nb.dcim.device_types.created == []


def test_split_aliases():
    assert _split_aliases("a, b\nc,,") == ["a", "b", "c"]
    assert _split_aliases(["a", " b "]) == ["a", "b"]
    assert _split_aliases(None) == []


N9K = FakeType(id=30, model="N9K-C9332D-GX2B", part_number="N9K-C9332D-GX2B", slug="n9k-c9332d-gx2b",
               manufacturer=SimpleNamespace(id=3, name="Cisco"), custom_fields={})
FWB = FakeType(id=31, model="FortiWeb VM", part_number="FWB_VM", slug="fortiweb-vm",
               manufacturer=SimpleNamespace(id=4, name="Fortinet"), custom_fields={})
SCOPED = {
    "enterprises.2440": "efficientip-generic",
    "cisco": {".1570": "N9K-C9332D-GX2B"},
    "fortinet": {".107.1.50001": "FWB_VM"},
}


def test_is_oid_model():
    from discobox import is_oid_model
    for oid in ("enterprises.2440", ".1570", ".107.1.50001", "1.3.6.1.4.1.9", ".112.100.1003"):
        assert is_oid_model(oid), oid
    for real in ("N9K-C9332D-GX2B", "FWB_VM", "2960", "WS-C3850-48P", "ISR4451-X/K9", "", None):
        assert not is_oid_model(real), real


def test_vendor_scoped_alias_by_netdisco_vendor_or_manufacturer():
    nb = _client([N9K, FWB, EIP], device_type_aliases=SCOPED)
    assert nb.get_or_create_device_type("Unknown", ".1570", part_number=".1570", vendor="cisco") is N9K
    assert nb.get_or_create_device_type(SimpleNamespace(id=3, name="Cisco"), ".1570") is N9K
    assert nb.get_or_create_device_type("Unknown", ".107.1.50001", vendor="Fortinet") is FWB
    # unscoped alias applies to any vendor ("Efficient IP" from Netdisco)
    assert nb.get_or_create_device_type("Efficient IP", "enterprises.2440", vendor="Efficient IP") is EIP


def test_vendor_scoped_alias_ignored_for_other_vendor_and_oid_never_created():
    nb = _client([N9K], device_type_aliases=SCOPED)
    assert nb.get_or_create_device_type("Unknown", ".1570", part_number=".1570", vendor="juniper") is None
    assert nb.nb.dcim.device_types.created == []


def test_oid_part_number_dropped_when_model_is_real():
    # Fortinet: model parsed from sw_ver, part_number is the raw OID chassis model
    nb = _client([])
    dt = nb.get_or_create_device_type("Fortinet", "FortiWeb-VM", part_number=".107.1.99999")
    assert dt.model == "FortiWeb-VM" and dt.part_number == "FortiWeb-VM"


def test_oid_existing_bogus_type_not_matched():
    bogus = FakeType(id=12, model=".1570", part_number=".1570", slug="1570",
                     manufacturer=SimpleNamespace(id=3, name="Cisco"), custom_fields={})
    nb = _client([bogus])
    assert nb.get_or_create_device_type(SimpleNamespace(id=3, name="Cisco"), ".1570", part_number=".1570") is None


def test_oid_module_type_not_created():
    nb = _client([])
    assert nb.get_or_create_module_type(SimpleNamespace(id=3, name="Cisco"), ".1570") is None
    assert nb.nb.dcim.module_types.created == []


def test_custom_field_alias_prefers_matching_vendor():
    other = FakeType(id=40, model="Other", part_number="other", slug="other",
                     manufacturer=SimpleNamespace(id=9, name="Juniper Networks"),
                     custom_fields={"snmp_models": ".1570"})
    n9k = FakeType(**{**vars(N9K), "custom_fields": {"snmp_models": ".1570"}})
    nb = _client([other, n9k])
    assert nb.get_or_create_device_type("Unknown", ".1570", vendor="cisco") is n9k


def test_unquoted_yaml_float_key_ignored():
    import yaml
    compiled = _compile_type_aliases(yaml.safe_load("{.1570: X, '.1571': Y}"))
    assert [t for _, _, t in compiled] == ["Y"]


def test_resolve_alias_with_vendor():
    nb = _client([N9K], device_type_aliases=SCOPED)
    assert nb.resolve_device_type_alias(".1570", vendor="cisco") is N9K
    assert nb.resolve_device_type_alias(".1570") is None
