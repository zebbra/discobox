"""Tests for typesync: library index, matching and fill-blank diff (no Netbox needed)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from typesync import (  # noqa: E402
    CREATED_BY_DISCOBOX,
    load_library,
    match_library,
    plan_components,
    plan_type_fields,
)

AP_YAML = """\
---
manufacturer: Cisco
model: Catalyst 9120AXI-E
slug: cisco-c9120axi-e
part_number: C9120AXI-E
u_height: 0
is_full_depth: false
weight: 1.3
weight_unit: kg
comments: '[Datasheet](https://example.com/9120)'
interfaces:
  - name: Dot11Radio0
    type: ieee802.11ax
  - name: Dot11Radio1
    type: ieee802.11ax
  - name: GigabitEthernet0
    type: 2.5gbase-t
    poe_mode: pd
    poe_type: type2-ieee802.3at
console-ports:
  - name: console
    type: rj-45
module-bays:
  - name: Slot 1
"""


@pytest.fixture
def library(tmp_path: Path) -> Path:
    d = tmp_path / "lib" / "device-types" / "Cisco"
    d.mkdir(parents=True)
    (d / "C9120AXI-E.yaml").write_text(AP_YAML)
    (d / "broken.yaml").write_text("model: [unclosed")
    return tmp_path / "lib"


def _nb_type(**kw) -> dict:
    base = {"model": "9120AX", "slug": "9120ax", "part_number": "", "manufacturer": "Cisco",
            "weight": None, "weight_unit": None, "airflow": None, "description": "",
            "comments": "", "u_height": 1.0}
    return {**base, **kw}


def test_load_library_indexes_part_number_model_slug(library: Path) -> None:
    index = load_library([library])
    for key in ("c9120axi-e", "catalyst 9120axi-e", "cisco-c9120axi-e"):
        assert index[key]["part_number"] == "C9120AXI-E"
    assert index["c9120axi-e"]["_file"] == "device-types/Cisco/C9120AXI-E.yaml"


def test_overlay_wins_and_fills_gaps(library: Path, tmp_path: Path) -> None:
    d = tmp_path / "overlay" / "device-types" / "Cisco"
    d.mkdir(parents=True)
    (d / "C9120AXE-E.yaml").write_text(AP_YAML.replace("AXI", "AXE").replace("axi", "axe"))
    (d / "C9120AXI-E.yaml").write_text(AP_YAML.replace("weight: 1.3", "weight: 1.4"))
    index = load_library([library, tmp_path / "overlay"])
    assert index["c9120axe-e"]["model"] == "Catalyst 9120AXE-E"
    assert index["c9120axi-e"]["weight"] == 1.4


def test_load_library_requires_device_types_dir(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_library([tmp_path])


def test_match_by_mapping_then_part_number(library: Path) -> None:
    index = load_library([library])
    # bossy type: no part_number, only the mapping finds it (keys case-insensitive)
    entry, how = match_library(_nb_type(), index, {"9120ax": "C9120AXI-E"})
    assert how == "map" and entry["part_number"] == "C9120AXI-E"
    assert match_library(_nb_type(), index, {}) == (None, "")
    # part_number match needs the same manufacturer
    entry, how = match_library(_nb_type(part_number="C9120AXI-E"), index)
    assert how == "part_number"
    assert match_library(_nb_type(part_number="C9120AXI-E", manufacturer="Meraki"), index) == (None, "")
    # a mapping to a missing library entry doesn't fall through to part_number
    assert match_library(_nb_type(part_number="C9120AXI-E"), index, {"9120AX": "nope"}) == (None, "")
    # exact library model name (prod "Catalyst 9120AXI-E", part_number null), vendor case ignored
    entry, how = match_library(_nb_type(model="Catalyst 9120AXI-E", manufacturer="cisco"), index)
    assert how == "model" and entry["part_number"] == "C9120AXI-E"
    assert match_library(_nb_type(model="Catalyst 9120AXI-E", manufacturer="Meraki"), index) == (None, "")


def test_plan_type_fields_fill_blank(library: Path) -> None:
    entry = load_library([library])["c9120axi-e"]
    patch = plan_type_fields(_nb_type(), entry, racked=0)
    assert patch == {
        "weight": 1.3, "weight_unit": "kg",
        "comments": "[Datasheet](https://example.com/9120)",
        "u_height": 0,
    }
    # never overwrites existing values
    assert plan_type_fields(
        _nb_type(weight=2.0, weight_unit="kg", comments="curated", u_height=0.0), entry, racked=0,
    ) == {}


def test_plan_type_fields_discobox_comment_and_rack_guard(library: Path) -> None:
    entry = load_library([library])["c9120axi-e"]
    patch = plan_type_fields(_nb_type(comments=CREATED_BY_DISCOBOX), entry, racked=3)
    assert patch["comments"] == f"[Datasheet](https://example.com/9120)\n\n{CREATED_BY_DISCOBOX}"
    assert "u_height" not in patch      # racked devices: height never changes
    # already enriched once → comment not appended again
    again = plan_type_fields(_nb_type(comments=patch["comments"]), entry, racked=3)
    assert "comments" not in again


def test_plan_components_adds_missing_only_and_skips_bays(library: Path) -> None:
    entry = load_library([library])["c9120axi-e"]
    plan = plan_components({"interfaces": {"gigabitethernet0"}}, entry)
    assert [c["name"] for c in plan["interfaces"]] == ["Dot11Radio0", "Dot11Radio1"]
    assert plan["console-ports"] == [{"name": "console", "type": "rj-45"}]
    assert "module-bays" not in plan
    full = plan_components({}, entry)
    assert full["interfaces"][2] == {
        "name": "GigabitEthernet0", "type": "2.5gbase-t", "poe_mode": "pd", "poe_type": "type2-ieee802.3at",
    }
    assert plan_components({"interfaces": {"Dot11Radio0", "Dot11Radio1", "GigabitEthernet0"},
                            "console-ports": {"console"}}, entry) == {}


# ── sync_types end-to-end against a fake Netbox ────────────────────────────────

class _Rec:
    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.updates: list[dict] = []

    def update(self, patch: dict) -> bool:
        self.updates.append(patch)
        self.__dict__.update(patch)
        return True


class _Endpoint:
    def __init__(self, records=()):
        self.records = list(records)
        self.created: list = []

    def filter(self, **kw):
        out = self.records
        for k, v in kw.items():
            if k == "role":
                out = [r for r in out if r.role in v]
            elif k == "device_type_id":
                out = [r for r in out if r.device_type.id == v]
            else:
                out = [r for r in out if getattr(r, k, None) == v]
        return list(out)

    def get(self, id_):
        return next((r for r in self.records if r.id == id_), None)

    def count(self, **kw):
        rack = kw.pop("rack_id", None)
        out = self.filter(**kw)
        if rack == "null":
            out = [r for r in out if r.rack is None]
        return len(out)

    def create(self, payload):
        self.created.append(payload)


class _FakeNB:
    def __init__(self, dt, devices, tpl_ifaces=()):
        dcim = _Rec(
            device_types=_Endpoint([dt]), devices=_Endpoint(devices),
            interface_templates=_Endpoint(tpl_ifaces), console_port_templates=_Endpoint(),
            console_server_port_templates=_Endpoint(), power_port_templates=_Endpoint(),
        )
        self.nb = _Rec(dcim=dcim)


def _fake(library: Path, racked: bool = False, tpl_ifaces=()):
    from typesync import Library
    dt = _Rec(id=131, model="9120AX", slug="9120ax", part_number="", manufacturer=_Rec(name="Cisco"),
              weight=None, weight_unit=None, airflow=None, description="", comments="",
              u_height=1.0, device_count=2)
    devices = [_Rec(id=i, role="lwapp-ap", device_type=dt, rack=_Rec(id=1) if racked else None) for i in (1, 2)]
    return _FakeNB(dt, devices, tpl_ifaces), dt, Library([library])


def test_sync_types_dry_run_writes_nothing(library: Path) -> None:
    from typesync import sync_types
    nb, dt, lib = _fake(library)
    result = sync_types(nb, lib, {"9120AX": "C9120AXI-E"}, ["lwapp-ap"], [], apply=False)
    item = result["types"][0]
    assert item["library"] == "device-types/Cisco/C9120AXI-E.yaml" and item["match"] == "map"
    assert item["set"]["weight"] == 1.3 and item["set"]["u_height"] == 0
    assert [c["name"] for c in item["add"]["interfaces"]] == ["Dot11Radio0", "Dot11Radio1", "GigabitEthernet0"]
    assert item["not_imported"] == ["module-bays"]
    assert result["summary"] == {"types": 1, "matched": 1, "unmatched": 0, "fields": 4, "templates": 4, "errors": 0}
    assert dt.updates == [] and nb.nb.dcim.interface_templates.created == []


def test_sync_types_apply_fills_blanks_and_adds_missing_templates(library: Path) -> None:
    from typesync import sync_types
    existing = [_Rec(name="GigabitEthernet0", device_type=_Rec(id=131))]
    nb, dt, lib = _fake(library, racked=True, tpl_ifaces=existing)
    result = sync_types(nb, lib, {"9120AX": "C9120AXI-E"}, [], ["9120ax"], apply=True)
    assert result["types"][0]["applied"] is True
    assert "u_height" not in dt.updates[0]            # racked → height untouched
    created = nb.nb.dcim.interface_templates.created
    assert [c["name"] for c in created[0]] == ["Dot11Radio0", "Dot11Radio1"]
    assert all(c["device_type"] == 131 for c in created[0])


def test_sync_types_unmatched_type_reported(library: Path) -> None:
    from typesync import sync_types
    nb, dt, lib = _fake(library)
    result = sync_types(nb, lib, {}, ["lwapp-ap"], [], apply=True)
    assert result["types"][0]["library"] is None
    assert result["summary"]["unmatched"] == 1 and dt.updates == []
