"""Cisco 9800 HA SSO pairs: chassis selection, peer naming and the HA note (fixtures: wlc9800ha-*)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import (  # noqa: E402
    _HA_NOTE_RE,
    _ha_note_block,
    _ha_peer_name,
    _merge_note_block,
    _pick_primary_chassis,
)

SAMPLES = Path(__file__).resolve().parent / "samples"


def _load():
    device = json.loads((SAMPLES / "wlc9800ha-device.json").read_text())
    mods = json.loads((SAMPLES / "wlc9800ha-modules.json").read_text())
    return device, mods


def test_sample_is_a_multi_chassis_system_with_two_units() -> None:
    device, mods = _load()
    root = next(m for m in mods if m["class"] == "stack")
    assert root["name"] == "Multi Chassis System"
    chassis = [m for m in mods if m["class"] == "chassis" and m["model"]]
    assert sorted(c["pos"] for c in chassis) == [1, 2]
    # the logical controller reports every unit's serial in one field
    assert sorted(device["serial"].split()) == sorted(c["serial"] for c in chassis)


def test_primary_is_chassis_1_when_the_device_lists_all_serials() -> None:
    device, mods = _load()
    chassis = [m for m in mods if m["class"] == "chassis" and m["model"]]
    for order in (chassis, list(reversed(chassis))):     # list order must not matter
        assert _pick_primary_chassis(order, device["serial"])["pos"] == 1


def test_primary_single_serial_match_wins() -> None:
    chassis = [{"pos": 1, "serial": "AAA"}, {"pos": 2, "serial": "BBB"}]
    assert _pick_primary_chassis(chassis, "BBB")["serial"] == "BBB"     # StackWise Virtual style
    assert _pick_primary_chassis(chassis, "")["pos"] == 1
    assert _pick_primary_chassis([{"pos": None, "serial": "X"}, {"pos": 2, "serial": "Y"}], "")["pos"] == 2


def test_peer_name() -> None:
    assert _ha_peer_name("wlc1.example.com", 2) == "wlc1-2.example.com"
    assert _ha_peer_name("wlc1", 2) == "wlc1-2"


def test_ha_note_block_and_location_hint_lifecycle() -> None:
    created = _ha_note_block("wlc1.example.com", 2, created=True, location_missing=True)
    assert created.startswith("<!-- discobox:ha -->\n## HA peer (discobox)\n")
    assert " - Chassis 2 of wlc1.example.com" in created
    assert "Created by discobox" in created and "set them manually" in created
    comments = _merge_note_block("", created, _HA_NOTE_RE)
    assert comments == created
    # location set later: only the hint disappears, the rest (incl. "created") stays
    located = _ha_note_block("wlc1.example.com", 2, created=True, location_missing=False)
    merged = _merge_note_block("manual text\n\n" + comments, located, _HA_NOTE_RE)
    assert merged.startswith("manual text\n\n") and "set them manually" not in merged
    assert _merge_note_block(merged, located, _HA_NOTE_RE) is None     # idempotent
