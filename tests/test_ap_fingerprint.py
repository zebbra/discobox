"""WLC AP pass skip: fingerprint, plan (full / skip + retry), state round trip."""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import discobox  # noqa: E402
from discobox import _ap_fingerprint, _load_ap_state, _plan_ap_pass, _save_ap_state  # noqa: E402

MODS = [{"name": "AP-A", "description": "x", "model": "C9120AXI-E", "serial": "FAKE0001", "last_seen": 1}]
PORTS = [{"port": "00:00:5e:00:53:01.0", "mac": "00:00:5e:00:53:01", "type": "dot11a", "up": "up"}]


def test_fingerprint_tracks_what_the_pass_reads_only() -> None:
    fp = _ap_fingerprint(MODS, PORTS, [1, "wlc"])
    assert fp == _ap_fingerprint([dict(MODS[0], last_seen=2)], [dict(PORTS[0], up="down")], [1, "wlc"])
    assert fp != _ap_fingerprint([dict(MODS[0], serial="FAKE0002")], PORTS, [1, "wlc"])
    assert fp != _ap_fingerprint(MODS, [dict(PORTS[0], mac="00:00:5e:00:53:02")], [1, "wlc"])
    assert fp != _ap_fingerprint(MODS, PORTS, [1, "wlc-renamed"])
    two = MODS + [dict(MODS[0], name="AP-B", serial="FAKE0002")]
    assert _ap_fingerprint(two, PORTS, [1]) == _ap_fingerprint(list(reversed(two)), PORTS, [1])   # order-free


def test_plan(monkeypatch) -> None:
    monkeypatch.setattr(discobox, "AP_STATE_DIR", "/nonexistent")
    monkeypatch.setattr(discobox, "AP_FULL_PASS_DAYS", 7)
    now = 1_000_000.0
    st = {"fingerprint": "f", "full_at": now - 3600, "pending": ["AP-X"]}
    assert _plan_ap_pass(st, "f", False, now) == (False, {"AP-X"})     # unchanged: skip, retry pending
    assert _plan_ap_pass(st, "g", False, now) == (True, set())         # changed
    assert _plan_ap_pass(st, "f", True, now) == (True, set())          # rebuild
    assert _plan_ap_pass(dict(st, full_at=now - 8 * 86400), "f", False, now) == (True, set())   # due
    assert _plan_ap_pass({}, "f", False, now) == (True, set())         # first pass
    monkeypatch.setattr(discobox, "AP_STATE_DIR", None)
    assert _plan_ap_pass(st, "f", False, now) == (True, set())         # feature off


def test_state_round_trip(monkeypatch) -> None:
    monkeypatch.setattr(discobox, "AP_STATE_DIR", tempfile.mkdtemp())
    assert _load_ap_state(7) == {}
    _save_ap_state(7, {"fingerprint": "f", "full_at": 1.0, "pending": []})
    assert _load_ap_state(7)["fingerprint"] == "f" and _load_ap_state(8) == {}
