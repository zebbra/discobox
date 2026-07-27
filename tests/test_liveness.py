"""Unit tests for the reconcile liveness gate (fetch_liveness + reconcile filtering).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_liveness.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import discobox
from discobox import _resolve_ha_member_ip, fetch_liveness, reconcile_devices

# ── fetch_liveness ─────────────────────────────────────────────────────────────

class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise discobox.requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


def _vm_payload(*series):
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": metric, "value": [1_700_000_000, str(value)]}
                for metric, value in series
            ],
        },
    }


def _patch_get(monkeypatch, payload):
    calls = {}

    def fake_get(url, params=None, **kwargs):
        calls["url"] = url
        calls["params"] = params
        return FakeResponse(payload)

    monkeypatch.setattr(discobox.requests, "get", fake_get)
    return calls


def test_fetch_liveness_maps_label_to_bool(monkeypatch) -> None:
    payload = _vm_payload(
        ({"netbox_primary_ip": "10.0.0.1"}, 1),
        ({"netbox_primary_ip": "10.0.0.2"}, 0),
    )
    calls = _patch_get(monkeypatch, payload)
    got = fetch_liveness("http://vmselect:8481/select/0/prometheus/", "up")
    assert got == {"10.0.0.1": True, "10.0.0.2": False}
    # trailing slash stripped, query passed through
    assert calls["url"] == "http://vmselect:8481/select/0/prometheus/api/v1/query"
    assert calls["params"] == {"query": "up"}


def test_fetch_liveness_normalizes_cidr_and_dedupes_any_up(monkeypatch) -> None:
    payload = _vm_payload(
        ({"netbox_primary_ip": "10.0.0.1/24"}, 0),
        ({"netbox_primary_ip": "10.0.0.1"}, 1),   # second series wins: any > 0 = up
        ({"other_label": "10.0.0.9"}, 1),          # missing label: skipped
    )
    _patch_get(monkeypatch, payload)
    got = fetch_liveness("http://vm", "up")
    assert got == {"10.0.0.1": True}


def test_fetch_liveness_custom_label(monkeypatch) -> None:
    payload = _vm_payload(({"instance": "sw1.example.com"}, 1))
    _patch_get(monkeypatch, payload)
    got = fetch_liveness("http://vm", "up", label="instance")
    assert got == {"sw1.example.com": True}


def test_fetch_liveness_error_status_raises(monkeypatch) -> None:
    _patch_get(monkeypatch, {"status": "error", "error": "boom"})
    with pytest.raises(RuntimeError, match="boom"):
        fetch_liveness("http://vm", "up")


# ── _resolve_ha_member_ip ────────────────────────────────────────────────────────

def test_resolve_ha_member_ip_returns_target_label(monkeypatch) -> None:
    payload = _vm_payload(
        ({"fgHaStatsHostname": "PrimusA1", "netbox_primary_ip": "10.219.123.11"}, 1),
    )
    calls = _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip("http://vmselect:8481/select/0/prometheus/", "PrimusA1")
    assert got == "10.219.123.11"
    assert calls["url"] == "http://vmselect:8481/select/0/prometheus/api/v1/query"
    assert calls["params"] == {"query": 'fgHaStatsSyncStatus_info{fgHaStatsHostname="PrimusA1"}'}


def test_resolve_ha_member_ip_strips_cidr(monkeypatch) -> None:
    payload = _vm_payload(({"fgHaStatsHostname": "PrimusA1", "netbox_primary_ip": "10.219.123.11/24"}, 1))
    _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip("http://vm", "PrimusA1")
    assert got == "10.219.123.11"


def test_resolve_ha_member_ip_no_match_returns_none(monkeypatch) -> None:
    _patch_get(monkeypatch, _vm_payload())
    got = _resolve_ha_member_ip("http://vm", "PrimusA1")
    assert got is None


def test_resolve_ha_member_ip_custom_metric_and_labels(monkeypatch) -> None:
    payload = _vm_payload(({"unit_name": "PrimusA1", "target_ip": "10.219.123.11"}, 1))
    calls = _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip(
        "http://vm", "PrimusA1",
        metric="my_ha_info", hostname_label="unit_name", target_label="target_ip",
    )
    assert got == "10.219.123.11"
    assert calls["params"] == {"query": 'my_ha_info{unit_name="PrimusA1"}'}


def test_resolve_ha_member_ip_escapes_hostname_quotes(monkeypatch) -> None:
    calls = _patch_get(monkeypatch, _vm_payload())
    _resolve_ha_member_ip("http://vm", 'weird"host')
    assert calls["params"] == {"query": 'fgHaStatsSyncStatus_info{fgHaStatsHostname="weird\\"host"}'}


def test_resolve_ha_member_ip_error_status_raises(monkeypatch) -> None:
    _patch_get(monkeypatch, {"status": "error", "error": "boom"})
    with pytest.raises(RuntimeError, match="boom"):
        _resolve_ha_member_ip("http://vm", "PrimusA1")


# ── reconcile_devices with liveness gate ───────────────────────────────────────

class FakeND:
    def __init__(self):
        self.enqueued: list[str] = []

    def get_all_devices(self):
        return []

    def enqueue_discover(self, ip, device_auth_tag_hint=None, snmp_timeout_us=None):
        self.enqueued.append(ip)


class FakeDevice:
    def __init__(self, name, ip):
        self.name = name
        self.primary_ip4 = f"{ip}/24"
        self.custom_fields: dict = {}


class FakeDeviceEndpoint:
    def __init__(self, devices):
        self._devices = devices
        self.filter_calls: list[dict] = []

    def count(self, **kwargs):
        return len(self._devices)

    def filter(self, **kwargs):
        self.filter_calls.append(kwargs)
        return list(self._devices)


class FakeNB:
    def __init__(self, devices):
        class _NB:
            pass
        self.nb = _NB()
        self.nb.dcim = _NB()
        self.nb.dcim.devices = FakeDeviceEndpoint(devices)


DEVICES = [
    FakeDevice("sw-up",      "10.0.0.1"),
    FakeDevice("sw-down",    "10.0.0.2"),
    FakeDevice("sw-unknown", "10.0.0.3"),
]
LIVENESS = {"10.0.0.1": True, "10.0.0.2": False}


def _reconcile(liveness):
    nd = FakeND()
    counts = reconcile_devices(
        nd, FakeNB(DEVICES),
        max_queued=None, max_failed=None,
        liveness=liveness,
    )
    return nd, counts


def test_reconcile_skips_down_enqueues_up_and_unknown() -> None:
    nd, counts = _reconcile(LIVENESS)
    assert nd.enqueued == ["10.0.0.1", "10.0.0.3"]  # down device skipped, unknown fails open
    assert counts["enqueued"] == 2
    assert counts["skipped_offline"] == 1
    statuses = {e["ip"]: e["status"] for e in counts["not_in_netdisco_list"]}
    assert statuses == {"10.0.0.1": "up", "10.0.0.2": "down", "10.0.0.3": "unknown"}
    # down devices stay visible in the gap list/count
    assert counts["not_in_netdisco"] == 3


def test_reconcile_default_status_filter_is_active_only() -> None:
    nb = FakeNB(DEVICES)
    reconcile_devices(FakeND(), nb, max_queued=None, max_failed=None)
    assert nb.nb.dcim.devices.filter_calls[0]["status"] == ["active"]


def test_reconcile_statuses_param_passed_through() -> None:
    nb = FakeNB(DEVICES)
    reconcile_devices(FakeND(), nb, max_queued=None, max_failed=None, statuses=["active", "maintenance"])
    assert nb.nb.dcim.devices.filter_calls[0]["status"] == ["active", "maintenance"]


def test_reconcile_require_auth_tag_skips_devices_without_snmp_profile() -> None:
    tagged = FakeDevice("sw-tagged", "10.0.0.4")
    tagged.custom_fields = {"snmp_auth_profile": "default"}
    untagged = FakeDevice("sw-untagged", "10.0.0.5")
    nd = FakeND()
    counts = reconcile_devices(
        nd, FakeNB([tagged, untagged]), max_queued=None, max_failed=None, require_auth_tag=True,
    )
    assert nd.enqueued == ["10.0.0.4"]
    assert counts["enqueued"] == 1 and counts["skipped"] == 1


def test_reconcile_liveness_disabled_enqueues_all() -> None:
    nd, counts = _reconcile(None)
    assert nd.enqueued == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
    assert counts["skipped_offline"] == 0
    assert all("status" not in e for e in counts["not_in_netdisco_list"])


def test_reconcile_liveness_by_name() -> None:
    nd = FakeND()
    counts = reconcile_devices(
        nd, FakeNB(DEVICES),
        max_queued=None, max_failed=None,
        liveness={"sw-down": False, "sw-up": True},
        liveness_key="name",
    )
    assert nd.enqueued == ["10.0.0.1", "10.0.0.3"]
    assert counts["skipped_offline"] == 1


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
