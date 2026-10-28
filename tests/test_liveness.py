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
from discobox import discover_missing, fetch_liveness, find_gaps
from discobox.ha import _resolve_ha_member_ip

# ── fetch_liveness ─────────────────────────────────────────────────────────────

class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise discobox.reconcile.requests.HTTPError(f"{self.status_code}")

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

    monkeypatch.setattr(discobox.reconcile.requests, "get", fake_get)
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
        ({"fgHaStatsHostname": "ClusterA1", "netbox_primary_ip": "198.51.100.11"}, 1),
    )
    calls = _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip("http://vmselect:8481/select/0/prometheus/", "ClusterA1")
    assert got == "198.51.100.11"
    assert calls["url"] == "http://vmselect:8481/select/0/prometheus/api/v1/query"
    assert calls["params"] == {"query": 'fgHaStatsSyncStatus_info{fgHaStatsHostname="ClusterA1"}'}


def test_resolve_ha_member_ip_strips_cidr(monkeypatch) -> None:
    payload = _vm_payload(({"fgHaStatsHostname": "ClusterA1", "netbox_primary_ip": "198.51.100.11/24"}, 1))
    _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip("http://vm", "ClusterA1")
    assert got == "198.51.100.11"


def test_resolve_ha_member_ip_no_match_returns_none(monkeypatch) -> None:
    _patch_get(monkeypatch, _vm_payload())
    got = _resolve_ha_member_ip("http://vm", "ClusterA1")
    assert got is None


def test_resolve_ha_member_ip_custom_metric_and_labels(monkeypatch) -> None:
    payload = _vm_payload(({"unit_name": "ClusterA1", "target_ip": "198.51.100.11"}, 1))
    calls = _patch_get(monkeypatch, payload)
    got = _resolve_ha_member_ip(
        "http://vm", "ClusterA1",
        metric="my_ha_info", hostname_label="unit_name", target_label="target_ip",
    )
    assert got == "198.51.100.11"
    assert calls["params"] == {"query": 'my_ha_info{unit_name="ClusterA1"}'}


def test_resolve_ha_member_ip_escapes_hostname_quotes(monkeypatch) -> None:
    calls = _patch_get(monkeypatch, _vm_payload())
    _resolve_ha_member_ip("http://vm", 'weird"host')
    assert calls["params"] == {"query": 'fgHaStatsSyncStatus_info{fgHaStatsHostname="weird\\"host"}'}


def test_resolve_ha_member_ip_error_status_raises(monkeypatch) -> None:
    _patch_get(monkeypatch, {"status": "error", "error": "boom"})
    with pytest.raises(RuntimeError, match="boom"):
        _resolve_ha_member_ip("http://vm", "ClusterA1")


# ── gaps + discover-missing with liveness ──────────────────────────────────────

def reconcile_devices(nd, nb, max_queued=None, max_failed=None, max_enqueue=None, require_auth_tag=False,
                      auto_create_role=None, auto_create_site=None, **gaps_kwargs):
    """find_gaps, then discover_missing on its lists (what the two jobs do), counts merged."""
    gaps = find_gaps(nd, nb, **gaps_kwargs)
    done = discover_missing(nd, nb, gaps["not_in_netdisco_list"], gaps["not_in_netbox_list"],
                            max_queued=max_queued, max_failed=max_failed, max_enqueue=max_enqueue,
                            require_auth_tag=require_auth_tag,
                            auto_create_role=auto_create_role, auto_create_site=auto_create_site)
    return {**gaps, **done}

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

    def all(self):
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
    # down devices stay in the gap list but are excluded from the count
    assert len(counts["not_in_netdisco_list"]) == 3
    assert counts["not_in_netdisco"] == 2


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


def test_discover_missing_needs_only_the_gap_list() -> None:
    """Decoupled from Netbox: the entry carries the auth tag and SNMP budget find_gaps read."""
    class NDRec(FakeND):
        def enqueue_discover(self, ip, device_auth_tag_hint=None, snmp_timeout_us=None):
            self.enqueued.append((ip, device_auth_tag_hint, snmp_timeout_us))
    nd = NDRec()
    entries = [{"ip": "10.0.0.7", "name": "sw-7", "auth_tag": "v3", "snmp_timeout": "5m", "status": "up"},
               {"ip": "10.0.0.8", "name": "sw-8", "auth_tag": None, "snmp_timeout": None}]
    counts = discover_missing(nd, None, entries, max_queued=None, max_failed=None, require_auth_tag=True)
    assert nd.enqueued == [("10.0.0.7", "v3", 20_000_000)] and counts["skipped"] == 1


def test_find_gaps_entry_carries_the_discover_inputs() -> None:
    dev = FakeDevice("sw-9", "10.0.0.9")
    dev.custom_fields = {"snmp_auth_profile": "v3", "snmp_polling_timeout": "2m"}
    gaps = find_gaps(FakeND(), FakeNB([dev]))
    assert gaps["not_in_netdisco_list"] == [{"ip": "10.0.0.9", "name": "sw-9", "auth_tag": "v3", "snmp_timeout": "2m"}]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))


def test_fix_tag_mismatches_passes_the_device_snmp_timeout() -> None:
    from discobox import fix_tag_mismatches

    class _ND:
        def __init__(self):
            self.jobs: list = []

        def get_queue_status(self, since="1h"):
            return {"queued": 0, "failed": 0}

        def enqueue_discover(self, ip, device_auth_tag_hint=None, snmp_timeout_us=None):
            self.jobs.append((ip, device_auth_tag_hint, snmp_timeout_us))

    nd = _ND()
    fix_tag_mismatches(nd, [
        {"ip": "192.0.2.1", "name": "wlc", "netbox_tag": "v3", "snmp_polling_timeout": "3m"},
        {"ip": "192.0.2.2", "name": "sw", "netbox_tag": "v3"},          # older gap file: no field
    ], max_queued=None, max_failed=None)
    # "3m" budget → 180s / 12 = 15s per request; no field → the 10s floor
    assert nd.jobs == [("192.0.2.1", "v3", 15_000_000), ("192.0.2.2", "v3", 10_000_000)]


def test_discover_snmptimeout_is_derived_and_bounded() -> None:
    from discobox.util import discover_snmptimeout_us
    assert discover_snmptimeout_us("2m") == 10_000_000     # 120s / 12
    assert discover_snmptimeout_us("5m") == 20_000_000     # 25s → cap
    assert discover_snmptimeout_us("20s") == 10_000_000    # 1.7s → floor
    assert discover_snmptimeout_us("3m") == 15_000_000
    assert discover_snmptimeout_us("") == 10_000_000       # empty → floor
    assert discover_snmptimeout_us(None) == 10_000_000
    assert discover_snmptimeout_us("soon") == 10_000_000   # garbage → floor
    assert discover_snmptimeout_us("180") == 15_000_000    # bare number = seconds
    assert discover_snmptimeout_us("30") == 10_000_000     # bare 30s → 2.5s → floor


def test_discover_snmptimeout_policy_is_configurable() -> None:
    from discobox.util import DISCOVER_SNMPTIMEOUT, configure_discover_snmptimeout, discover_snmptimeout_us
    saved = dict(DISCOVER_SNMPTIMEOUT)
    try:
        configure_discover_snmptimeout(divisor=6, floor="5s", cap="1m")
        assert discover_snmptimeout_us("2m") == 20_000_000
        assert discover_snmptimeout_us("") == 5_000_000
        assert discover_snmptimeout_us("1h") == 60_000_000
    finally:
        DISCOVER_SNMPTIMEOUT.update(saved)


def test_sessions_retry_reads_not_writes() -> None:
    import discobox
    nd = discobox.NetdiscoClient("http://netdisco.example", token="t")     # token: no login request
    for session, url in ((discobox.netbox._ChangelogSession(), "https://netbox.example/api/"),
                         (nd.session, "http://netdisco.example/api/v1/")):
        retry = session.get_adapter(url).max_retries
        assert retry.total == 3 and retry._is_method_retryable("GET")
        assert not retry._is_method_retryable("POST") and not retry._is_method_retryable("PATCH")
        assert not retry._is_method_retryable("DELETE")


# ── reconcile: stale not-in-Netbox devices ─────────────────────────────────────

class FakeNDInventory(FakeND):
    def __init__(self, devices):
        super().__init__()
        self._devices = devices

    def get_all_devices(self):
        return self._devices

    def get_device(self, ip):
        return {"ip": ip}


ND_INVENTORY = [
    {"ip": "10.0.0.1", "name": "sw-up", "last_discover": "2026-10-04 06:00:00.12"},    # in Netbox
    {"ip": "10.9.0.1", "name": "sw-new", "last_discover": "2026-10-03 08:00:00.5"},   # 22h behind newest
    {"ip": "10.9.0.2", "name": "sw-gone", "last_discover": "2026-09-01 08:00:00"},
    {"ip": "10.9.0.3", "name": "sw-never", "last_discover": None},
]


def test_reconcile_lists_stale_not_in_netbox_but_does_not_count_them() -> None:
    counts = reconcile_devices(FakeNDInventory(ND_INVENTORY), FakeNB(DEVICES[:1]),
                               max_queued=None, max_failed=None, max_enqueue=0)
    stale = {e["ip"]: e["stale"] for e in counts["not_in_netbox_list"]}
    assert stale == {"10.9.0.1": False, "10.9.0.2": True, "10.9.0.3": True}
    assert counts["not_in_netbox"] == 1 and counts["not_in_netbox_stale"] == 2
    assert counts["not_in_netbox_list"][0]["last_discover"] == "2026-10-03 08:00"


def test_reconcile_stale_hours_off_counts_everything() -> None:
    counts = reconcile_devices(FakeNDInventory(ND_INVENTORY), FakeNB(DEVICES[:1]),
                               max_queued=None, max_failed=None, max_enqueue=0, not_in_netbox_stale_hours=0)
    assert counts["not_in_netbox"] == 3 and counts["not_in_netbox_stale"] == 0


def test_reconcile_never_auto_creates_stale_devices(monkeypatch) -> None:
    created: list[str] = []
    monkeypatch.setattr(discobox.reconcile, "_create_device_from_nd",
                        lambda nb, nd, ip, dev, **kw: created.append(ip) or True)
    reconcile_devices(FakeNDInventory(ND_INVENTORY), FakeNB(DEVICES[:1]), max_queued=None, max_failed=None,
                      max_enqueue=0, auto_create_role="switch", auto_create_site="lab")
    assert created == ["10.9.0.1"]


# ── gaps: same hostname under a different IP ─────────────────────────────────────

def test_gaps_matches_by_short_hostname_when_ips_differ() -> None:
    nb_devices = [FakeDevice("fw-edge-01.example.net", "10.0.0.5"),    # Netdisco: other IP, no domain
                  FakeDevice("sw-dup", "10.0.0.6"),                    # Netdisco: this IP and an old one
                  FakeDevice("sw-ha-a", "10.0.0.7"),
                  FakeDevice("sw-missing", "10.0.0.8")]
    nd_devices = [
        {"ip": "10.8.0.5", "name": "FW-EDGE-01", "last_discover": "2026-10-04 06:00:00"},
        {"ip": "10.0.0.6", "name": "sw-dup", "last_discover": "2026-10-04 06:00:00"},
        {"ip": "10.8.0.6", "name": "sw-dup", "last_discover": "2026-09-01 06:00:00"},
        {"ip": "10.0.0.7", "name": "sw-ha-a", "last_discover": "2026-10-04 06:00:00"},
        {"ip": "10.8.0.8", "name": "sw-missing", "last_discover": "2026-10-04 06:00:00"},
        {"ip": "10.9.9.9", "name": "10.9.9.9", "last_discover": "2026-10-04 06:00:00"},   # IP as name: no match
    ]
    # sw-missing's Netdisco IP belongs to another Netbox device: not a name match
    nb_devices.append(FakeDevice("other", "10.8.0.8"))
    gaps = find_gaps(FakeNDInventory(nd_devices), FakeNB(nb_devices))
    by_name = {e["name"]: e for e in gaps["ip_mismatches_list"]}
    assert set(by_name) == {"fw-edge-01.example.net", "sw-dup"}
    assert by_name["fw-edge-01.example.net"] == {
        "name": "fw-edge-01.example.net", "netbox_ip": "10.0.0.5", "netdisco_ip": "10.8.0.5",
        "netdisco_name": "FW-EDGE-01", "last_discover": "2026-10-04 06:00", "stale": False,
        "netdisco_duplicate": False}
    assert by_name["sw-dup"]["netdisco_ip"] == "10.8.0.6"
    assert by_name["sw-dup"]["stale"] and by_name["sw-dup"]["netdisco_duplicate"]
    assert gaps["ip_mismatches"] == 2
    assert [e["ip"] for e in gaps["not_in_netdisco_list"]] == ["10.0.0.8"]
    assert [e["ip"] for e in gaps["not_in_netbox_list"]] == ["10.9.9.9"]
