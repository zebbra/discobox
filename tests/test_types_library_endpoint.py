"""/types/library endpoint: request validation and delegation to typesync.sync_types.

Calls the endpoint function directly (fastapi's TestClient needs httpx, which
isn't a dependency).
"""
from __future__ import annotations

import os
import sys
import tempfile
from types import SimpleNamespace

os.environ.setdefault("DISCOBOX_STATE_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import server  # noqa: E402

GET, POST = SimpleNamespace(method="GET"), SimpleNamespace(method="POST")


@pytest.fixture
def calls(monkeypatch) -> list[dict]:
    seen: list[dict] = []

    def fake_sync_types(nb, library, mapping, roles, types, apply=False, bare=False, **kw):
        seen.append({"roles": roles, "types": types, "apply": apply, **({"bare": True} if bare else {})})
        return {"apply": apply, "summary": {}, "types": []}

    monkeypatch.setattr(server, "sync_types", fake_sync_types)
    monkeypatch.setattr(server, "_refresh_types_bare_safe", lambda: None)
    monkeypatch.setattr(server, "_get_library", lambda: object())
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_LIBRARY_ROLES", [])
    return seen


def test_get_is_dry_run(calls) -> None:
    result = server.types_library(GET, role=["lwapp-ap"], type=["9120AX"], apply=False)
    assert result["apply"] is False
    assert calls == [{"roles": ["lwapp-ap"], "types": ["9120AX"], "apply": False}]


def test_apply_needs_post(calls) -> None:
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=["lwapp-ap"], type=None, apply=True)
    assert exc.value.status_code == 405 and calls == []
    server.types_library(POST, role=["lwapp-ap"], type=None, apply=True)
    assert calls[-1]["apply"] is True


def test_nothing_selected_is_rejected(calls) -> None:
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=None, type=None, apply=False)
    assert exc.value.status_code == 400 and calls == []


def test_library_roles_default(calls, monkeypatch) -> None:
    monkeypatch.setattr(server, "_LIBRARY_ROLES", ["lwapp-ap"])
    server.types_library(GET, role=None, type=None, apply=False)
    assert calls[-1]["roles"] == ["lwapp-ap"]


def test_type_only_request_ignores_default_roles(calls, monkeypatch) -> None:
    monkeypatch.setattr(server, "_LIBRARY_ROLES", ["lwapp-ap"])
    server.types_library(GET, role=None, type=["WS-C3560CG-8TC-S"], apply=False)
    assert calls[-1] == {"roles": [], "types": ["WS-C3560CG-8TC-S"], "apply": False}


def test_missing_library_is_503(monkeypatch) -> None:
    monkeypatch.setattr(server, "_library", None)
    monkeypatch.setattr(server, "_LIBRARY_PATH", "/nonexistent")
    with pytest.raises(HTTPException) as exc:
        server._get_library()
    assert exc.value.status_code == 503


GAP_COUNTS = {"netbox_total": 10, "netdisco_total": 9, "already_known": 8, "skipped": 0, "not_in_netdisco": 1,
              "offline": 1, "not_in_netbox": 1, "not_in_netbox_stale": 0, "tag_mismatches": 2}


def _gaps_env(monkeypatch, find_gaps):
    """server with find_gaps stubbed, its own job registry and state files."""
    from prometheus_client import CollectorRegistry

    from jobs import Job, JobRegistry
    state = tempfile.mkdtemp(prefix="discobox-state-")
    jobs = JobRegistry(os.path.join(state, "jobs.json"), registry=CollectorRegistry())
    jobs.register(Job("gaps", server._job_gaps))
    jobs.register(Job("discover-missing", server._job_discover_missing))
    monkeypatch.setattr(server, "_jobs", jobs)
    monkeypatch.setattr(server, "find_gaps", find_gaps)
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: object())
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_LIVENESS_URL", None)
    for attr, name in (("_GAPS_COUNTS_FILE", "gaps"), ("_NOT_IN_NETDISCO_FILE", "nind"),
                       ("_NOT_IN_NETBOX_FILE", "ninb"), ("_TAG_MISMATCHES_FILE", "tm")):
        monkeypatch.setattr(server, attr, os.path.join(state, name + ".json"))
    return jobs


def test_gaps_view_runs_the_gaps_job_read_only(monkeypatch) -> None:
    seen: dict = {}

    def fake_find_gaps(nd, nb, **kw):
        seen.update(kw)
        return {**GAP_COUNTS, "not_in_netdisco_list": [{"ip": "192.0.2.1"}], "not_in_netbox_list": [],
                "tag_mismatches_list": []}

    _gaps_env(monkeypatch, fake_find_gaps)
    monkeypatch.setattr(server, "_is_paused", lambda: True)          # read-only: runs while paused
    r = server.gaps_view(lists=False, refresh=False, wait=True)     # GET /gaps?wait=true
    assert set(seen) == {"roles", "statuses", "liveness", "liveness_key", "not_in_netbox_stale_hours"}
    assert {k: r[k] for k in GAP_COUNTS} == GAP_COUNTS
    assert r["last_run"]["status"] == "ok" and r["running"] is False and r["refresh_started"] is False
    # lists come from the saved gap files
    assert server.gaps_view(lists=True, refresh=False, wait=False)["not_in_netdisco_list"] == [{"ip": "192.0.2.1"}]


def test_gaps_view_answers_immediately_and_refreshes_in_background(monkeypatch) -> None:
    import threading
    gate, calls = threading.Event(), []

    def slow_find_gaps(nd, nb, **kw):
        calls.append(kw)
        gate.wait(5)
        return {**GAP_COUNTS, "not_in_netdisco_list": [], "not_in_netbox_list": [], "tag_mismatches_list": []}

    jobs = _gaps_env(monkeypatch, slow_find_gaps)
    r = server.gaps_view(lists=False, refresh=False, wait=False)   # no result yet → starts one
    assert r["refresh_started"] is True and r["running"] is True and r["finished"] is None
    r2 = server.gaps_view(lists=False, refresh=True, wait=False)   # already running → not doubled
    assert r2["refresh_started"] is False and r2["running"] is True
    gate.set()
    jobs.wait("gaps")
    r3 = server.gaps_view(lists=False, refresh=False, wait=False)
    assert len(calls) == 1
    assert r3["netbox_total"] == 10 and r3["finished"] and r3["running"] is False and r3["refresh_started"] is False


def test_gaps_view_records_errors(monkeypatch) -> None:
    def boom(nd, nb, **kw):
        raise ConnectionError("Remote end closed connection without response")

    _gaps_env(monkeypatch, boom)
    with pytest.raises(HTTPException) as exc:
        server.gaps_view(lists=False, refresh=False, wait=True)
    assert exc.value.status_code == 502
    r = server.gaps_view(lists=False, refresh=False, wait=False)
    assert "Remote end closed" in r["last_run"]["error"] and r["refresh_started"] is False and r["finished"] is None


def test_discover_missing_refuses_a_missing_or_old_gaps_result(monkeypatch) -> None:
    jobs = _gaps_env(monkeypatch, lambda nd, nb, **kw: {**GAP_COUNTS, "not_in_netdisco_list": [],
                                                        "not_in_netbox_list": [], "tag_mismatches_list": []})
    monkeypatch.setattr(server, "_is_paused", lambda: False)
    monkeypatch.setattr(server, "_inflight_hosts", lambda: [])
    called = []
    monkeypatch.setattr(server, "discover_missing", lambda *a, **kw: called.append(a) or
                        {"aborted": False, "enqueued": 0, "skipped": 0, "skipped_offline": 0})
    rec = jobs.run("discover-missing")
    assert rec["status"] == "skipped" and "no gaps result" in rec["summary"] and not called
    jobs.run("gaps")
    monkeypatch.setattr(server, "_GAPS_MAX_AGE", 3600)
    assert jobs.run("discover-missing")["status"] == "ok" and len(called) == 1
    monkeypatch.setattr(server.time, "time", lambda: jobs.last_success("gaps") + 7200)
    rec = jobs.run("discover-missing")
    assert rec["status"] == "skipped" and "120m old (max 60m)" in rec["summary"] and len(called) == 1


# ── /discover ──

class _NDJobs:
    def __init__(self):
        self.jobs: list = []

    def enqueue_discover(self, ip, device_auth_tag_hint=None, snmp_timeout_us=None):
        self.jobs.append((ip, device_auth_tag_hint, snmp_timeout_us))


def _discover_env(monkeypatch, devices=()):
    nd = _NDJobs()
    lookups: list = []

    class _NB:
        def __init__(self):
            self.nb = SimpleNamespace(dcim=SimpleNamespace(devices=SimpleNamespace(filter=self._filter)))

        def _filter(self, name__ie=None, name__isw=None):
            lookups.append(name__ie or name__isw)
            if name__ie:
                return [d for d in devices if d.name.lower() == name__ie.lower()]
            return [d for d in devices if d.name.lower().startswith(name__isw.lower())]

        def find_device_by_ip(self, ip):
            lookups.append(ip)
            return next((d for d in devices if str(d.primary_ip4).split("/")[0] == ip), None)

    monkeypatch.setattr(server, "_get_netdisco_client", lambda: nd)
    monkeypatch.setattr(server, "_get_netbox_client", lambda: _NB())
    return nd, lookups


def _dev(name, ip, tag=None, timeout=None):
    return SimpleNamespace(name=name, primary_ip4=f"{ip}/24" if ip else None,
                           custom_fields={"snmp_auth_profile": tag, "snmp_polling_timeout": timeout})


def test_discover_with_params_needs_no_lookup(monkeypatch) -> None:
    nd, lookups = _discover_env(monkeypatch)
    r = server.discover(host="192.0.2.1", tag="v3", timeout="3m")
    assert nd.jobs == [("192.0.2.1", "v3", 180_000_000)] and lookups == []
    assert r["tag_source"] == r["timeout_source"] == "param"


def test_discover_takes_profile_and_timeout_from_netbox(monkeypatch) -> None:
    nd, _ = _discover_env(monkeypatch, [_dev("wlc1.example.com", "192.0.2.8", tag="wlc-v3", timeout="2m")])
    r = server.discover(host="192.0.2.8", tag=None, timeout=None)
    # the CF is a whole-device budget: "2m" → 10s per request (bounded)
    assert nd.jobs == [("192.0.2.8", "wlc-v3", 10_000_000)] and r["device"] == "wlc1.example.com"
    assert r["tag_source"] == r["timeout_source"] == "netbox"
    # by (short) name: the device's primary IP is discovered
    r = server.discover(host="WLC1", tag=None, timeout=None)
    assert nd.jobs[-1] == ("192.0.2.8", "wlc-v3", 10_000_000) and r["host"] == "192.0.2.8"


def test_discover_unknown_ip_default_and_errors(monkeypatch) -> None:
    nd, _ = _discover_env(monkeypatch)
    r = server.discover(host="192.0.2.99", tag=None, timeout=None)
    assert nd.jobs == [("192.0.2.99", None, 10_000_000)] and r["timeout_us"] == 10_000_000
    assert r["tag_source"] == r["timeout_source"] == "default"
    for kw, code in (({"host": "no-such-device", "tag": None, "timeout": None}, 404),
                     ({"host": "192.0.2.1", "tag": "x", "timeout": "soon"}, 400)):
        with pytest.raises(HTTPException) as exc:
            server.discover(**kw)
        assert exc.value.status_code == code


def test_types_library_unknown_role_is_400(monkeypatch) -> None:
    def boom(*a, **kw):
        raise ValueError("Unknown device role(s): 4 (valid slugs: switch)")
    monkeypatch.setattr(server, "sync_types", boom)
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_get_library", lambda: object())
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=["4"], type=None, apply=False)
    assert exc.value.status_code == 400 and "Unknown device role" in exc.value.detail



class _TypeEP:
    def __init__(self, recs):
        self.recs, self.calls = recs, []

    def filter(self, **kw):
        self.calls.append(kw)
        return [r for r in self.recs if r.custom_fields.get("source") == kw.get("cf_source")]


def test_types_bare_metric_from_netbox(monkeypatch) -> None:
    mfr = SimpleNamespace(name="Fortinet")
    dts = [SimpleNamespace(model="FGT-100X", part_number="FGT-100X", manufacturer=mfr, device_count=3,
                           custom_fields={"source": "netdisco"}),
           SimpleNamespace(model="FGT-60F", part_number="FGT60F", manufacturer=mfr, device_count=9,
                           custom_fields={"source": "manual"})]
    mts = [SimpleNamespace(model="FAKE-NM", part_number="", manufacturer=mfr, module_count=2,
                           custom_fields={"source": "netdisco"})]
    nb = SimpleNamespace(cf_on=lambda name, ot: True,
                         nb=SimpleNamespace(dcim=SimpleNamespace(device_types=_TypeEP(dts), module_types=_TypeEP(mts))))
    monkeypatch.setattr(server, "_get_netbox_client", lambda: nb)
    monkeypatch.setattr(server, "_IFACE_SOURCE_CF", "source")
    monkeypatch.setattr(server, "_IFACE_SOURCE_VALUE", "netdisco")
    monkeypatch.setattr(server, "_library_has", lambda m, model, pn="": "none")
    assert server._refresh_types_bare() == 2

    def val(kind, model, pn, lib):
        return server._custom_registry.get_sample_value("discobox_types_bare", {
            "kind": kind, "manufacturer": "Fortinet", "model": model, "part_number": pn, "library": lib})
    assert val("device", "FGT-100X", "FGT-100X", "none") == 3
    assert val("module", "FAKE-NM", "", "unknown") == 2
    assert val("device", "FGT-60F", "FGT60F", "none") is None           # manual: not bare
    dts[0].custom_fields["source"] = "manual"                            # fixed by hand → drops out
    assert server._refresh_types_bare() == 1 and val("device", "FGT-100X", "FGT-100X", "none") is None


def test_types_library_needs_a_selection_bare_counts(calls) -> None:
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=None, type=None, bare=False, apply=False)
    assert exc.value.status_code == 400
    server.types_library(GET, role=None, type=None, bare=True, apply=False)
    assert calls[-1] == {"roles": [], "types": [], "apply": False, "bare": True}   # no default roles pulled in


def test_ap_and_partial_errors_reach_the_metrics(monkeypatch) -> None:
    def fake_sync(**kw):
        return {"ok": True, "errors": 2, "hostname": "wlc-test-1", "interfaces": {"unchanged": 3},
                "aps": {"updated": 1, "unchanged": 5, "not_found": 0, "error": 2, "skipped": 0}}
    monkeypatch.setattr(server, "sync_device", fake_sync)
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: object())
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_is_paused", lambda: False)
    monkeypatch.setattr(server, "_release_host", lambda h: None)
    monkeypatch.setattr(server, "_mark_synced", lambda h: None)
    reg = server._custom_registry

    def get(name, labels=None):
        return reg.get_sample_value(name, labels or {}) or 0
    before = (get("discobox_aps_total", {"action": "error"}), get("discobox_sync_errors_total"))
    server.sync_in_progress.inc()           # _run_sync decrements it
    server._run_sync("192.0.2.250", True, True, True, True, True, False)
    assert get("discobox_aps_total", {"action": "error"}) - before[0] == 2
    assert get("discobox_sync_errors_total") - before[1] == 2
    assert get("discobox_device_last_sync_errors", {"instance": "wlc-test-1"}) == 2
    assert get("discobox_device_last_sync_failed", {"instance": "wlc-test-1"}) == 0   # still a success
