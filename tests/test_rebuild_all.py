"""Unit tests for the bulk rebuild: the delete brake, the worker and the
/rebuild/all endpoints.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_rebuild_all.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

_TMPDIR = tempfile.mkdtemp(prefix="discobox-test-")
os.environ.setdefault("DISCOBOX_STATE_DIR", _TMPDIR)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402
from discobox.sync import prune_brake  # noqa: E402

# ── brake ──────────────────────────────────────────────────────────────────────


def test_brake_trips_on_the_share_of_one_kind() -> None:
    stale = {"interfaces_deleted": 2, "modules_deleted": 8, "inventory_deleted": 0, "sfps_deleted": 0}
    current = {"interfaces_deleted": 48, "modules_deleted": 2, "inventory_deleted": 4, "sfps_deleted": 10}
    assert prune_brake(stale, current, 50) == {"kind": "modules", "stale": 8, "total": 10, "percent": 80}


def test_brake_trips_when_netdisco_reports_none_of_a_kind() -> None:
    blocked = prune_brake({"sfps_deleted": 24}, {"sfps_deleted": 0}, 50)
    assert blocked["kind"] == "sfps" and blocked["percent"] == 100


def test_brake_lets_small_and_minor_deletions_through() -> None:
    # all of them, but only a handful
    assert prune_brake({"sfps_deleted": 5}, {"sfps_deleted": 0}, 50) is None
    # many, but a small share
    assert prune_brake({"interfaces_deleted": 20}, {"interfaces_deleted": 80}, 50) is None
    # exactly at the limit
    assert prune_brake({"interfaces_deleted": 10}, {"interfaces_deleted": 10}, 50) is None


# ── worker ─────────────────────────────────────────────────────────────────────

def _entry(host: str) -> dict:
    return {"host": host, "hostname": host, "device_id": 1, "status": "queued", "reason": None,
            "duration_s": None, "prune": {}, "new_stack_members": None}


def _run(hosts, dry_run=True) -> dict:
    return {"status": "running", "dry_run": dry_run, "started": "t", "finished": None,
            "cancel": False, "devices": [_entry(h) for h in hosts]}


@pytest.fixture
def quiet(monkeypatch):
    server._in_flight.clear()
    server._rebuild_run.clear()
    monkeypatch.setattr(server, "_is_paused", lambda: False)
    monkeypatch.setattr(server, "_REBUILD_MAX_DEVICES", 20)
    yield
    server._in_flight.clear()
    server._rebuild_run.clear()


def test_worker_rebuilds_in_order_and_records_each_result(monkeypatch, quiet) -> None:
    seen = []

    def fake(host, dry_run, max_percent):
        seen.append((host, dry_run, max_percent, host in server._in_flight))
        if host == "192.0.2.2":
            return {"ok": True, "hostname": "sw2", "prune": {
                "sfps_deleted": 24, "blocked": {"kind": "sfps", "stale": 24, "total": 24, "percent": 100}}}
        if host == "192.0.2.3":
            raise RuntimeError("netbox down")
        return {"ok": True, "hostname": "sw1", "prune": {"interfaces_deleted": 3, "stack_members": {"was": 2, "now": 3}}}

    monkeypatch.setattr(server, "_rebuild_device", fake)
    monkeypatch.setattr(server, "_REBUILD_MAX_DELETE_PERCENT", 50)
    run = _run(["192.0.2.1", "192.0.2.2", "192.0.2.3"], dry_run=False)
    server._rebuild_all_worker(run)

    assert seen == [("192.0.2.1", False, 50, True), ("192.0.2.2", False, 50, True), ("192.0.2.3", False, 50, True)]
    d1, d2, d3 = run["devices"]
    assert (d1["status"], d1["hostname"], d1["new_stack_members"]) == ("ok", "sw1", 3)
    assert d1["prune"]["interfaces_deleted"] == 3 and d1["duration_s"] is not None
    assert d2["status"] == "blocked" and "sfps 24/24" in d2["reason"]
    assert (d3["status"], d3["reason"]) == ("error", "netbox down")
    assert run["status"] == "done" and run["finished"]
    assert not server._in_flight


def test_cancel_drops_the_devices_not_started(monkeypatch, quiet) -> None:
    run = _run(["192.0.2.1", "192.0.2.2", "192.0.2.3"])

    def fake(host, dry_run, max_percent):
        run["cancel"] = True   # arrives while the first device is being rebuilt
        return {"ok": True, "hostname": host, "prune": {}}

    monkeypatch.setattr(server, "_rebuild_device", fake)
    server._rebuild_all_worker(run)
    assert [d["status"] for d in run["devices"]] == ["ok", "cancelled", "cancelled"]
    assert run["status"] == "cancelled"


def test_worker_waits_for_a_sync_of_the_same_host_then_skips(monkeypatch, quiet) -> None:
    server._in_flight.add("192.0.2.1")
    monkeypatch.setattr(server, "_REBUILD_CLAIM_WAIT_S", 10)
    monkeypatch.setattr(server, "_rebuild_device", lambda *a: {"ok": True, "hostname": "x", "prune": {}})
    naps = []
    run = _run(["192.0.2.1", "192.0.2.2"])
    server._rebuild_all_worker(run, sleep=naps.append)
    assert [d["status"] for d in run["devices"]] == ["skipped", "ok"]
    assert run["devices"][0]["reason"] == "already in progress" and len(naps) == 2
    assert server._in_flight == {"192.0.2.1"}   # the other sync's claim is untouched


def test_snapshot_totals_and_average(quiet) -> None:
    assert server._rebuild_run_snapshot()["status"] == "none"
    run = _run(["192.0.2.1", "192.0.2.2", "192.0.2.3"])
    run["devices"][0].update(status="ok", duration_s=100.0)
    run["devices"][1].update(status="blocked", duration_s=200.0)
    run["devices"][2].update(status="running")
    server._rebuild_run.update(run)
    snap = server._rebuild_run_snapshot()
    assert (snap["total"], snap["done"], snap["remaining"], snap["running"]) == (3, 2, 1, 1)
    assert (snap["ok"], snap["blocked"], snap["avg_duration_s"]) == (1, 1, 150.0)
    assert "cancel" not in snap and len(snap["devices"]) == 3


# ── endpoint ───────────────────────────────────────────────────────────────────

def _request(query: list) -> SimpleNamespace:
    return SimpleNamespace(query_params=SimpleNamespace(multi_items=lambda: query))


def _wire(monkeypatch, nb_devices, nd_ips) -> list:
    filters: list = []

    def nb_filter(**kw):
        filters.append(kw)
        return nb_devices

    nb = SimpleNamespace(nb=SimpleNamespace(dcim=SimpleNamespace(devices=SimpleNamespace(filter=nb_filter))))
    nd = SimpleNamespace(get_all_devices=lambda: [{"ip": ip} for ip in nd_ips])
    monkeypatch.setattr(server, "_get_netbox_client", lambda: nb)
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: nd)
    monkeypatch.setattr(server, "_start_rebuild_worker", lambda run: None)
    return filters


def _dev(i: int) -> SimpleNamespace:
    return SimpleNamespace(id=i, name=f"sw{i}", primary_ip4=f"192.0.2.{i}/24")


def test_post_caps_the_run_and_counts_the_rest(monkeypatch, quiet) -> None:
    monkeypatch.setattr(server, "_REBUILD_MAX_DEVICES", 3)
    filters = _wire(monkeypatch, [_dev(i) for i in range(1, 7)], [f"192.0.2.{i}" for i in range(1, 6)])
    out = asyncio.run(server.rebuild_all(_request([("id", "1"), ("id", "2"), ("dry_run", "true")]), True, None))

    assert filters == [{"has_primary_ip": True, "id": ["1", "2"]}]
    assert out["status"] == "running" and out["dry_run"] is True
    assert (out["selected"], out["queued"], out["over_limit"], out["not_in_netdisco"]) == (6, 3, 2, 1)
    snap = server._rebuild_run_snapshot()
    assert [(d["host"], d["hostname"], d["device_id"], d["status"]) for d in snap["devices"]] == [
        ("192.0.2.1", "sw1", 1, "queued"), ("192.0.2.2", "sw2", 2, "queued"), ("192.0.2.3", "sw3", 3, "queued")]

    # a second run while this one is running
    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.rebuild_all(_request([("id", "1")]), True, None))
    assert exc.value.status_code == 409

    # cancel marks the run; the worker (not started here) does the rest
    asyncio.run(server.rebuild_all_cancel())
    assert server._rebuild_run["cancel"] is True


def test_post_limit_only_lowers_the_cap(monkeypatch, quiet) -> None:
    monkeypatch.setattr(server, "_REBUILD_MAX_DEVICES", 3)
    ips = [f"192.0.2.{i}" for i in range(1, 7)]
    _wire(monkeypatch, [_dev(i) for i in range(1, 7)], ips)
    assert asyncio.run(server.rebuild_all(_request([("site", "zrh")]), True, 2))["queued"] == 2
    server._rebuild_run.clear()
    assert asyncio.run(server.rebuild_all(_request([("site", "zrh")]), True, 10))["queued"] == 3


def test_post_needs_a_filter_and_refuses_while_paused(monkeypatch, quiet) -> None:
    _wire(monkeypatch, [], [])
    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.rebuild_all(_request([("dry_run", "false")]), False, None))
    assert exc.value.status_code == 400
    monkeypatch.setattr(server, "_is_paused", lambda: True)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.rebuild_all(_request([("id", "1")]), True, None))
    assert exc.value.status_code == 409


def test_post_with_nothing_to_rebuild_starts_no_run(monkeypatch, quiet) -> None:
    _wire(monkeypatch, [_dev(1)], [])
    out = asyncio.run(server.rebuild_all(_request([("id", "1")]), True, None))
    assert out["status"] == "none" and out["not_in_netdisco"] == 1
    assert server._rebuild_run_snapshot()["status"] == "none"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
