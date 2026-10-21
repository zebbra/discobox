"""Unit tests for the job registry (jobs.py): scheduling, persisted state, metrics.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_jobs.py
"""
from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prometheus_client import CollectorRegistry  # noqa: E402

from jobs import Job, JobRegistry, JobResult  # noqa: E402


class Clock:
    def __init__(self, now: float = 1_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


def _registry(tmp_path: Path, clock: Clock) -> tuple[JobRegistry, CollectorRegistry]:
    reg = CollectorRegistry()
    return JobRegistry(str(tmp_path / "jobs.json"), registry=reg, clock=clock), reg


def test_first_run_after_startup_delay_then_interval(tmp_path) -> None:
    clock = Clock()
    jobs, _ = _registry(tmp_path, clock)
    jobs.register(Job("r", lambda: JobResult(summary="fine"), interval=3600, startup_delay=300))
    assert jobs.next_run("r") == clock.now + 300
    clock.now += 400
    rec = jobs.run("r")
    assert rec["status"] == "ok" and rec["summary"] == "fine"
    assert jobs.next_run("r") == clock.now + 3600


def test_on_demand_job_has_no_next_run(tmp_path) -> None:
    jobs, _ = _registry(tmp_path, Clock())
    jobs.register(Job("m", lambda: JobResult()))
    assert jobs.next_run("m") is None
    assert jobs.snapshot()[0]["next_run"] is None and jobs.snapshot()[0]["last"] is None


def test_deferred_retries_sooner_and_keeps_last_success(tmp_path) -> None:
    clock = Clock()
    jobs, _ = _registry(tmp_path, clock)
    outcome = [JobResult()]
    jobs.register(Job("r", lambda: outcome[0], interval=86400, startup_delay=0))
    jobs.run("r")
    ok_at = clock.now
    clock.now += 86400
    outcome[0] = JobResult("deferred", "3 in flight", retry_in=600)
    jobs.run("r")
    assert jobs.next_run("r") == clock.now + 600
    assert jobs.last_success("r") == ok_at and jobs.last("r")["status"] == "deferred"


def test_error_is_recorded_not_raised(tmp_path) -> None:
    jobs, reg = _registry(tmp_path, Clock())

    def boom() -> JobResult:
        raise ConnectionError("netdisco down")

    jobs.register(Job("r", boom, interval=60))
    rec = jobs.run("r")
    assert rec["status"] == "error" and rec["error"] == "ConnectionError: netdisco down"
    assert jobs.last_success("r") is None
    assert reg.get_sample_value("discobox_job_runs_total", {"job": "r", "status": "error"}) == 1


def test_state_survives_restart_and_overdue_runs_soon(tmp_path) -> None:
    clock = Clock()
    jobs, _ = _registry(tmp_path, clock)
    jobs.register(Job("r", lambda: JobResult(), interval=3600, startup_delay=30))
    jobs.run("r")
    finished = clock.now
    # restart 2h later: the run is overdue, so it's due right after the startup delay
    clock.now += 7200
    jobs2, reg2 = _registry(tmp_path, clock)
    jobs2.register(Job("r", lambda: JobResult(), interval=3600, startup_delay=30))
    assert jobs2.last_success("r") == finished
    assert jobs2.next_run("r") == clock.now + 30
    assert reg2.get_sample_value("discobox_job_last_success_timestamp_seconds", {"job": "r"}) == finished
    # restart 10 min after the run: due one interval after it, not after the restart
    clock.now = finished + 600
    jobs3, _ = _registry(tmp_path, clock)
    jobs3.register(Job("r", lambda: JobResult(), interval=3600, startup_delay=30))
    assert jobs3.next_run("r") == finished + 3600


def test_unreadable_state_starts_empty(tmp_path) -> None:
    (tmp_path / "jobs.json").write_text("{not json")
    jobs, _ = _registry(tmp_path, Clock())
    jobs.register(Job("r", lambda: JobResult(), interval=60))
    assert jobs.last("r") == {}


def test_start_runs_once_at_a_time(tmp_path) -> None:
    jobs, _ = _registry(tmp_path, Clock())
    gate, calls = threading.Event(), []

    def slow(**kw) -> JobResult:
        calls.append(kw)
        gate.wait(5)
        return JobResult()

    jobs.register(Job("r", slow))
    assert jobs.start("r", max_enqueue=5) is True
    assert jobs.start("r") is False
    assert jobs.run("r") is None
    gate.set()
    jobs.wait("r")
    assert calls == [{"max_enqueue": 5}]
    assert jobs.is_running("r") is False and jobs.last("r")["status"] == "ok"


# ── /jobs endpoints ──

def _server(monkeypatch, tmp_path):
    import tempfile
    os.environ.setdefault("DISCOBOX_STATE_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
    import server
    jobs = JobRegistry(str(tmp_path / "jobs.json"), registry=CollectorRegistry())
    monkeypatch.setattr(server, "_jobs", jobs)
    return server, jobs


def test_jobs_endpoints(monkeypatch, tmp_path) -> None:
    import pytest
    from fastapi import HTTPException, Response

    server, jobs = _server(monkeypatch, tmp_path)
    seen = []
    jobs.register(Job("reconcile", lambda **kw: seen.append(kw) or JobResult(summary="2 enqueued"), interval=60))
    jobs.register(Job("compare", lambda: JobResult()))

    r = server.jobs_run("reconcile", Response(), wait=True, max_enqueue=5, offset=None)
    assert r["status"] == "ok" and r["summary"] == "2 enqueued"
    assert seen == [{"max_enqueue": 5, "offset": None, "manual": True}]
    listed = server.jobs_list()
    assert [j["job"] for j in listed["jobs"]] == ["reconcile", "compare"]
    assert listed["jobs"][0]["last"]["summary"] == "2 enqueued" and listed["retries"]["pending"] >= 0
    with pytest.raises(HTTPException) as exc:
        server.jobs_run("nope", Response())
    assert exc.value.status_code == 404
    assert server.jobs_run("compare", Response()) == {"job": "compare", "status": "started"}
    jobs.wait("compare")


def test_get_on_action_is_deprecated_not_refused(monkeypatch, tmp_path) -> None:
    import asyncio

    from starlette.requests import Request
    from starlette.responses import Response as StarletteResponse

    server, _ = _server(monkeypatch, tmp_path)

    async def call_next(request):
        return StarletteResponse("ok")

    def req(method: str, path: str) -> Request:
        return Request({"type": "http", "method": method, "path": path, "headers": [], "query_string": b""})

    before = server.deprecated_requests_total.labels("/rebuild")._value.get()
    r = asyncio.run(server._deprecated_get(req("GET", "/rebuild"), call_next))
    assert r.headers["Deprecation"] == "true"
    assert server.deprecated_requests_total.labels("/rebuild")._value.get() == before + 1
    assert "Deprecation" not in asyncio.run(server._deprecated_get(req("POST", "/rebuild"), call_next)).headers
    assert "Deprecation" not in asyncio.run(server._deprecated_get(req("GET", "/jobs"), call_next)).headers
