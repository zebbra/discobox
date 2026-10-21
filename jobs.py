"""
Periodic and on-demand jobs (reconcile, compare, fix-tags, types-bare): one
registry that runs them, keeps each job's last result in a state file and
exports the same metrics for all of them.

A job is a plain function returning a JobResult (or raising: status error).
The scheduler runs a job with an interval when it's due; any job also runs on
demand (start/run). The state survives restarts, so the next run follows the
last one instead of the process start, and an overdue job runs shortly after
startup.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge

log = logging.getLogger("discobox.jobs")


@dataclass
class JobResult:
    status: str = "ok"                 # ok | deferred | skipped | aborted (error: the job raised)
    summary: str = ""
    retry_in: Optional[float] = None   # next try after this many seconds instead of the interval
    data: dict = field(default_factory=dict)


@dataclass
class Job:
    name: str
    func: Callable[..., JobResult]
    interval: float = 0                # seconds; 0 = on demand only
    description: str = ""
    startup_delay: float = 30          # earliest scheduled run after the process starts


class JobRegistry:
    def __init__(self, state_path: str, registry: Optional[CollectorRegistry] = None,
                 clock: Callable[[], float] = time.time):
        self.state_path = state_path
        self.clock = clock
        self.started_at = clock()
        self._jobs: dict[str, Job] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._running: dict[str, float] = {}     # name → start time of the run in progress
        self._state: dict[str, dict] = {}
        self._state_lock = threading.Lock()
        reg = {"registry": registry} if registry is not None else {}
        self.m_last_run = Gauge(
            "discobox_job_last_run_timestamp_seconds", "Unix time the job's last run finished", ["job"], **reg)
        self.m_last_success = Gauge(
            "discobox_job_last_success_timestamp_seconds", "Unix time of the job's last run with status ok",
            ["job"], **reg)
        self.m_duration = Gauge(
            "discobox_job_duration_seconds", "Duration of the job's last run", ["job"], **reg)
        self.m_next_run = Gauge(
            "discobox_job_next_run_timestamp_seconds", "Unix time of the job's next scheduled run (0: on demand only)",
            ["job"], **reg)
        self.m_runs = Counter(
            "discobox_job_runs_total", "Job runs by status: ok | deferred | skipped | aborted | error",
            ["job", "status"], **reg)
        self._load()

    # ── registry ──

    def register(self, job: Job) -> None:
        self._jobs[job.name] = job
        self._locks[job.name] = threading.Lock()
        last = self._state.get(job.name, {})
        if last.get("finished"):
            self.m_last_run.labels(job.name).set(last["finished"])
            self.m_duration.labels(job.name).set(last.get("duration_s") or 0)
        if last.get("last_success"):
            self.m_last_success.labels(job.name).set(last["last_success"])
        self.m_next_run.labels(job.name).set(self.next_run(job.name) or 0)

    def names(self) -> list[str]:
        return list(self._jobs)

    def __contains__(self, name: str) -> bool:
        return name in self._jobs

    # ── state ──

    def _load(self) -> None:
        try:
            with open(self.state_path) as f:
                data = json.load(f)
            self._state = data if isinstance(data, dict) else {}
        except FileNotFoundError:
            self._state = {}
        except (OSError, ValueError) as exc:
            log.warning("Job state %s unreadable, starting empty: %s", self.state_path, exc)
            self._state = {}

    def _save(self) -> None:
        tmp = f"{self.state_path}.tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self._state, f)
            os.replace(tmp, self.state_path)
        except OSError as exc:
            log.warning("Could not save job state: %s", exc)

    def last(self, name: str) -> dict:
        with self._state_lock:
            return dict(self._state.get(name, {}))

    def last_success(self, name: str) -> Optional[float]:
        return self.last(name).get("last_success")

    def is_running(self, name: str) -> bool:
        return self._locks[name].locked()

    def next_run(self, name: str) -> Optional[float]:
        """When the scheduler runs the job next; None: on demand only."""
        job = self._jobs[name]
        if job.interval <= 0:
            return None
        last = self.last(name)
        if last.get("retry_at"):
            due = last["retry_at"]
        elif last.get("finished"):
            due = last["finished"] + job.interval
        else:
            due = 0.0
        return max(due, self.started_at + job.startup_delay)

    def snapshot(self) -> list[dict]:
        out = []
        for name, job in self._jobs.items():
            last = self.last(name)
            out.append({
                "job": name,
                "description": job.description,
                "interval_s": job.interval or None,
                "running": self.is_running(name),
                "running_since": self._running.get(name),
                "last": {k: last.get(k) for k in ("started", "finished", "duration_s", "status", "summary", "error")}
                if last.get("finished") else None,
                "last_success": last.get("last_success"),
                "next_run": self.next_run(name),
            })
        return out

    # ── running ──

    def run(self, name: str, wait_if_running: bool = False, **kwargs: Any) -> Optional[dict]:
        """
        Run the job now in this thread; returns its recorded result. None when
        it's already running (with wait_if_running: once that run is done,
        whose result is then current).
        """
        lock = self._locks[name]
        if not lock.acquire(blocking=False):
            if wait_if_running:
                with lock:
                    pass
            return None
        try:
            return self._run_locked(self._jobs[name], kwargs)
        finally:
            lock.release()

    def start(self, name: str, **kwargs: Any) -> bool:
        """Run the job in a background thread; False when it's already running."""
        lock = self._locks[name]
        if not lock.acquire(blocking=False):    # taken here, so a second start can't slip in
            return False

        def _bg() -> None:
            try:
                self._run_locked(self._jobs[name], kwargs)
            finally:
                lock.release()

        threading.Thread(target=_bg, name=f"job-{name}", daemon=True).start()
        return True

    def wait(self, name: str) -> None:
        """Block until a run in progress (if any) is done."""
        with self._locks[name]:
            pass

    def _run_locked(self, job: Job, kwargs: dict) -> dict:
        started = self.clock()
        self._running[job.name] = started
        error = None
        try:
            result = job.func(**kwargs) or JobResult()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            log.error("Job %s failed: %s", job.name, error)
            result = JobResult(status="error", summary=error)
        finally:
            self._running.pop(job.name, None)
        finished = self.clock()
        with self._state_lock:
            prev = self._state.get(job.name, {})
            rec = {
                "started": started, "finished": finished, "duration_s": round(finished - started, 1),
                "status": result.status, "summary": result.summary, "error": error,
                "last_success": finished if result.status == "ok" else prev.get("last_success"),
                "retry_at": finished + result.retry_in if result.retry_in else None,
            }
            self._state[job.name] = rec
            self._save()
        self.m_last_run.labels(job.name).set(finished)
        self.m_duration.labels(job.name).set(finished - started)
        if result.status == "ok":
            self.m_last_success.labels(job.name).set(finished)
        self.m_runs.labels(job.name, result.status).inc()
        self.m_next_run.labels(job.name).set(self.next_run(job.name) or 0)
        log.info("Job %s: %s%s (%.1fs)", job.name, result.status,
                 f": {result.summary}" if result.summary else "", finished - started)
        return {**rec, "data": result.data}

    async def scheduler(self, tick: float = 5) -> None:
        """Start every job whose next run is due; runs until cancelled."""
        while True:
            now = self.clock()
            for name in self._jobs:
                due = self.next_run(name)
                if due is not None and due <= now:
                    self.start(name)
            await asyncio.sleep(tick)
