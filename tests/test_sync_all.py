"""Unit tests for the /sync/all enqueue loop (_enqueue_all).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_sync_all.py
"""
from __future__ import annotations

import os
import sys
import tempfile

_TMPDIR = tempfile.mkdtemp(prefix="discobox-test-")
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", _TMPDIR)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402


def _reset() -> None:
    server._in_flight.clear()
    for f in os.scandir(server._INFLIGHT_DIR):
        if f.name.startswith(("discobox.inflight.", "discobox.synced.")):
            os.unlink(f.path)
    server._MAX_QUEUE = 10_000
    server._SYNC_COOLDOWN = 3600


def _run(hosts, **kwargs):
    submitted: list[str] = []
    counts = server._enqueue_all(hosts, submitted.append, **kwargs)
    return submitted, counts


def test_enqueue_all_queues_valid_hosts() -> None:
    _reset()
    submitted, counts = _run(["10.0.0.1", "10.0.0.2", "not-an-ip"])
    assert submitted == ["10.0.0.1", "10.0.0.2"]
    assert counts["queued"] == 2 and counts["invalid"] == 1


def test_enqueue_all_respects_limit() -> None:
    _reset()
    submitted, counts = _run([f"10.0.0.{i}" for i in range(1, 10)], limit=3)
    assert len(submitted) == 3
    assert counts["queued"] == 3


def test_enqueue_all_cooldown_and_force() -> None:
    _reset()
    server._mark_synced("10.0.0.1")
    submitted, counts = _run(["10.0.0.1", "10.0.0.2"])
    assert submitted == ["10.0.0.2"]
    assert counts["cooldown"] == 1

    _reset()
    server._mark_synced("10.0.0.1")
    submitted, counts = _run(["10.0.0.1", "10.0.0.2"], force=True)
    assert submitted == ["10.0.0.1", "10.0.0.2"]
    assert counts["cooldown"] == 0


def test_enqueue_all_skips_in_flight_and_full_queue() -> None:
    _reset()
    assert server._claim_host("10.0.0.1")  # someone else is already syncing it
    submitted, counts = _run(["10.0.0.1", "10.0.0.2"])
    assert submitted == ["10.0.0.2"]
    assert counts["in_progress"] == 1

    _reset()
    server._MAX_QUEUE = 1
    submitted, counts = _run(["10.0.0.1", "10.0.0.2", "10.0.0.3"])
    assert submitted == ["10.0.0.1"]
    assert counts["queue_full"] == 2


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
