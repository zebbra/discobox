"""Unit tests for the server retry scheduling (_on_timeout / _drain_retries).

Run with `pytest tests/` or directly:
    python tests/test_server_retry.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import time

# Isolate all file-based state (metrics, inflight claims, circuit breaker)
# in a throwaway dir before server.py is imported.
_TMPDIR = tempfile.mkdtemp(prefix="discobox-test-")
os.environ["PROMETHEUS_MULTIPROC_DIR"] = _TMPDIR

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402

NOW = 1_000_000.0
KWARGS = {"sync_mac": True, "sync_ip": False}


def _reset() -> None:
    """Clear all retry/in-flight state between tests."""
    server._retry_pending.clear()
    server._recent_timeouts.clear()
    server._in_flight.clear()
    for f in os.scandir(_TMPDIR):
        if f.name.startswith(("discobox.inflight.", "discobox.cb.")):
            os.unlink(f.path)
    # Keep the circuit breaker out of these tests
    server._CB_THRESHOLD = 10_000
    server._RETRY_DELAY = 480
    server._RETRY_MAX_DELAY = 1800
    server._RETRY_MAX = 6
    server._MAX_QUEUE = 10_000


# ── _on_timeout: backoff schedule ──────────────────────────────────────────────

def test_on_timeout_backoff_doubles_and_caps() -> None:
    _reset()
    want = [480, 960, 1800, 1800, 1800, 1800]  # 8m, 16m, then capped at 30m
    for retry_count, expected_delay in enumerate(want):
        server._retry_pending.clear()
        before = time.time()
        server._on_timeout("sw1", retry_count, dict(KWARGS))
        entry = server._retry_pending["sw1"]
        assert entry["retry_count"] == retry_count + 1
        got_delay = entry["retry_after"] - before
        assert abs(got_delay - expected_delay) < 5, (
            f"retry_count={retry_count}: delay {got_delay:.0f}s, want {expected_delay}s"
        )
        assert entry["sync_mac"] is True


def test_on_timeout_max_retries_drops() -> None:
    _reset()
    server._on_timeout("sw1", server._RETRY_MAX, dict(KWARGS))
    assert "sw1" not in server._retry_pending


def test_on_timeout_does_not_overwrite_pending() -> None:
    _reset()
    server._on_timeout("sw1", 0, dict(KWARGS))
    first = dict(server._retry_pending["sw1"])
    server._on_timeout("sw1", 3, dict(KWARGS))
    assert server._retry_pending["sw1"] == first


# ── _drain_retries ─────────────────────────────────────────────────────────────

def test_drain_submits_ready_entries() -> None:
    _reset()
    server._retry_pending["sw1"] = {"retry_count": 2, "retry_after": NOW - 1, **KWARGS}
    server._retry_pending["sw2"] = {"retry_count": 1, "retry_after": NOW + 999, **KWARGS}
    submitted = []
    server._drain_retries(NOW, lambda h, rc, e: submitted.append((h, rc, e)))
    assert submitted == [("sw1", 2, KWARGS)]
    assert "sw1" not in server._retry_pending
    assert "sw1" in server._in_flight
    # Not yet due: untouched
    assert server._retry_pending["sw2"]["retry_count"] == 1


def test_drain_defers_when_queue_full() -> None:
    _reset()
    server._MAX_QUEUE = 0
    server._retry_pending["sw1"] = {"retry_count": 3, "retry_after": NOW - 1, **KWARGS}
    submitted = []
    server._drain_retries(NOW, lambda h, rc, e: submitted.append((h, rc, e)))
    assert submitted == []
    # Re-scheduled, attempt NOT burned, sync kwargs preserved
    entry = server._retry_pending["sw1"]
    assert entry["retry_count"] == 3
    assert entry["retry_after"] == NOW + 60
    assert entry["sync_mac"] is True
    assert "sw1" not in server._in_flight
    # Next tick with a freed-up queue submits it
    server._MAX_QUEUE = 10_000
    server._drain_retries(NOW + 60, lambda h, rc, e: submitted.append((h, rc, e)))
    assert submitted == [("sw1", 3, KWARGS)]


def test_drain_drops_host_already_in_flight() -> None:
    _reset()
    assert server._claim_host("sw1")  # simulate a sync already running elsewhere
    server._retry_pending["sw1"] = {"retry_count": 1, "retry_after": NOW - 1, **KWARGS}
    submitted = []
    server._drain_retries(NOW, lambda h, rc, e: submitted.append((h, rc, e)))
    assert submitted == []
    assert "sw1" not in server._retry_pending  # dropped, not re-scheduled


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
