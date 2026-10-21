"""Unit tests for the reconcile leader election (_acquire_reconcile_leadership).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_reconcile_leadership.py
"""
from __future__ import annotations

import os
import sys
import tempfile

os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp(prefix="discobox-test-"))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402


def _write_leader(pid: str, tmp_path, monkeypatch) -> str:
    path = str(tmp_path / "discobox.reconcile-leader.pid")
    monkeypatch.setattr(server, "_RECONCILE_LEADER_FILE", path)
    if pid:
        with open(path, "w") as f:
            f.write(pid)
    return path


def test_acquires_when_no_leader(tmp_path, monkeypatch) -> None:
    path = _write_leader("", tmp_path, monkeypatch)
    assert server._acquire_reconcile_leadership()
    assert open(path).read() == str(os.getpid())


def test_own_pid_left_over_from_a_restart_is_ours(tmp_path, monkeypatch) -> None:
    # container restart: same PID, file kept on the state volume
    _write_leader(str(os.getpid()), tmp_path, monkeypatch)
    assert server._acquire_reconcile_leadership()


def test_other_live_pid_keeps_leadership(tmp_path, monkeypatch) -> None:
    _write_leader(str(os.getppid()), tmp_path, monkeypatch)
    assert not server._acquire_reconcile_leadership()


def test_dead_pid_is_reclaimed(tmp_path, monkeypatch) -> None:
    path = _write_leader("999999999", tmp_path, monkeypatch)
    assert server._acquire_reconcile_leadership()
    assert open(path).read() == str(os.getpid())
