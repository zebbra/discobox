"""Deadlock-retrying deletes and per-Netbox-device serialisation of syncs."""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import discobox  # noqa: E402
from discobox import _delete_with_retry, _device_lock  # noqa: E402


class _Obj:
    def __init__(self, fail_times: int, msg: str = "deadlock detected\nDETAIL: ..."):
        self.fail_times, self.msg, self.calls = fail_times, msg, 0

    def delete(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(self.msg)
        return True


def test_deadlock_is_retried_then_succeeds(monkeypatch) -> None:
    monkeypatch.setattr(discobox, "DEADLOCK_RETRIES", (0, 0, 0))
    o = _Obj(fail_times=2)
    assert _delete_with_retry(o, "x") is True and o.calls == 3


def test_persistent_deadlock_or_other_error_returns_false(monkeypatch) -> None:
    monkeypatch.setattr(discobox, "DEADLOCK_RETRIES", (0, 0, 0))
    o = _Obj(fail_times=99)
    assert _delete_with_retry(o, "x") is False and o.calls == 4          # 1 + 3 retries
    other = _Obj(fail_times=1, msg="400 Bad Request")
    assert _delete_with_retry(other, "x") is False and other.calls == 1  # not retried


def test_device_lock_serialises_same_device_only() -> None:
    assert _device_lock(4711) is _device_lock(4711)
    assert _device_lock(4711) is not _device_lock(4712)
    order: list = []

    def worker(tag):
        with _device_lock(99):
            order.append(f"{tag}-in")
            time.sleep(0.05)
            order.append(f"{tag}-out")

    ts = [threading.Thread(target=worker, args=(t,)) for t in ("a", "b")]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    # never interleaved: each "in" is directly followed by its own "out"
    assert order[0][0] == order[1][0] and order[2][0] == order[3][0]


def test_sync_device_releases_its_lock_on_error(monkeypatch) -> None:
    lock = _device_lock(5)

    def boom(*a, _held_locks=None, **kw):
        lock.acquire()
        _held_locks.append(lock)
        raise RuntimeError("sync blew up")

    monkeypatch.setattr(discobox, "_sync_device", boom)
    try:
        discobox.sync_device("192.0.2.1", None, None)
    except RuntimeError:
        pass
    assert lock.acquire(blocking=False)        # released by the wrapper
    lock.release()

