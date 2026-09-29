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



def test_logged_errors_count_per_sync_thread(monkeypatch) -> None:
    import logging
    import threading

    import discobox

    def fake(*a, _held_locks=None, **kw):
        log = discobox._DeviceLogAdapter(discobox.logger, {"ip": "192.0.2.1"})
        log.error("note update error")                           # only logged, not counted anywhere
        logging.getLogger("discobox.sync").error("child logger")  # propagates to "discobox"
        log.warning("not an error")
        return {"ok": True, "seen": discobox._sync_error_tls.count}
    monkeypatch.setattr(discobox, "_sync_device", fake)
    assert discobox.sync_device()["seen"] == 2
    assert discobox._sync_error_tls.count is None             # reset afterwards
    discobox.logger.error("outside a sync: not counted, no crash")

    other: list = []
    t = threading.Thread(target=lambda: other.append(getattr(discobox._sync_error_tls, "count", None)))
    t.start()
    t.join()
    assert other == [None]                                     # per thread


def test_reads_retry_on_5xx_but_not_504_or_writes() -> None:
    import discobox
    retry = discobox._ChangelogSession().get_adapter("https://netbox.example/api/").max_retries
    assert retry.is_retry("GET", 500) and retry.is_retry("GET", 503)
    assert not retry.is_retry("GET", 504) and not retry.is_retry("PATCH", 500) and not retry.is_retry("POST", 503)


def test_read_retry_pauses_grow_from_the_first_retry(monkeypatch) -> None:
    import discobox
    monkeypatch.setattr(discobox.random, "uniform", lambda a, b: 0.0)
    retry = discobox._ChangelogSession().get_adapter("https://netbox.example/api/").max_retries
    assert retry.get_backoff_time() == 0.0
    pauses = []
    for _ in range(3):
        retry = retry.increment(method="GET", url="/x", error=discobox.urllib3.exceptions.ProtocolError("reset"))
        pauses.append(retry.get_backoff_time())
    assert pauses == [2.0, 4.0, 8.0] and isinstance(retry, discobox._ReadRetry)
