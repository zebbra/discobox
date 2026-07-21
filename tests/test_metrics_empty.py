"""Regression test: /metrics must never silently return an empty 200.

generate_latest() returns b"" when the multiprocess collector finds zero
*.db files in PROMETHEUS_MULTIPROC_DIR — structurally this shouldn't
happen (every metric gets a backing file at import time), but it has been
observed in production after a container restart. Whatever the root
cause, this state must be visibly a scrape failure (503 + logged
diagnostics), not something Prometheus records as "0 samples, success".

Run with `pytest tests/` or directly:
    python -m pytest tests/test_metrics_empty.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import tempfile

# prometheus_client.values.ValueClass (and the file-handle cache in its
# MultiProcessValue closure) is computed once at prometheus_client's own
# import time and keyed by metric-type only, not by directory — it is a
# true process-wide singleton no amount of re-importing server.py escapes.
# So this must share the one PROMETHEUS_MULTIPROC_DIR every other test file
# in this suite uses (same plain `import server` as test_server_retry.py /
# test_sync_all.py) rather than trying to isolate its own.
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp(prefix="discobox-test-"))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402


def test_metrics_ok_when_db_files_present() -> None:
    # Sanity check: the normal case (files exist, from module import) returns
    # real content with a 200.
    resp = asyncio.run(server.metrics())
    assert resp.status_code == 200
    assert len(resp.body) > 0


def test_metrics_returns_503_when_multiproc_dir_is_empty(caplog) -> None:
    # Simulate the observed failure: every *.db file is gone (whatever the
    # cause), directory still exists. Runs after the ok-case test above so it
    # doesn't stop other tests in this file/session from finding real data.
    for f in os.scandir(server._MULTIPROC_DIR):
        if f.name.endswith(".db"):
            os.unlink(f.path)

    with caplog.at_level(logging.ERROR, logger="discobox.server"):
        resp = asyncio.run(server.metrics())

    assert resp.status_code == 503
    assert resp.body  # never a silent empty body — the error message is real content
    assert any("empty scrape" in r.message for r in caplog.records)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
