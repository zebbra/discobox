"""A hook for a device in Netdisco but not in Netbox is its own outcome, not an error.

Run with `pytest tests/` or directly:
    python tests/test_sync_not_in_netbox.py
"""
from __future__ import annotations

import os
import sys
import tempfile

os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
os.environ.setdefault("DISCOBOX_STATE_DIR", tempfile.mkdtemp(prefix="discobox-state-"))

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server  # noqa: E402

def test_device_not_found_is_not_an_error(monkeypatch, tmp_path) -> None:
    host = "192.0.2.47"
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: None)
    monkeypatch.setattr(server, "_get_netbox_client", lambda: None)
    monkeypatch.setattr(server, "sync_device", lambda **kw: {
        "ok": False, "reason": "device_not_found", "hostname": "sw-test.example.net",
        "interfaces": {}, "ips": {}, "modules": {}, "sfps": {},
    })
    monkeypatch.setattr(server, "_UNKNOWN_DEVICES_FILE", str(tmp_path / "unknown.json"))
    monkeypatch.setattr(server, "_PAUSE_ON_ERROR", True)
    monkeypatch.setattr(server, "_PAUSE_FILE", str(tmp_path / "paused"))
    statuses = []
    monkeypatch.setattr(server.syncs_total, "labels", lambda status: statuses.append(status) or _Noop())
    failed = []
    monkeypatch.setattr(server.device_sync_failed, "labels", lambda instance: failed.append(instance) or _Noop())
    marked = []
    monkeypatch.setattr(server, "_mark_synced", marked.append)

    server._run_sync(host, sync_mac=False, sync_ip=False, sync_modules=False,
                     sync_sfp=False, sync_poe=False, housekeeping=False)

    assert statuses == ["not_in_netbox"]
    assert failed == []                       # no failed flag for the device
    assert not server._is_paused()            # no auto-pause
    assert marked == []                       # no cooldown mark
    assert host in server._load_unknown_devices()

class _Noop:
    def inc(self, *a) -> None: ...
    def set(self, *a) -> None: ...

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
