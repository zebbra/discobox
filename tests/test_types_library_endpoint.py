"""/types/library endpoint: request validation and delegation to typesync.sync_types.

Calls the endpoint function directly (fastapi's TestClient needs httpx, which
isn't a dependency).
"""
from __future__ import annotations

import os
import sys
import tempfile
from types import SimpleNamespace

os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import server  # noqa: E402

GET, POST = SimpleNamespace(method="GET"), SimpleNamespace(method="POST")


@pytest.fixture
def calls(monkeypatch) -> list[dict]:
    seen: list[dict] = []

    def fake_sync_types(nb, library, mapping, roles, types, apply=False):
        seen.append({"roles": roles, "types": types, "apply": apply})
        return {"apply": apply, "summary": {}, "types": []}

    monkeypatch.setattr(server, "sync_types", fake_sync_types)
    monkeypatch.setattr(server, "_get_library", lambda: object())
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_LIBRARY_ROLES", [])
    return seen


def test_get_is_dry_run(calls) -> None:
    result = server.types_library(GET, role=["lwapp-ap"], type=["9120AX"], apply=False)
    assert result["apply"] is False
    assert calls == [{"roles": ["lwapp-ap"], "types": ["9120AX"], "apply": False}]


def test_apply_needs_post(calls) -> None:
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=["lwapp-ap"], type=None, apply=True)
    assert exc.value.status_code == 405 and calls == []
    server.types_library(POST, role=["lwapp-ap"], type=None, apply=True)
    assert calls[-1]["apply"] is True


def test_nothing_selected_is_rejected(calls) -> None:
    with pytest.raises(HTTPException) as exc:
        server.types_library(GET, role=None, type=None, apply=False)
    assert exc.value.status_code == 400 and calls == []


def test_library_roles_default(calls, monkeypatch) -> None:
    monkeypatch.setattr(server, "_LIBRARY_ROLES", ["lwapp-ap"])
    server.types_library(GET, role=None, type=None, apply=False)
    assert calls[-1]["roles"] == ["lwapp-ap"]


def test_type_only_request_ignores_default_roles(calls, monkeypatch) -> None:
    monkeypatch.setattr(server, "_LIBRARY_ROLES", ["lwapp-ap"])
    server.types_library(GET, role=None, type=["WS-C3560CG-8TC-S"], apply=False)
    assert calls[-1] == {"roles": [], "types": ["WS-C3560CG-8TC-S"], "apply": False}


def test_missing_library_is_503(monkeypatch) -> None:
    monkeypatch.setattr(server, "_library", None)
    monkeypatch.setattr(server, "_LIBRARY_PATH", "/nonexistent")
    with pytest.raises(HTTPException) as exc:
        server._get_library()
    assert exc.value.status_code == 503


def test_reconcile_diff_is_compare_only(monkeypatch) -> None:
    seen: dict = {}

    def fake_reconcile(nd, nb, **kw):
        seen.update(kw)
        return {"netbox_total": 10, "netdisco_total": 9, "already_known": 8, "not_in_netdisco": 1,
                "skipped_offline": 1, "not_in_netbox": 1, "tag_mismatches": 2,
                "not_in_netdisco_list": [{"ip": "192.0.2.1"}], "not_in_netbox_list": [], "tag_mismatches_list": []}

    monkeypatch.setattr(server, "reconcile_devices", fake_reconcile)
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: object())
    monkeypatch.setattr(server, "_get_netbox_client", lambda: object())
    monkeypatch.setattr(server, "_LIVENESS_URL", None)
    monkeypatch.setattr(server, "_is_paused", lambda: True)          # read-only: runs while paused
    monkeypatch.setattr(server, "_save_gap", lambda *a: None)
    r = server.reconcile_diff(lists=False)          # GET /reconcile
    assert seen["max_enqueue"] == 0 and seen["auto_create_role"] is None
    assert seen["max_queued"] is None and seen["max_failed"] is None
    assert r == {"netbox_total": 10, "netdisco_total": 9, "already_known": 8, "not_in_netdisco": 1,
                 "not_in_netbox": 1, "tag_mismatches": 2, "offline": 1}
    assert server.reconcile_diff(lists=True)["not_in_netdisco_list"] == [{"ip": "192.0.2.1"}]


# ── /discover ──

class _NDJobs:
    def __init__(self):
        self.jobs: list = []

    def enqueue_discover(self, ip, device_auth_tag_hint=None, snmp_timeout_us=None):
        self.jobs.append((ip, device_auth_tag_hint, snmp_timeout_us))


def _discover_env(monkeypatch, devices=()):
    nd = _NDJobs()
    lookups: list = []

    class _NB:
        def __init__(self):
            self.nb = SimpleNamespace(dcim=SimpleNamespace(devices=SimpleNamespace(filter=self._filter)))

        def _filter(self, name__ie=None, name__isw=None):
            lookups.append(name__ie or name__isw)
            if name__ie:
                return [d for d in devices if d.name.lower() == name__ie.lower()]
            return [d for d in devices if d.name.lower().startswith(name__isw.lower())]

        def find_device_by_ip(self, ip):
            lookups.append(ip)
            return next((d for d in devices if str(d.primary_ip4).split("/")[0] == ip), None)

    monkeypatch.setattr(server, "_get_netdisco_client", lambda: nd)
    monkeypatch.setattr(server, "_get_netbox_client", lambda: _NB())
    return nd, lookups


def _dev(name, ip, tag=None, timeout=None):
    return SimpleNamespace(name=name, primary_ip4=f"{ip}/24" if ip else None,
                           custom_fields={"snmp_auth_profile": tag, "snmp_polling_timeout": timeout})


def test_discover_with_params_needs_no_lookup(monkeypatch) -> None:
    nd, lookups = _discover_env(monkeypatch)
    r = server.discover(host="192.0.2.1", tag="v3", timeout="3m")
    assert nd.jobs == [("192.0.2.1", "v3", 180_000_000)] and lookups == []
    assert r["tag_source"] == r["timeout_source"] == "param"


def test_discover_takes_profile_and_timeout_from_netbox(monkeypatch) -> None:
    nd, _ = _discover_env(monkeypatch, [_dev("wlc1.example.com", "192.0.2.8", tag="wlc-v3", timeout="2m")])
    r = server.discover(host="192.0.2.8", tag=None, timeout=None)
    assert nd.jobs == [("192.0.2.8", "wlc-v3", 120_000_000)] and r["device"] == "wlc1.example.com"
    assert r["tag_source"] == r["timeout_source"] == "netbox"
    # by (short) name: the device's primary IP is discovered
    r = server.discover(host="WLC1", tag=None, timeout=None)
    assert nd.jobs[-1] == ("192.0.2.8", "wlc-v3", 120_000_000) and r["host"] == "192.0.2.8"


def test_discover_unknown_ip_default_and_errors(monkeypatch) -> None:
    nd, _ = _discover_env(monkeypatch)
    r = server.discover(host="192.0.2.99", tag=None, timeout=None)
    assert nd.jobs == [("192.0.2.99", None, None)] and r["timeout_us"] == 3_000_000
    assert r["tag_source"] == r["timeout_source"] == "default"
    for kw, code in (({"host": "no-such-device", "tag": None, "timeout": None}, 404),
                     ({"host": "192.0.2.1", "tag": "x", "timeout": "soon"}, 400)):
        with pytest.raises(HTTPException) as exc:
            server.discover(**kw)
        assert exc.value.status_code == code
