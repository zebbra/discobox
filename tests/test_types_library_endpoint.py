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


def test_missing_library_is_503(monkeypatch) -> None:
    monkeypatch.setattr(server, "_library", None)
    monkeypatch.setattr(server, "_LIBRARY_PATH", "/nonexistent")
    with pytest.raises(HTTPException) as exc:
        server._get_library()
    assert exc.value.status_code == 503
