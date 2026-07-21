"""Unit tests for the on_request hooks used to count outbound HTTP requests
to Netbox/Netdisco (discobox_netbox_requests_total / discobox_netdisco_requests_total).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_request_counting.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient, _ChangelogSession


def test_changelog_session_invokes_on_request(monkeypatch) -> None:
    calls: list[str] = []
    session = _ChangelogSession("discobox", on_request=calls.append)

    class _FakeResponse:
        status_code = 200

    import requests
    monkeypatch.setattr(requests.Session, "request", lambda self, method, url, **kw: _FakeResponse())
    session.request("GET", "http://netbox.example/api/")
    session.request("patch", "http://netbox.example/api/dcim/devices/1/")  # method is uppercased before the hook fires

    assert calls == ["GET", "PATCH"]


def test_netbox_client_wires_on_request_into_session() -> None:
    # Bound methods (like list.append) aren't `is`-stable across attribute
    # accesses, so verify functionally: calling the wired hook must reach
    # the same list this test passed in.
    seen: list[str] = []
    nb = NetboxClient(url="http://netbox.example", token="x", on_request=seen.append)
    nb.nb.http_session._on_request("GET")
    assert seen == ["GET"]


def test_no_on_request_is_a_safe_no_op() -> None:
    # Default (no callback) must not raise.
    session = _ChangelogSession("discobox")
    session._on_request("GET")


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
