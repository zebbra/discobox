"""A sync for an IP Netdisco doesn't know ends as not_in_netdisco, not as an error.

Run with `pytest tests/` or directly:
    python tests/test_sync_not_in_netdisco.py
"""
from __future__ import annotations

import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import discobox  # noqa: E402


class _FakeNetdisco:
    def __init__(self, status: int, canonical: str | None = None) -> None:
        self.status, self.canonical = status, canonical

    def get_device(self, ip: str) -> dict:
        resp = requests.Response()
        resp.status_code = self.status
        raise requests.HTTPError(f"{self.status}", response=resp)

    def get_ports(self, ip: str) -> list:
        return []

    def find_canonical_ip(self, ip: str) -> str | None:
        return self.canonical


def test_404_without_canonical_ip_is_not_in_netdisco() -> None:
    result = discobox.sync_device("192.0.2.10", _FakeNetdisco(404), None)
    assert result["ok"] is False and result["reason"] == "not_in_netdisco"


def test_other_http_error_stays_an_error() -> None:
    result = discobox.sync_device("192.0.2.10", _FakeNetdisco(403), None)
    assert result["ok"] is False and "reason" not in result


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
