"""Unit tests for NetdiscoClient.find_canonical_ip — the 404 fallback used when
Netbox's primary_ip4 is a cluster's shared/floating address that Netdisco only
knows as a secondary IP, not the device's own canonical one.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_netdisco_client.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetdiscoClient


def _fake_self(search_result):
    return SimpleNamespace(_get=lambda path: search_result)


def test_find_canonical_ip_unique_match() -> None:
    result = [{"name": "zcmgt0004p1v", "ip": "10.176.136.21"}]
    assert NetdiscoClient.find_canonical_ip(_fake_self(result), "10.176.136.20") == "10.176.136.21"


def test_find_canonical_ip_no_match() -> None:
    assert NetdiscoClient.find_canonical_ip(_fake_self([]), "10.176.136.20") is None


def test_find_canonical_ip_ambiguous_match_not_guessed() -> None:
    result = [
        {"name": "dev-a", "ip": "10.0.0.1"},
        {"name": "dev-b", "ip": "10.0.0.2"},
    ]
    assert NetdiscoClient.find_canonical_ip(_fake_self(result), "10.176.136.20") is None


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
