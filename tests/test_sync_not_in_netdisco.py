"""A sync for an IP Netdisco doesn't know ends as not_in_netdisco, not as an error.

Run with `pytest tests/` or directly:
    python tests/test_sync_not_in_netdisco.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

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


class _AliasNetdisco:
    """The synced IP 404s; Netdisco has it as an alias of an old device elsewhere."""

    def get_device(self, ip: str) -> dict:
        if ip == "192.0.2.10":
            resp = requests.Response()
            resp.status_code = 404
            raise requests.HTTPError("404", response=resp)
        return {"ip": ip, "name": "old-edge.example.net", "serial": "OLD1"}

    def get_ports(self, ip: str) -> list:
        if ip == "192.0.2.10":
            return []
        return [{"port": "GigabitEthernet0/1", "type": "ethernetCsmacd"}]

    def find_canonical_ip(self, ip: str) -> str | None:
        return "198.51.100.7"


class _Devices:
    def filter(self, **_):
        return []


class _Netbox:
    def __init__(self) -> None:
        self.device = SimpleNamespace(id=1, name="new-switch.example.net", custom_fields={}, tags=[])
        self.nb = SimpleNamespace(dcim=SimpleNamespace(devices=_Devices()))

    def find_device_by_ip(self, ip, hostname=None, serial=None):
        return self.device

    def find_device_by_serial(self, serial):
        return None

    def find_ha_partner(self, name):
        return None


def test_address_held_by_a_device_not_in_netbox_is_not_synced() -> None:
    result = discobox.sync_device("192.0.2.10", _AliasNetdisco(), _Netbox())
    assert result["ok"] is False and result["reason"] == "ip_held_by_other_device"
    assert result["netdisco_ip"] == "198.51.100.7" and result["hostname"] == "old-edge.example.net"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
