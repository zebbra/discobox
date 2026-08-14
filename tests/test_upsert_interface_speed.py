"""Unit tests for upsert_interface's speed-change suppression: speed flaps on
its own (autonegotiation jitter) without anything else about the interface
changing, so it must never be the sole reason for a patch — but it should
still ride along when some other field change already triggers one.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_upsert_interface_speed.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient


class FakeIface:
    def __init__(self, id: int, **fields):
        self.id = id
        for k, v in fields.items():
            setattr(self, k, v)
        self.custom_fields = {}
        self.updated_with = None

    def update(self, patch):
        self.updated_with = patch
        for k, v in patch.items():
            setattr(self, k, v)
        return True


def _fake_self():
    return SimpleNamespace(_nb_value=NetboxClient._nb_value)


def _run(data, existing):
    return NetboxClient.upsert_interface(_fake_self(), device_id=1, data=data, existing=existing)


def test_speed_only_change_is_not_patched() -> None:
    existing = FakeIface(1, name="Gi1/0/1", speed=1_000_000, description="", enabled=True)
    action, iface = _run(
        {"name": "Gi1/0/1", "speed": 100_000, "description": "", "enabled": True},
        existing,
    )
    assert action == "unchanged"
    assert existing.updated_with is None
    assert existing.speed == 1_000_000  # left at the stale value, on purpose


def test_speed_change_rides_along_with_a_real_change() -> None:
    existing = FakeIface(1, name="Gi1/0/1", speed=1_000_000, description="", enabled=True)
    action, iface = _run(
        {"name": "Gi1/0/1", "speed": 100_000, "description": "uplink", "enabled": True},
        existing,
    )
    assert action == "updated"
    assert existing.updated_with == {"description": "uplink", "speed": 100_000}
    assert existing.speed == 100_000


def test_non_speed_change_alone_still_patches_as_before() -> None:
    existing = FakeIface(1, name="Gi1/0/1", speed=1_000_000, description="", enabled=True)
    action, iface = _run(
        {"name": "Gi1/0/1", "speed": 1_000_000, "description": "uplink", "enabled": True},
        existing,
    )
    assert action == "updated"
    assert existing.updated_with == {"description": "uplink"}


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
