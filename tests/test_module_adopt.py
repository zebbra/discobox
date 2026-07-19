"""Unit tests for _clear_module_template_collisions (module adopt emulation).

Netbox's REST API has no adopt_components: module creation always replicates
interface templates and 500s on any existing same-name interface. discobox
deletes empty colliding interfaces and retries.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_module_adopt.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient


class FakeIface:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name
        self.deleted = False

    def delete(self):
        self.deleted = True


def _fake_self(templates: list, ifaces: list[FakeIface], iface_ids_with_ips: set[int]):
    def _endpoint(filter_result):
        return SimpleNamespace(filter=lambda **kw: list(filter_result))

    nb = SimpleNamespace(
        dcim=SimpleNamespace(
            interface_templates=_endpoint(templates),
            interfaces=_endpoint(ifaces),
        ),
        ipam=SimpleNamespace(
            ip_addresses=SimpleNamespace(
                filter=lambda **kw: [object()] if kw.get("assigned_object_id") in iface_ids_with_ips else []
            ),
        ),
    )
    return SimpleNamespace(nb=nb)


DEVICE = SimpleNamespace(id=24330)
BAY = SimpleNamespace(id=1, name="Switch 1 FRU Uplink Module 1", position=1)
MODULE_TYPE = SimpleNamespace(id=7)
TEMPLATES = [SimpleNamespace(name="TwentyFiveGigE{module}/1/1"), SimpleNamespace(name="TwentyFiveGigE{module}/1/2")]


def _run(templates, ifaces, with_ips=set()):
    return NetboxClient._clear_module_template_collisions(
        _fake_self(templates, ifaces, with_ips), DEVICE, BAY, MODULE_TYPE,
    )


def test_colliding_empty_interfaces_deleted_with_module_token() -> None:
    uplink1 = FakeIface(1, "TwentyFiveGigE1/1/1")   # {module} → bay position 1
    uplink2 = FakeIface(2, "TwentyFiveGigE1/1/2")
    other = FakeIface(3, "GigabitEthernet1/0/1")
    cleared = _run(TEMPLATES, [uplink1, uplink2, other])
    assert cleared == 2
    assert uplink1.deleted and uplink2.deleted and not other.deleted


def test_interface_with_ip_never_deleted() -> None:
    uplink = FakeIface(1, "TwentyFiveGigE1/1/1")
    cleared = _run(TEMPLATES, [uplink], with_ips={1})
    assert cleared == 0 and not uplink.deleted


def test_no_templates_or_no_collision() -> None:
    assert _run([], [FakeIface(1, "TwentyFiveGigE1/1/1")]) == 0
    assert _run(TEMPLATES, [FakeIface(1, "HundredGigE1/0/25")]) == 0


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
