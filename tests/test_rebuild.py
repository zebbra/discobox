"""Unit tests for the /rebuild prune helpers on NetboxClient:
remove_stale_interfaces, remove_stale_modules, remove_stale_bare_inventory_items,
remove_stale_sfps.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_rebuild.py
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient


class FakeRecord:
    def __init__(self, id: int, name: str = "", custom_fields: dict | None = None, **extra):
        self.id = id
        self.name = name
        self.custom_fields = custom_fields or {}
        self.deleted = False
        for k, v in extra.items():
            setattr(self, k, v)

    def delete(self):
        self.deleted = True


def _fake_self(interfaces=(), module_bays=(), inventory_items=()):
    def _endpoint(items, id_field="device_id"):
        def _filter(**kw):
            module_id = kw.get("module_id")
            if module_id is not None:
                return [i for i in items if getattr(i, "module_id", None) == module_id]
            ctype = kw.get("component_type")
            if ctype is not None:
                return [i for i in items if getattr(i, "component_type", None) == ctype]
            return list(items)
        return SimpleNamespace(filter=_filter)

    nb = SimpleNamespace(
        dcim=SimpleNamespace(
            interfaces=_endpoint(interfaces),
            module_bays=_endpoint(module_bays),
            inventory_items=_endpoint(inventory_items),
        ),
    )
    return SimpleNamespace(nb=nb)


DEVICE = SimpleNamespace(id=1)


# ── remove_stale_interfaces ─────────────────────────────────────────────────────

def test_remove_stale_interfaces_deletes_netdisco_owned_only() -> None:
    stale_owned = FakeRecord(1, "Gi1/0/1", custom_fields={"source": "netdisco"})
    stale_foreign = FakeRecord(2, "Gi1/0/2", custom_fields={"source": "manual"})
    current = FakeRecord(3, "Gi1/0/3", custom_fields={"source": "netdisco"})
    fake = _fake_self(interfaces=[stale_owned, stale_foreign, current])

    deleted = NetboxClient.remove_stale_interfaces(
        fake, DEVICE, {"Gi1/0/3"}, "source", "netdisco", dry_run=False,
    )

    assert deleted == 1
    assert stale_owned.deleted is True
    assert stale_foreign.deleted is False
    assert current.deleted is False


def test_remove_stale_interfaces_dry_run_counts_without_deleting() -> None:
    stale_owned = FakeRecord(1, "Gi1/0/1", custom_fields={"source": "netdisco"})
    fake = _fake_self(interfaces=[stale_owned])

    deleted = NetboxClient.remove_stale_interfaces(fake, DEVICE, set(), "source", "netdisco", dry_run=True)

    assert deleted == 1
    assert stale_owned.deleted is False


# ── remove_stale_modules ─────────────────────────────────────────────────────────

def test_remove_stale_modules_deletes_bay_with_no_module() -> None:
    bay = FakeRecord(1, "Slot 3", installed_module=None)
    fake = _fake_self(module_bays=[bay])

    deleted, skipped = NetboxClient.remove_stale_modules(fake, DEVICE, set(), "source", "netdisco", dry_run=False)

    assert (deleted, skipped) == (1, 0)
    assert bay.deleted is True


def test_remove_stale_modules_deletes_module_and_bay_when_clear() -> None:
    module = FakeRecord(10)
    bay = FakeRecord(1, "Slot 3", installed_module=module)
    fake = _fake_self(module_bays=[bay], interfaces=[])  # no interfaces on the module

    deleted, skipped = NetboxClient.remove_stale_modules(fake, DEVICE, set(), "source", "netdisco", dry_run=False)

    assert (deleted, skipped) == (1, 0)
    assert module.deleted is True
    assert bay.deleted is True


def test_remove_stale_modules_kept_when_foreign_interface_attached() -> None:
    module = FakeRecord(10)
    bay = FakeRecord(1, "Slot 3", installed_module=module)
    foreign_iface = FakeRecord(5, "Te3/1/1", custom_fields={"source": "manual"}, module_id=10)
    fake = _fake_self(module_bays=[bay], interfaces=[foreign_iface])

    deleted, skipped = NetboxClient.remove_stale_modules(fake, DEVICE, set(), "source", "netdisco", dry_run=False)

    assert (deleted, skipped) == (0, 1)
    assert module.deleted is False
    assert bay.deleted is False


def test_remove_stale_modules_deleted_when_only_netdisco_owned_interface_attached() -> None:
    # remove_stale_interfaces runs first in sync_device — by the time this runs, a
    # netdisco-owned interface on a genuinely-removed module is already gone. If it
    # somehow isn't, it must NOT block deletion the way a foreign one does.
    module = FakeRecord(10)
    bay = FakeRecord(1, "Slot 3", installed_module=module)
    owned_iface = FakeRecord(5, "Te3/1/1", custom_fields={"source": "netdisco"}, module_id=10)
    fake = _fake_self(module_bays=[bay], interfaces=[owned_iface])

    deleted, skipped = NetboxClient.remove_stale_modules(fake, DEVICE, set(), "source", "netdisco", dry_run=False)

    assert (deleted, skipped) == (1, 0)
    assert module.deleted is True
    assert bay.deleted is True


def test_remove_stale_modules_bay_in_current_names_kept() -> None:
    bay = FakeRecord(1, "Slot 3", installed_module=None)
    fake = _fake_self(module_bays=[bay])

    deleted, skipped = NetboxClient.remove_stale_modules(
        fake, DEVICE, {"Slot 3"}, "source", "netdisco", dry_run=False,
    )

    assert (deleted, skipped) == (0, 0)
    assert bay.deleted is False


def test_remove_stale_modules_dry_run_counts_without_deleting() -> None:
    module = FakeRecord(10)
    bay = FakeRecord(1, "Slot 3", installed_module=module)
    fake = _fake_self(module_bays=[bay], interfaces=[])

    deleted, skipped = NetboxClient.remove_stale_modules(fake, DEVICE, set(), "source", "netdisco", dry_run=True)

    assert (deleted, skipped) == (1, 0)
    assert module.deleted is False
    assert bay.deleted is False


# ── remove_stale_bare_inventory_items ───────────────────────────────────────────

def test_remove_stale_bare_inventory_items_ignores_component_linked() -> None:
    sfp_item = FakeRecord(1, "SFP1", component_type="dcim.interface")
    fake = _fake_self(inventory_items=[sfp_item])

    deleted = NetboxClient.remove_stale_bare_inventory_items(fake, DEVICE, set(), dry_run=False)

    assert deleted == 0
    assert sfp_item.deleted is False


def test_remove_stale_bare_inventory_items_deletes_stale_fan_or_psu() -> None:
    # Real discobox-created fan/PSU items always have a serial or part_id —
    # upsert_inventory_item() only ever creates one when Netdisco reports one.
    stale_fan = FakeRecord(1, "Fan 1", serial="FAN-SN-1")
    current_psu = FakeRecord(2, "PSU 1", part_id="PWR-715")
    fake = _fake_self(inventory_items=[stale_fan, current_psu])

    deleted = NetboxClient.remove_stale_bare_inventory_items(fake, DEVICE, {"PSU 1"}, dry_run=False)

    assert deleted == 1
    assert stale_fan.deleted is True
    assert current_psu.deleted is False


def test_remove_stale_bare_inventory_items_skips_template_placeholders() -> None:
    # A stack member's "StackPort1/1"/"StackAdapter1/1" etc. come from the
    # Module Type's InventoryItemTemplate, never from upsert_inventory_item() —
    # they have no serial and no part_id, unlike anything discobox creates.
    # Netbox auto-recreates these on module save, so discobox must never
    # touch them regardless of whether they look "stale".
    stack_port = FakeRecord(1, "StackPort1/1")
    fake = _fake_self(inventory_items=[stack_port])

    deleted = NetboxClient.remove_stale_bare_inventory_items(fake, DEVICE, set(), dry_run=False)

    assert deleted == 0
    assert stack_port.deleted is False


# ── remove_stale_sfps ────────────────────────────────────────────────────────────

def test_remove_stale_sfps_deletes_when_interface_no_longer_reports_one() -> None:
    stale_sfp = FakeRecord(1, "SFP-old", component_type="dcim.interface", component_id=99)
    current_sfp = FakeRecord(2, "SFP-current", component_type="dcim.interface", component_id=42)
    fake = _fake_self(inventory_items=[stale_sfp, current_sfp])

    deleted = NetboxClient.remove_stale_sfps(fake, DEVICE, {42}, dry_run=False)

    assert deleted == 1
    assert stale_sfp.deleted is True
    assert current_sfp.deleted is False


def test_remove_stale_sfps_dry_run_counts_without_deleting() -> None:
    stale_sfp = FakeRecord(1, "SFP-old", component_type="dcim.interface", component_id=99)
    fake = _fake_self(inventory_items=[stale_sfp])

    deleted = NetboxClient.remove_stale_sfps(fake, DEVICE, set(), dry_run=True)

    assert deleted == 1
    assert stale_sfp.deleted is False


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
