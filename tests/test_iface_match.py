"""Unit tests for _match_existing_iface (case-insensitive interface adoption).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_iface_match.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import _match_existing_iface


class FakeIface:
    def __init__(self, id: int, name: str, source: str | None = None):
        self.id = id
        self.name = name
        self.custom_fields = {"source": source} if source else {}
        self.deleted = False

    def delete(self):
        self.deleted = True


class FakeNB:
    """Only nb.nb.ipam.ip_addresses.filter(assigned_object_id=...) is used."""
    def __init__(self, iface_ids_with_ips: set[int]):
        outer = self

        class _IPs:
            def filter(self, **kw):
                return [object()] if kw.get("assigned_object_id") in outer._with_ips else []

        class _NS:
            pass

        self._with_ips = iface_ids_with_ips
        self.nb = _NS()
        self.nb.ipam = _NS()
        self.nb.ipam.ip_addresses = _IPs()


def test_exact_match_wins_untouched() -> None:
    exact = FakeIface(1, "Loopback0")
    existing = {"Loopback0": exact}
    got = _match_existing_iface(FakeNB(set()), existing, "Loopback0", source_cf="source")
    assert got is exact and not exact.deleted


def test_case_variant_adopted_and_rekeyed() -> None:
    manual = FakeIface(1, "loopback3899")
    existing = {"loopback3899": manual}
    got = _match_existing_iface(FakeNB({1}), existing, "Loopback3899", source_cf="source")
    assert got is manual and not manual.deleted
    assert "Loopback3899" in existing and "loopback3899" not in existing


def test_duplicate_merged_keeps_ip_holder() -> None:
    # discobox-created duplicate (empty, ours) + manual variant holding the IP
    dup = FakeIface(1, "Loopback3899", source="netdisco")
    manual = FakeIface(2, "loopback3899")
    existing = {"Loopback3899": dup, "loopback3899": manual}
    got = _match_existing_iface(FakeNB({2}), existing, "Loopback3899", source_cf="source")
    assert got is manual
    assert dup.deleted and not manual.deleted
    assert existing == {"Loopback3899": manual}


def test_duplicate_merged_keeps_exact_with_ip() -> None:
    exact = FakeIface(1, "Loopback3899", source="netdisco")
    stale = FakeIface(2, "loopback3899")
    existing = {"Loopback3899": exact, "loopback3899": stale}
    got = _match_existing_iface(FakeNB({1}), existing, "Loopback3899", source_cf="source")
    assert got is exact
    assert stale.deleted and not exact.deleted


def test_both_hold_ips_nothing_deleted() -> None:
    exact = FakeIface(1, "Loopback3899")
    variant = FakeIface(2, "loopback3899")
    existing = {"Loopback3899": exact, "loopback3899": variant}
    got = _match_existing_iface(FakeNB({1, 2}), existing, "Loopback3899", source_cf="source")
    assert got is exact
    assert not exact.deleted and not variant.deleted


def test_foreign_owned_duplicate_kept() -> None:
    exact = FakeIface(1, "Loopback3899")
    foreign = FakeIface(2, "loopback3899", source="manual")
    existing = {"Loopback3899": exact, "loopback3899": foreign}
    got = _match_existing_iface(FakeNB({1}), existing, "Loopback3899", source_cf="source")
    assert got is exact and not foreign.deleted


# ── _iface_lookup ──────────────────────────────────────────────────────────────

def test_iface_lookup_exact_and_case_fallback() -> None:
    from discobox import _iface_lookup
    lo = FakeIface(1, "loopback3899")
    up = FakeIface(2, "Loopback3899")
    both = {"loopback3899": lo, "Loopback3899": up}
    # exact match preferred when present
    assert _iface_lookup(both, "Loopback3899") is up
    # case-insensitive fallback (Netdisco device_ips vs ports case mismatch)
    assert _iface_lookup({"loopback3899": lo}, "Loopback3899") is lo
    assert _iface_lookup({"Loopback3899": up}, "loopback3899") is up
    assert _iface_lookup({"loopback3899": lo}, "GigabitEthernet1/0/1") is None


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
