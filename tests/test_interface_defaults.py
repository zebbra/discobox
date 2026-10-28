"""Unit tests for interface_defaults: fill empty interface custom fields per device custom field.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_interface_defaults.py
"""
from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import parse_settings  # noqa: E402
from discobox.interfaces import apply_interface_defaults, interface_defaults_for  # noqa: E402

RULE = {"field": "interface_severity", "value": "warning", "device_field": "cf_service_team",
        "device_values": ["netops", "campus"], "physical_only": True}
LOG = logging.getLogger("test")


def _iface(id_, type_, cf=None):
    return SimpleNamespace(id=id_, type=SimpleNamespace(value=type_), custom_fields=dict(cf or {}))


class FakeNB:
    def __init__(self):
        self.batches: list = []

    def flush_interface_updates(self, batch, log=None):
        self.batches.append(batch)
        return set()


def test_device_match_by_select_multiselect_object_or_text() -> None:
    def dev(value):
        return SimpleNamespace(custom_fields={"service_team": value})
    assert interface_defaults_for(dev("NETOPS"), [RULE]) == [RULE]
    assert interface_defaults_for(dev(["xyz", "campus"]), [RULE]) == [RULE]
    assert interface_defaults_for(dev({"id": 3, "name": "Campus", "slug": "campus"}), [RULE]) == [RULE]
    assert interface_defaults_for(dev("other"), [RULE]) == []
    assert interface_defaults_for(dev(None), [RULE]) == []
    assert interface_defaults_for(SimpleNamespace(custom_fields=None), [RULE]) == []


def test_fills_only_empty_physical_interfaces_never_overwrites() -> None:
    ifaces = {
        "Gi1/0/1": _iface(1, "1000base-t"),                                    # empty: filled
        "Gi1/0/2": _iface(2, "1000base-t", {"interface_severity": ""}),        # blank: filled
        "Gi1/0/3": _iface(3, "1000base-t", {"interface_severity": "none"}),    # deliberate: kept
        "Gi1/0/4": _iface(4, "1000base-t", {"interface_severity": "critical", "source": "manual"}),
        "Vlan10": _iface(5, "virtual"),                                        # not physical
        "Po1": _iface(6, "lag"),
        "Dot11Radio0": _iface(7, "ieee802.11ax"),
    }
    nb = FakeNB()
    counts = apply_interface_defaults(nb, ifaces, [RULE], LOG)
    assert counts == {"interface_severity": 2}
    assert [p["id"] for p in nb.batches[0]] == [1, 2]
    # the rest of the interface's custom fields is kept
    ifaces["Gi1/0/1"].custom_fields = {"source": "netdisco"}
    nb2 = FakeNB()
    apply_interface_defaults(nb2, {"Gi1/0/1": ifaces["Gi1/0/1"]}, [RULE], LOG)
    assert nb2.batches[0][0]["custom_fields"] == {"source": "netdisco", "interface_severity": "warning"}


def test_physical_only_off_includes_virtual_and_nothing_to_do_sends_nothing() -> None:
    rule = {**RULE, "physical_only": False}
    nb = FakeNB()
    assert apply_interface_defaults(nb, {"Vlan10": _iface(5, "virtual")}, [rule], LOG) == {"interface_severity": 1}
    nb2 = FakeNB()
    full = {"Gi1": _iface(1, "1000base-t", {"interface_severity": "warning"})}
    assert apply_interface_defaults(nb2, full, [RULE], LOG) == {} and nb2.batches == []


def test_config_schema() -> None:
    s = parse_settings({"interface_defaults": [{"field": "interface_severity", "value": "warning",
                                                "device_field": "cf_service_team", "device_values": "netops, campus"}]})
    rule = s.interface_defaults[0]
    assert rule.device_values == ["netops", "campus"] and rule.physical_only is True
    assert parse_settings({}).interface_defaults == []


class _Record:
    """pynetbox Record stand-in: dict(record) gives its fields, like the real one."""
    def __init__(self, **kw):
        self._kw = kw

    def __iter__(self):
        return iter(self._kw.items())


def test_device_field_builtin_and_debug_log(caplog) -> None:
    rule = {**RULE, "device_field": "status", "device_values": ["active"]}
    active = SimpleNamespace(status=_Record(value="active", label="Active"), custom_fields={})
    planned = SimpleNamespace(status=_Record(value="planned", label="Planned"), custom_fields={})
    assert interface_defaults_for(active, [rule]) == [rule]
    role = {**RULE, "device_field": "role", "device_values": ["Access-Switch"]}
    assert interface_defaults_for(SimpleNamespace(role=_Record(id=1, name="Access Switch", slug="access-switch")),
                                  [role]) == [role]
    with caplog.at_level(logging.DEBUG, logger="test"):
        assert interface_defaults_for(planned, [rule], LOG) == []
    assert "status=['planned'] not in ['active']" in caplog.text
