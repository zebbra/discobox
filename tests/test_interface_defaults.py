"""Unit tests for interface_defaults: fill empty interface custom fields on devices matching a rule.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_interface_defaults.py
"""
from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import ConfigError, parse_settings  # noqa: E402
from discobox.interfaces import apply_interface_defaults, interface_defaults_for  # noqa: E402

RULE = {"name": "teams", "match": {"custom_fields.service_team": "netops|campus"},
        "set": {"interface_severity": "warning"}, "physical_only": True}
LOG = logging.getLogger("test")


def _iface(id_, type_, cf=None):
    return SimpleNamespace(id=id_, type=SimpleNamespace(value=type_), custom_fields=dict(cf or {}))


def _dev(**kw):
    kw.setdefault("custom_fields", {})
    return SimpleNamespace(**kw)


class FakeNB:
    def __init__(self):
        self.batches: list = []

    def flush_interface_updates(self, batch, log=None):
        self.batches.append(batch)
        return set()


def test_match_custom_field_select_multiselect_and_object_path() -> None:
    assert interface_defaults_for(_dev(custom_fields={"service_team": "NETOPS"}), [RULE]) == [RULE]
    assert interface_defaults_for(_dev(custom_fields={"service_team": ["xyz", "campus"]}), [RULE]) == [RULE]
    assert interface_defaults_for(_dev(custom_fields={"service_team": "other"}), [RULE]) == []
    assert interface_defaults_for(_dev(custom_fields={"service_team": None}), [RULE]) == []
    assert interface_defaults_for(_dev(custom_fields=None), [RULE]) == []
    obj = {**RULE, "match": {"custom_fields.service_team.slug": "campus"}}
    assert interface_defaults_for(_dev(custom_fields={"service_team": {"id": 3, "slug": "campus"}}), [obj]) == [obj]


def test_match_all_keys_fullmatch_regex_and_tags(caplog) -> None:
    rule = {**RULE, "match": {"status.value": "active", "role.slug": "access-.*", "tags": "prod"}}
    dev = _dev(status=SimpleNamespace(value="active"), role=SimpleNamespace(slug="access-switch"),
               tags=[SimpleNamespace(slug="lab"), SimpleNamespace(slug="prod")])
    assert interface_defaults_for(dev, [rule]) == [rule]
    dev.role.slug = "core-access"                  # fullmatch: no substring hit
    with caplog.at_level(logging.DEBUG, logger="test"):
        assert interface_defaults_for(dev, [rule], LOG) == []
    assert "rule [teams] MISS  role.slug pattern='access-.*' value='core-access'" in caplog.text


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


def test_first_rule_per_field_wins_and_several_fields_per_rule() -> None:
    core = {"name": "core", "match": {"role.slug": "core"}, "physical_only": True,
            "set": {"interface_severity": "critical", "monitoring_profile": "full"}}
    virtual = {"name": "all", "match": {"status.value": "active"}, "physical_only": False,
               "set": {"interface_severity": "warning"}}
    ifaces = {"Gi1": _iface(1, "1000base-t"), "Vlan10": _iface(2, "virtual")}
    nb = FakeNB()
    counts = apply_interface_defaults(nb, ifaces, [core, virtual], LOG)
    assert counts == {"interface_severity": 2, "monitoring_profile": 1}
    by_id = {p["id"]: p["custom_fields"] for p in nb.batches[0]}
    assert by_id[1] == {"interface_severity": "critical", "monitoring_profile": "full"}
    assert by_id[2] == {"interface_severity": "warning"}       # core skips virtual, the next rule covers it


def test_nothing_to_do_sends_nothing() -> None:
    nb = FakeNB()
    full = {"Gi1": _iface(1, "1000base-t", {"interface_severity": "warning"})}
    assert apply_interface_defaults(nb, full, [RULE], LOG) == {} and nb.batches == []


def test_config_schema() -> None:
    s = parse_settings({"interface_defaults": [{"match": {"custom_fields.service_team": "netops|campus"},
                                                "set": {"interface_severity": "warning"}}]})
    rule = s.interface_defaults[0]
    assert rule.match == {"custom_fields.service_team": "netops|campus"} and rule.physical_only is True
    assert parse_settings({}).interface_defaults == []
    with pytest.raises(ConfigError):
        parse_settings({"interface_defaults": [{"match": {"role.slug": "("}, "set": {"x": 1}}]})
    with pytest.raises(ConfigError):
        parse_settings({"interface_defaults": [{"match": {}, "set": {"x": 1}}]})
