"""Unit tests for the settings schema (config.py).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_config.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import ConfigError, effective, load_settings, parse_settings  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


def test_repo_example_config_is_valid() -> None:
    settings, source = load_settings(str(REPO / "discobox.yaml"))
    assert source.endswith("discobox.yaml")
    assert settings.jobs.reconcile.interval == 86400 and settings.jobs.types_bare.interval == 3600


def test_defaults() -> None:
    s = parse_settings({})
    assert s.sync.cooldown == 3600 and s.sync.max_concurrent == 3 and s.sync.retry.delay == 480
    assert s.sync.remove_orphaned_interfaces is None
    assert s.reconcile.roles == ["router", "switch", "firewall", "appliance"] and s.reconcile.statuses == ["active"]
    assert s.custom_fields.touch == "netdisco_last_update" and s.reconcile.liveness.url is None


def test_null_or_blank_turns_an_optional_field_off() -> None:
    cf = parse_settings({"custom_fields": {"touch": None, "os_name": "  ", "controller": " ctl "}}).custom_fields
    assert cf.touch is None and cf.os_name is None and cf.controller == "ctl"


def test_lists_from_comma_strings_and_jobs_by_yaml_name() -> None:
    s = parse_settings({"reconcile": {"roles": "router, switch"}, "jobs": {"types-bare": {"interval": 0}}})
    assert s.reconcile.roles == ["router", "switch"] and s.jobs.types_bare.interval == 0


def test_unknown_keys_and_bad_types_name_the_path() -> None:
    with pytest.raises(ConfigError) as exc:
        parse_settings({"sync": {"cooldwn": 1}, "reconcile": {"interval": 3600, "max_enqueue": "many"}})
    msg = str(exc.value)
    assert "sync.cooldwn" in msg and "reconcile.interval" in msg and "reconcile.max_enqueue" in msg


def test_missing_explicit_file_is_an_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DISCOBOX_CONFIG", str(tmp_path / "nope.yaml"))
    with pytest.raises(ConfigError):
        load_settings()


def test_effective_masks_secrets(monkeypatch) -> None:
    monkeypatch.setenv("NETBOX_TOKEN", "s3cret")
    monkeypatch.setenv("NETBOX_URL", "http://netbox")
    out = effective(parse_settings({"auth": {"token": "t0ken"}}), "test")
    assert out["env"]["NETBOX_TOKEN"] == "***" and out["env"]["NETBOX_URL"] == "http://netbox"
    assert out["settings"]["auth"]["token"] == "***" and "types-bare" in out["settings"]["jobs"]
