"""Regression test: server.py must be importable twice in one process.

Every metric must go to the module's private _custom_registry: one on
prometheus_client's global REGISTRY raises "Duplicated timeseries in
CollectorRegistry" on a second import (tests import server more than once).

Run with `pytest tests/` or directly:
    python -m pytest tests/test_metrics_registry.py
"""
from __future__ import annotations

import importlib.util
import os
import sys
import tempfile

_TMPDIR = tempfile.mkdtemp(prefix="discobox-test-")
os.environ["DISCOBOX_STATE_DIR"] = _TMPDIR

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)


def _import_server_as(name: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_ROOT, "server.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_server_imports_twice_without_registry_collision() -> None:
    _import_server_as("server_first_copy")
    _import_server_as("server_second_copy")  # raised Duplicated timeseries before fix
