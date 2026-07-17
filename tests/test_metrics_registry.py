"""Regression test: server.py must be importable twice in one process.

Under uvicorn multiprocessing the module is loaded twice in the same
process (once as __main__ via spawn re-import, once as "server" for the
server:app import string). Any metric registered to prometheus_client's
global default REGISTRY instead of the module's private _custom_registry
raises "Duplicated timeseries in CollectorRegistry" on the second import
and crashes the worker at startup.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_metrics_registry.py
"""
from __future__ import annotations

import importlib.util
import os
import sys
import tempfile

_TMPDIR = tempfile.mkdtemp(prefix="discobox-test-")
os.environ["PROMETHEUS_MULTIPROC_DIR"] = _TMPDIR

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
