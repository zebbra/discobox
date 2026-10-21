"""Every discobox metric is on the private registry (metrics._custom_registry),
none on prometheus_client's global REGISTRY: a metric there collides
("Duplicated timeseries in CollectorRegistry") when its module is loaded again.

Run with `pytest tests/` or directly:
    python -m pytest tests/test_metrics_registry.py
"""
from __future__ import annotations

import os
import sys
import tempfile

os.environ.setdefault("DISCOBOX_STATE_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prometheus_client import REGISTRY  # noqa: E402

import server  # noqa: E402


def _names(registry) -> set[str]:
    return {m.name for m in registry.collect()}


def test_no_discobox_metric_on_the_global_registry() -> None:
    assert not {n for n in _names(REGISTRY) if n.startswith("discobox_")}
    private = _names(server._custom_registry)
    assert "discobox_syncs" in private and "discobox_job_runs" in private
