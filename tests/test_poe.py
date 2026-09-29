"""_apply_poe: poe_mode=pse on powered ports, never on virtual/bridge/LAG interfaces."""
from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox import NetboxClient, _apply_poe  # noqa: E402


class _If:
    def __init__(self, name, type_, poe=None):
        self.name, self.type, self.poe_mode = name, NS(value=type_), (NS(value=poe) if poe else None)
        self.updates: list = []

    def update(self, patch):
        self.updates.append(patch)


def test_poe_skips_virtual_bridge_lag() -> None:
    ifaces = {i.name: i for i in (
        _If("GigabitEthernet1/0/1", "1000base-t"), _If("GigabitEthernet1/0/2", "1000base-t", poe="pse"),
        _If("Vlan1", "virtual"), _If("Port-channel1", "lag"), _If("br0", "bridge"),
    )}
    ports = [{"port": n} for n in (*ifaces, "GigabitEthernet9/0/9")]
    counts = {"updated": 0, "unchanged": 0, "skipped": 0, "error": 0}
    _apply_poe(NS(_nb_value=NetboxClient._nb_value), ifaces, ports, counts, logging.getLogger("t"))
    assert counts == {"updated": 1, "unchanged": 1, "skipped": 4, "error": 0}
    assert ifaces["GigabitEthernet1/0/1"].updates == [{"poe_mode": "pse"}]
    assert all(not ifaces[n].updates for n in ("Vlan1", "Port-channel1", "br0"))
