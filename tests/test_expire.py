"""Expire: Netdisco devices whose Netbox device is decommissioned or that are long gone."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discobox.reconcile import expire_devices, find_gaps  # noqa: E402


def nb_dev(name, ip, status="active", **extra):
    return SimpleNamespace(name=name, primary_ip4=f"{ip}/24" if ip else None, status=SimpleNamespace(value=status),
                           custom_fields={}, **extra)


class Devices:
    def __init__(self, devices):
        self._devices = devices

    def filter(self, status=None, primary_ip4__isnull=None, has_primary_ip=None, **_):
        out = self._devices
        if status is not None:
            out = [d for d in out if d.status.value in status]
        if primary_ip4__isnull is False or has_primary_ip:
            out = [d for d in out if d.primary_ip4]
        return out


class NB:
    def __init__(self, devices):
        self.nb = SimpleNamespace(dcim=SimpleNamespace(devices=Devices(devices)))


class ND:
    def __init__(self, devices):
        self._devices = devices
        self.jobs: list[dict] = []

    def get_all_devices(self):
        return self._devices

    def get_queue_status(self, since="1h"):
        return {"queued": 0, "failed": 0}

    def enqueue_delete(self, ip, archive=True, log_msg=""):
        self.jobs.append({"ip": ip, "archive": archive, "log": log_msg})


def nd_dev(ip, name, last="2026-11-20 10:00:00", **extra):
    return {"ip": ip, "name": name, "dns": None, "last_discover": last, **extra}


ND_DEVICES = [
    nd_dev("10.0.0.1", "sw-active"),
    nd_dev("10.0.0.2", "sw-decom"),
    nd_dev("10.0.0.3", "sw-decom-noip.example.net"),
    nd_dev("10.0.0.4", "sw-gone", last="2026-09-01 10:00:00"),
    nd_dev("10.0.0.5", "sw-recent-gone", last="2026-11-15 10:00:00"),
    nd_dev("10.0.0.6", "pseudo", last="2025-01-01 10:00:00", is_pseudo=True),
    nd_dev("10.0.0.7", "sw-reused-old"),
]
NB_DEVICES = [
    nb_dev("sw-active", "10.0.0.1"),
    nb_dev("sw-decom", "10.0.0.2", "decommissioning"),
    nb_dev("sw-decom-noip", None, "decommissioning"),
    nb_dev("sw-reused-old", "10.0.0.7", "decommissioning"),
    nb_dev("sw-reused-new", "10.0.0.7", "active"),       # a new device on the old address
]


def gaps(**kw):
    return find_gaps(ND(ND_DEVICES), NB(NB_DEVICES), not_in_netbox_stale_hours=24, **kw)


def test_expire_off_by_default() -> None:
    c = gaps()
    assert c["expire"] == 0 and c["expire_list"] == []


def test_expire_by_status_and_stale() -> None:
    c = gaps(expire_statuses=["decommissioning"], expire_stale_days=30)
    by_ip = {e["ip"]: e for e in c["expire_list"]}
    # decommissioning by primary IP and by name; gone for >30d; not the recent one,
    # not the pseudo device, not the address an active device has now
    assert set(by_ip) == {"10.0.0.2", "10.0.0.3", "10.0.0.4"}
    assert by_ip["10.0.0.2"]["reason"] == "Netbox status decommissioning"
    assert by_ip["10.0.0.3"]["name"] == "sw-decom-noip"
    assert "not in Netbox" in by_ip["10.0.0.4"]["reason"]
    assert c["expire_list"][0]["ip"] == "10.0.0.4"      # oldest discovery first


def test_expire_respects_exclude() -> None:
    c = gaps(expire_statuses=["decommissioning"], exclude=[{"name": "keep", "match": {"name": "sw-decom"}}])
    assert {e["ip"] for e in c["expire_list"]} == {"10.0.0.3"}


def test_expire_devices_dry_run_deletes_nothing() -> None:
    nd = ND([])
    c = expire_devices(nd, [{"ip": "10.0.0.2", "name": "a", "reason": "r"}], dry_run=True)
    assert c["would_delete"] == 1 and nd.jobs == []


def test_expire_devices_archives_and_caps() -> None:
    nd = ND([])
    cands = [{"ip": f"10.0.0.{i}", "name": str(i), "reason": "r"} for i in range(1, 4)]
    c = expire_devices(nd, cands, max_delete=2, archive=True, dry_run=False)
    assert c["deleted"] == 2 and c["capped"] == 1
    assert [j["ip"] for j in nd.jobs] == ["10.0.0.1", "10.0.0.2"] and all(j["archive"] for j in nd.jobs)
    assert nd.jobs[0]["log"] == "discobox expire: r"
