"""refresh-ip: name resolution (DNS, Netdisco fallback), IP placement, and the endpoint's validation."""
from __future__ import annotations

import os
import sys
import tempfile
from types import SimpleNamespace as NS

os.environ.setdefault("DISCOBOX_STATE_DIR", tempfile.mkdtemp(prefix="discobox-test-"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import server  # noqa: E402
from discobox.refresh_ip import Resolver, refresh_primary_ip, refresh_primary_ips  # noqa: E402

DNS = {"sw1.net.example.com.": ["192.0.2.10"], "multi.example.com.": ["192.0.2.1", "192.0.2.2"]}


def _resolver(domains=("net.example.com",), nd_devices=None) -> Resolver:
    nd = NS(get_all_devices=lambda: nd_devices) if nd_devices is not None else None
    return Resolver(list(domains), nd, lookup=lambda fqdn: DNS.get(fqdn, []))


def test_bare_name_only_with_domains_dotted_also_as_is() -> None:
    r = _resolver(["net.example.com", "example.org"])
    assert r.candidates("sw1") == ["sw1.net.example.com.", "sw1.example.org."]
    assert r.candidates("sw1.zrh") == ["sw1.zrh.", "sw1.zrh.net.example.com.", "sw1.zrh.example.org."]
    assert r.candidates("sw1.net.example.com") == ["sw1.net.example.com.", "sw1.net.example.com.example.org."]
    assert _resolver([]).candidates("sw1") == ["sw1."]


def test_resolve_dns_then_netdisco() -> None:
    nd = [{"ip": "192.0.2.20", "name": "SW2.other.example", "dns": None},
          {"ip": "192.0.2.21", "name": "sw3", "dns": "sw3.a"}, {"ip": "192.0.2.22", "name": "sw3", "dns": "sw3.b"}]
    r = _resolver(nd_devices=nd)
    assert r.resolve("sw1") == ("192.0.2.10", "dns", "sw1.net.example.com")
    assert r.resolve("sw2") == ("192.0.2.20", "netdisco", "sw2")          # short-name match
    assert r.resolve("sw3")[2] == "not in DNS, several Netdisco devices by that name"
    assert r.resolve("sw9")[2] == "not in DNS, not in Netdisco"
    assert "several addresses" in r.resolve("multi.example.com")[2]
    assert _resolver().resolve("sw9") == (None, None, "not in DNS")


class _Rec(NS):
    def update(self, patch: dict) -> None:
        self.__dict__.setdefault("updates", []).append(patch)


class _EP:
    def __init__(self, records=()):
        self.records, self.created = list(records), []

    def filter(self, **kw):
        return list(self.records)

    def create(self, **kw):
        self.created.append(kw)
        rec = _Rec(id=900 + len(self.created), **kw)
        self.records.append(rec)
        return rec


def _nb(ips=(), ifaces=(), prefixes=()):
    return NS(nb=NS(
        ipam=NS(ip_addresses=_EP(ips), prefixes=_EP(prefixes)),
        dcim=NS(interfaces=_EP(ifaces)),
    ))


def _dev(primary=None):
    return _Rec(id=1, name="sw1", primary_ip4=NS(address=primary) if primary else None)


def test_dry_run_plans_new_address_on_dummy_interface_and_writes_nothing() -> None:
    nb, dev = _nb(prefixes=[NS(prefix="192.0.2.0/24"), NS(prefix="192.0.0.0/16")]), _dev()
    item = refresh_primary_ip(nb, dev, _resolver())
    assert item == {"id": 1, "device": "sw1", "old": None, "new": "192.0.2.10", "source": "dns",
                    "resolved": "sw1.net.example.com", "interface": "main", "interface_created": True,
                    "action": "set", "address": "192.0.2.10/24", "address_created": True}
    assert not nb.nb.dcim.interfaces.created and not nb.nb.ipam.ip_addresses.created and not hasattr(dev, "updates")


def test_apply_creates_interface_and_address_and_sets_primary() -> None:
    nb, dev = _nb(), _dev()
    item = refresh_primary_ip(nb, dev, _resolver(), dry_run=False, source_cf="source")
    assert item["applied"] and item["address"] == "192.0.2.10/32"
    assert nb.nb.dcim.interfaces.created == [
        {"device": 1, "name": "main", "type": "other", "custom_fields": {"source": "netdisco"}}]
    assert nb.nb.ipam.ip_addresses.created[0]["assigned_object_id"] == 901
    assert dev.updates == [{"primary_ip4": 901}]


def test_address_already_on_the_device_only_becomes_primary_also_over_a_differing_one() -> None:
    ip = _Rec(id=5, address="192.0.2.10/26", assigned_object_type="dcim.interface",
              assigned_object=NS(name="Vlan10", device=NS(id=1, name="sw1")))
    nb, dev = _nb(ips=[ip]), _dev(primary="192.0.2.99/24")
    item = refresh_primary_ip(nb, dev, _resolver(), dry_run=False)
    assert (item["action"], item["old"], item["interface"], item["address"]) == ("replace", "192.0.2.99", "Vlan10", "192.0.2.10/26")
    assert dev.updates == [{"primary_ip4": 5}] and not hasattr(ip, "updates") and not nb.nb.dcim.interfaces.created


def test_unassigned_address_goes_on_an_existing_dummy_interface() -> None:
    ip = _Rec(id=5, address="192.0.2.10/24", assigned_object_type=None, assigned_object=None)
    nb, dev = _nb(ips=[ip], ifaces=[NS(id=7, name="Gi1/0/1"), NS(id=8, name="mgmt0")]), _dev()
    item = refresh_primary_ip(nb, dev, _resolver(), dry_run=False)
    assert item["interface"] == "mgmt0" and "interface_created" not in item
    assert ip.updates == [{"assigned_object_type": "dcim.interface", "assigned_object_id": 8}]


def test_unchanged_foreign_address_and_unresolvable_are_reported_not_written() -> None:
    assert refresh_primary_ip(_nb(), _dev(primary="192.0.2.10/24"), _resolver())["action"] == "unchanged"
    ip = _Rec(id=5, address="192.0.2.10/24", assigned_object_type="dcim.interface",
              assigned_object=NS(name="Vlan10", device=NS(id=2, name="sw-other")))
    dev = _dev()
    item = refresh_primary_ip(_nb(ips=[ip]), dev, _resolver(), dry_run=False)
    assert item["action"] == "error" and item["error"] == "192.0.2.10 is assigned to sw-other (Vlan10)"
    assert not hasattr(dev, "updates")
    result = refresh_primary_ips(_nb(), [_Rec(id=3, name="sw9", primary_ip4=None), _dev()], _resolver())
    assert result["summary"] == {"devices": 2, "set": 1, "replace": 0, "unchanged": 0, "error": 1}
    assert result["devices"][0]["error"] == "not in DNS"


# ── endpoint ────────────────────────────────────────────────────────────────────

def _req(method="GET", **params):
    items = [(k, v) for k, vs in params.items() for v in (vs if isinstance(vs, list) else [vs])]
    return NS(method=method, query_params=NS(multi_items=lambda: items))


@pytest.fixture
def seen(monkeypatch) -> dict:
    out: dict = {}
    devices = {1: NS(id=1, name="sw1"), 2: NS(id=2, name="sw2")}

    def _filter(**kw):
        out["filter"] = kw
        return [devices[1]] if "name__ie" in kw else list(devices.values())

    nb = NS(nb=NS(dcim=NS(devices=NS(get=devices.get, filter=_filter))))
    monkeypatch.setattr(server, "_get_netbox_client", lambda: nb)
    monkeypatch.setattr(server, "_get_netdisco_client", lambda: None)
    monkeypatch.setattr(server, "refresh_primary_ips",
                        lambda nb, devs, resolver, dry_run, **kw: out.update(ids=[d.id for d in devs], dry_run=dry_run) or {})
    return out


def test_endpoint_selects_by_id_name_and_filter(seen) -> None:
    server.refresh_ip(_req(device=["2", "sw1"]), device=["2", "sw1"], dry_run=True)
    assert seen["ids"] == [2, 1] and seen["dry_run"] is True
    server.refresh_ip(_req("POST", has_primary_ip="false", role=["switch", "router"], dry_run="false"), device=None, dry_run=False)
    assert seen["filter"] == {"has_primary_ip": "false", "role": ["switch", "router"]} and seen["dry_run"] is False


def test_endpoint_rejects_write_by_get_empty_selection_and_unknown_device(seen) -> None:
    for req, kw, code in [
        (_req(device="1", dry_run="false"), {"device": ["1"], "dry_run": False}, 405),
        (_req(), {"device": None, "dry_run": True}, 400),
        (_req(device="99"), {"device": ["99"], "dry_run": True}, 404),
    ]:
        with pytest.raises(HTTPException) as exc:
            server.refresh_ip(req, **kw)
        assert exc.value.status_code == code
    assert "ids" not in seen
