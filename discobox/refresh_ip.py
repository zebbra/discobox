"""
Refresh a Netbox device's primary IPv4 from its name: DNS first, then the
device Netdisco knows by that name. For devices that lost (or never had) a
primary IP, which nothing else can reach: every sync starts from it.

The address is reused where it already sits on one of the device's
interfaces; otherwise it goes onto a dummy interface ("main", created when
missing) and the next sync moves it to the real port. An address held by
another device is only taken over on request (move), one device at a time.
"""
from __future__ import annotations

import ipaddress
import logging
import socket
from typing import Callable, Optional

from .netbox import NetboxClient
from .netdisco import NetdiscoClient
from .util import DUMMY_INTERFACES

log = logging.getLogger("discobox.refresh_ip")

DUMMY_INTERFACE = "main"


def _dns_a(fqdn: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(fqdn, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except (socket.gaierror, UnicodeError):
        return []
    return sorted({info[4][0] for info in infos})


class Resolver:
    """Device name → (ip, source, detail). ip None: detail says why."""

    def __init__(
        self, domains: Optional[list[str]] = None, nd: Optional[NetdiscoClient] = None,
        lookup: Callable[[str], list[str]] = _dns_a,
    ):
        self.domains = [d.strip(".") for d in domains or [] if d and d.strip(".")]
        self.nd = nd
        self.lookup = lookup
        self._nd_devices: Optional[list[dict]] = None

    def candidates(self, name: str) -> list[str]:
        """
        FQDNs to try. A dotted name is asked as-is, absolute, so the system
        search list can't expand it; a bare one only with the configured
        domains (in a cluster it could match a service), as-is without any.
        """
        name = name.strip().rstrip(".")
        out = [f"{name}."] if "." in name or not self.domains else []
        out += [f"{name}.{d}." for d in self.domains if not name.lower().endswith(f".{d.lower()}")]
        return out

    def resolve(self, name: str) -> tuple[Optional[str], Optional[str], str]:
        for fqdn in self.candidates(name):
            ips = self.lookup(fqdn)
            if len(ips) == 1:
                return ips[0], "dns", fqdn.rstrip(".")
            if ips:
                return None, None, f"DNS: {fqdn.rstrip('.')} has several addresses ({', '.join(ips)})"
        if self.nd is None:
            return None, None, "not in DNS"
        try:
            hits = self._netdisco(name)
        except Exception as exc:
            return None, None, f"not in DNS, Netdisco lookup failed: {exc}"
        if len(hits) == 1:
            return hits[0], "netdisco", name
        return None, None, "not in DNS, " + ("several Netdisco devices by that name" if hits else "not in Netdisco")

    def _netdisco(self, name: str) -> list[str]:
        if self._nd_devices is None:           # once per request, only when DNS misses
            self._nd_devices = self.nd.get_all_devices()
        want = name.strip().rstrip(".").lower()

        def ips(key: Callable[[str], str]) -> list[str]:
            return sorted({
                d["ip"] for d in self._nd_devices
                if d.get("ip") and key(want) in {key(str(d.get(f) or "").rstrip(".").lower()) for f in ("name", "dns")} - {""}
            })
        return ips(lambda s: s) or ips(lambda s: s.split(".", 1)[0])


def _host(address: object) -> str:
    return str(address or "").split("/")[0]


def _prefix_len(nb: NetboxClient, ip: str) -> int:
    """Mask of the smallest Netbox prefix containing ip; 32 when none does."""
    lengths = []
    for p in nb.nb.ipam.prefixes.filter(contains=ip):
        try:
            lengths.append(ipaddress.ip_network(str(p.prefix), strict=False).prefixlen)
        except ValueError:
            pass
    return max(lengths, default=32)


def refresh_primary_ip(
    nb: NetboxClient, device, resolver: Resolver, dry_run: bool = True,
    source_cf: Optional[str] = None, source_value: str = "netdisco", move: bool = False,
) -> dict:
    """
    One device. action: set (had none) | replace (differed) | unchanged |
    move | error. With dry_run nothing is written; otherwise `applied` is set.

    move: an address on another device's interface is taken over (a cluster
    or stack with the IP on the wrong member) instead of being an error; that
    device loses its primary IP if this was it.
    """
    old = _host(getattr(getattr(device, "primary_ip4", None), "address", None)) or None
    item: dict = {"id": device.id, "device": device.name, "old": old, "new": None}

    def fail(why: str) -> dict:
        item.update(action="error", error=why)
        return item

    if not device.name:
        return fail("device has no name")
    ip, source, detail = resolver.resolve(device.name)
    if not ip:
        return fail(detail)
    item.update(new=ip, source=source, resolved=detail)
    if ip == old:
        item["action"] = "unchanged"
        return item

    ip_obj = next((o for o in nb.nb.ipam.ip_addresses.filter(address=ip) if _host(o.address) == ip), None)
    iface = None
    from_dev = None
    owner = getattr(ip_obj, "assigned_object", None) if ip_obj is not None else None
    owner_dev = getattr(owner, "device", None)
    if owner and (ip_obj.assigned_object_type != "dcim.interface" or getattr(owner_dev, "id", None) != device.id):
        if not move or ip_obj.assigned_object_type != "dcim.interface" or owner_dev is None:
            holder = getattr(owner_dev, "name", None) or getattr(getattr(owner, "virtual_machine", None), "name", None)
            return fail(f"{ip} is assigned to {holder or ip_obj.assigned_object_type} ({getattr(owner, 'name', owner)})")
        # the nested device carries no primary_ip4: fetch it
        from_dev = nb.nb.dcim.devices.get(owner_dev.id)
        item.update(
            from_device=owner_dev.name, from_device_id=owner_dev.id,
            from_interface=owner.name, from_interface_id=owner.id,
            from_primary=getattr(getattr(from_dev, "primary_ip4", None), "id", None) == ip_obj.id,
        )
    if owner and from_dev is None:
        item["interface"] = owner.name
    else:
        dummies = {d.lower() for d in DUMMY_INTERFACES}
        ifaces = [i for i in nb.nb.dcim.interfaces.filter(device_id=device.id) if (i.name or "").lower() in dummies]
        iface = next((i for i in ifaces if i.name.lower() == DUMMY_INTERFACE), ifaces[0] if ifaces else None)
        item["interface"] = iface.name if iface else DUMMY_INTERFACE
        if iface is None:
            item["interface_created"] = True
    item["action"] = "move" if from_dev is not None else "replace" if old else "set"
    if ip_obj is not None:
        item["address"] = str(ip_obj.address)
    else:
        item["address"] = f"{ip}/{_prefix_len(nb, ip)}"
        item["address_created"] = True
    if dry_run:
        return item

    try:
        if "interface_created" in item:
            iface = nb.nb.dcim.interfaces.create(
                device=device.id, name=DUMMY_INTERFACE, type="other",
                **({"custom_fields": {source_cf: source_value}} if source_cf else {}),
            )
        if ip_obj is None:
            ip_obj = nb.nb.ipam.ip_addresses.create(
                address=item["address"], status="active",
                assigned_object_type="dcim.interface", assigned_object_id=iface.id,
            )
        elif iface is not None:
            if item.get("from_primary"):
                # Netbox refuses to reassign an address that is a device's primary
                from_dev.update({"primary_ip4": None})
            ip_obj.update({"assigned_object_type": "dcim.interface", "assigned_object_id": iface.id})
        device.update({"primary_ip4": ip_obj.id})
    except Exception as exc:
        log.error("refresh-ip %s: %s", device.name, exc)
        return fail(str(exc))
    item["applied"] = True
    log.info("refresh-ip %s: primary IPv4 %s → %s (%s, on %s)%s", device.name, old, item["address"], source,
             item["interface"], f", moved from {item['from_device']} ({item['from_interface']})" if from_dev is not None else "")
    return item


def _role_slug(device) -> str:
    role = getattr(device, "role", None) or getattr(device, "device_role", None)   # NetBox 4.x / 3.x
    return str(getattr(role, "slug", "") or "").lower()


def refresh_primary_ips(
    nb: NetboxClient, devices: list, resolver: Resolver, dry_run: bool = True,
    source_cf: Optional[str] = None, source_value: str = "netdisco",
    exclude_roles: Optional[list[str]] = None, named: Optional[set[int]] = None, move: bool = False,
) -> dict:
    """
    Devices of an excluded role (slug) are never resolved or written: one
    asked for by name/id (named) is reported as an error, one a filter
    pulled in is only counted.
    """
    blocked = {r.lower() for r in exclude_roles or []}
    items, excluded = [], 0
    for d in devices:
        if _role_slug(d) in blocked:
            excluded += 1
            if d.id in (named or ()):
                old = _host(getattr(getattr(d, "primary_ip4", None), "address", None)) or None
                items.append({"id": d.id, "device": d.name, "old": old, "new": None,
                              "action": "error", "error": f"role {_role_slug(d)} is excluded"})
            continue
        items.append(refresh_primary_ip(nb, d, resolver, dry_run, source_cf, source_value, move=move))
    summary = {"devices": len(items)}
    for action in ("set", "replace", "move", "unchanged", "error"):
        summary[action] = sum(1 for i in items if i["action"] == action)
    summary["excluded"] = excluded
    return {"dry_run": dry_run, "summary": summary, "devices": items}
