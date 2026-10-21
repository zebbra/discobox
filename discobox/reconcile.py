"""Reconcile Netbox with Netdisco: gaps, discover jobs, auto-create, liveness, tag fixes."""

import ipaddress
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

from .netbox import NetboxClient
from .netdisco import NetdiscoClient
from .util import discover_snmptimeout_us, vendor_from_chassis


def _slugify(text: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _resolve_location_chain(
    nb: "NetboxClient", fallback_site_id: int, location_str: str
) -> tuple[int, Optional[int]]:
    """
    Parse a comma-separated SNMP location string.

    If the first part matches an existing NetBox site slug, that site is used
    and the remaining parts become the location chain.  Otherwise all parts are
    treated as locations under fallback_site_id.

    Returns (site_id, deepest_location_id).  deepest_location_id is None when
    no location parts remain or all fail.
    """
    log = logging.getLogger("discobox.reconcile")
    SKIP = {"", "unknown", "-", "n/a", "na", "tbd"}
    parts = [p.strip() for p in location_str.split(",")]
    parts = [p for p in parts if p.lower() not in SKIP]
    if not parts:
        return fallback_site_id, None

    site_slug = _slugify(parts[0])
    matched_sites = list(nb.nb.dcim.sites.filter(slug=site_slug))
    if matched_sites:
        site_id = matched_sites[0].id
        log.debug("SNMP location: site %r (id=%s)", matched_sites[0].name, site_id)
    else:
        log.warning("SNMP location: site slug %r not found, falling back to configured site", site_slug)
        site_id = fallback_site_id
    parts = parts[1:]

    parent_id: Optional[int] = None
    location_id: Optional[int] = None

    for part in parts:
        slug = _slugify(part)
        filter_kwargs: dict = {"site_id": site_id, "slug": slug}
        if parent_id is not None:
            filter_kwargs["parent_id"] = parent_id
        else:
            filter_kwargs["parent_id__isnull"] = True

        existing = list(nb.nb.dcim.locations.filter(**filter_kwargs))
        if existing:
            loc = existing[0]
        else:
            create_kwargs: dict = {"name": part, "slug": slug, "site": site_id}
            if parent_id is not None:
                create_kwargs["parent"] = parent_id
            try:
                loc = nb.nb.dcim.locations.create(**create_kwargs)
                log.debug("Auto-create location: %r (slug=%s, parent_id=%s)", part, slug, parent_id)
            except Exception as exc:
                log.warning("Auto-create location %r failed: %s — stopping chain", part, exc)
                break

        parent_id = loc.id
        location_id = loc.id

    return site_id, location_id


def _create_device_from_nd(
    nb: "NetboxClient",
    nd: "NetdiscoClient",
    ip: str,
    nd_device: dict,
    role_slug: str,
    site_slug: str,
    status: str = "active",
    auto_create_location: bool = False,
    iface_source_cf: Optional[str] = None,
    iface_source_value: str = "netdisco",
) -> bool:
    """
    Bootstrap a new NetBox device from Netdisco discovery data.
    Creates the device, its management interface, and primary IP.
    Returns True on success (even if IP assignment partially fails).
    """
    log = logging.getLogger("discobox.reconcile")
    hostname = nd_device.get("name") or nd_device.get("dns") or ip

    roles = list(nb.nb.dcim.device_roles.filter(slug=role_slug))
    if not roles:
        log.error("Auto-create %s: role slug %r not found in NetBox", ip, role_slug)
        return False
    sites = list(nb.nb.dcim.sites.filter(slug=site_slug))
    if not sites:
        log.error("Auto-create %s: site slug %r not found in NetBox", ip, site_slug)
        return False

    try:
        nd_mods = nd.get_modules(ip)
    except Exception as exc:
        log.warning("Auto-create %s: could not fetch modules: %s: using description as model", ip, exc)
        nd_mods = []
    chassis = [m for m in nd_mods if m.get("class") == "chassis" and m.get("model")]
    primary_ch = chassis[0] if chassis else {}
    vendor_name = vendor_from_chassis(primary_ch) if primary_ch else None
    vendor_name = vendor_name or nd_device.get("vendor") or None
    model = primary_ch.get("model") or nd_device.get("model") or nd_device.get("description") or "Unknown"
    serial = primary_ch.get("serial") or nd_device.get("serial") or ""

    try:
        # Name, not Record: an aliased type brings its own manufacturer, so
        # "Unknown" is only created if a new type actually needs it
        device_type = nb.get_or_create_device_type(
            vendor_name or "Unknown", model, part_number=model, vendor=nd_device.get("vendor"),
        )
    except Exception as exc:
        log.error("Auto-create %s: device type %r/%r failed: %s", ip, vendor_name, model, exc)
        return False
    if not device_type:
        log.warning("Auto-create %s: no DeviceType for %r/%r, device not created", ip, vendor_name, model)
        return False

    resolved_site_id = sites[0].id
    location_id: Optional[int] = None
    if auto_create_location:
        nd_location = nd_device.get("location") or ""
        if nd_location:
            resolved_site_id, location_id = _resolve_location_chain(nb, sites[0].id, nd_location)

    existing = list(nb.nb.dcim.devices.filter(name=hostname, site_id=resolved_site_id))
    if existing:
        log.debug("Auto-create %s: device %r already exists, skipping", ip, hostname)
        nb_device = existing[0]
    else:
        try:
            create_kwargs: dict = {
                "name": hostname,
                "device_type": device_type.id,
                "role": roles[0].id,
                "site": resolved_site_id,
                "status": status,
            }
            if serial:
                create_kwargs["serial"] = serial
            if location_id is not None:
                create_kwargs["location"] = location_id
            if iface_source_cf:
                create_kwargs["custom_fields"] = {iface_source_cf: iface_source_value}
            nb_device = nb.nb.dcim.devices.create(**create_kwargs)
        except Exception as exc:
            log.error("Auto-create %s: device create failed: %s", ip, exc)
            return False

    log.info("Auto-create: created %r (%s)  type=%s/%s", hostname, ip, vendor_name or "?", model)

    # Find management interface name and address from device_ips
    try:
        nd_ips_list = nd.get_device_ips(ip)
    except Exception as exc:
        log.warning("Auto-create %s: could not fetch device_ips: %s", ip, exc)
        nd_ips_list = []

    mgmt_entry = next(
        (e for e in nd_ips_list if (e.get("ip") or "").split("/")[0] == ip),
        None,
    )
    port_name = (mgmt_entry or {}).get("port") or "mgmt0"
    subnet = (mgmt_entry or {}).get("subnet")
    address = ip
    if "/" not in address and subnet:
        try:
            prefix = ipaddress.ip_network(subnet, strict=False).prefixlen
            address = f"{ip}/{prefix}"
        except ValueError:
            address = f"{ip}/32"

    try:
        iface_kwargs: dict = {"device": nb_device.id, "name": port_name, "type": "1000base-t"}
        iface = nb.nb.dcim.interfaces.create(**iface_kwargs)
        if iface_source_cf:
            iface.update({"custom_fields": {iface_source_cf: iface_source_value}})
    except Exception as exc:
        log.error("Auto-create %s: interface %r create failed: %s", ip, port_name, exc)
        return True

    try:
        nb.upsert_ip(address, iface)
        ip_objs = list(nb.nb.ipam.ip_addresses.filter(address=ip))
        if ip_objs:
            nb_device.update({"primary_ip4": ip_objs[0].id})
            log.info("Auto-create: IP %s → %r set as primary on %r", address, port_name, hostname)
    except Exception as exc:
        log.error("Auto-create %s: IP assignment failed: %s", ip, exc)

    return True


def fetch_liveness(
    url: str,
    query: str,
    label: str = "netbox_primary_ip",
    timeout: int = 15,
    verify_tls: bool = True,
) -> dict[str, bool]:
    """
    Run an instant PromQL query against a Prometheus-compatible API
    (e.g. VictoriaMetrics vmselect, URL including any tenant prefix such
    as /select/0/prometheus) and map each series' `label` value to a
    liveness verdict: True when the sample value is > 0.

    A device with multiple series counts as up if any of them is > 0.
    Label values carrying a CIDR suffix are normalized to the bare IP.

    Raises on HTTP or payload errors — the caller decides fail-open behavior.
    """
    resp = requests.get(
        f"{url.rstrip('/')}/api/v1/query",
        params={"query": query},
        timeout=timeout,
        verify=verify_tls,
        headers={"Accept": "application/json", "User-Agent": "discobox"},
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"liveness query failed: {payload.get('error') or 'unknown error'}")

    liveness: dict[str, bool] = {}
    for series in payload.get("data", {}).get("result", []):
        key = str(series.get("metric", {}).get(label) or "").split("/")[0]
        if not key:
            continue
        try:
            alive = float(series["value"][1]) > 0
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        liveness[key] = liveness.get(key, False) or alive
    return liveness


def _netdisco_queue_ok(
    nd: "NetdiscoClient",
    max_queued: Optional[int],
    max_failed: Optional[int],
    log: logging.Logger,
    action: str,
) -> bool:
    """Check Netdisco's job queue (last 1h) against the given thresholds. None disables a check."""
    if max_queued is None and max_failed is None:
        return True
    try:
        qs = nd.get_queue_status(since="1h")
    except requests.HTTPError as exc:
        log.error("%s aborted: could not fetch Netdisco queue status: %s", action, exc)
        return False
    queued, failed = qs.get("queued", 0), qs.get("failed", 0)
    if (max_queued is not None and queued > max_queued) or \
       (max_failed is not None and failed > max_failed):
        log.warning(
            "%s aborted: queue too busy (queued=%d failed=%d, limits queued=%s failed=%s)",
            action, queued, failed, max_queued, max_failed,
        )
        return False
    return True


def fix_tag_mismatches(
    nd: "NetdiscoClient",
    mismatches: list[dict],
    max_queued: Optional[int] = 500,
    max_failed: Optional[int] = 500,
    max_enqueue: Optional[int] = None,
) -> dict:
    """
    Re-enqueue Netdisco discovery, hinting the Netbox-expected auth tag, for
    devices from reconcile's tag-mismatch report (see reconcile_devices).
    Entries with no Netbox tag to hint toward (reason="missing" and Netbox
    itself has no snmp_auth_profile set) are skipped since there's nothing to
    correct toward.

    This does not re-derive the mismatch list itself — pass in the list from
    a prior reconcile run (e.g. counts["tag_mismatches_list"]).
    """
    log = logging.getLogger("discobox.reconcile")
    result = {"aborted": False, "enqueued": 0, "skipped": 0, "errors": 0}

    if not _netdisco_queue_ok(nd, max_queued, max_failed, log, "Fix tag mismatches"):
        result["aborted"] = True
        return result

    for entry in mismatches:
        nb_tag = entry.get("netbox_tag")
        if not nb_tag:
            result["skipped"] += 1
            continue
        if max_enqueue is not None and result["enqueued"] >= max_enqueue:
            result["skipped"] += 1
            continue

        ip = entry["ip"]
        try:
            nd.enqueue_discover(
                ip, device_auth_tag_hint=nb_tag,
                snmp_timeout_us=discover_snmptimeout_us(entry.get("snmp_polling_timeout")),
            )
            log.info("Re-enqueued discover for %s (%s) to fix auth tag: %r -> %r",
                      ip, entry.get("name"), entry.get("netdisco_tag"), nb_tag)
            result["enqueued"] += 1
        except Exception as exc:
            log.error("Failed to enqueue discover for %s: %s", ip, exc)
            result["errors"] += 1

    return result


def _parse_nd_time(value) -> Optional[datetime]:
    """Netdisco timestamp ('2026-09-24 16:31:20.9008', no zone) → naive datetime, None if unparsable."""
    if not value:
        return None
    try:
        # an offset, if any, is dropped: values are only compared with each other
        return datetime.fromisoformat(str(value).replace(" ", "T", 1)).replace(tzinfo=None)
    except ValueError:
        return None


def _stale_cutoff(nd_devices: list[dict], hours: Optional[float]) -> Optional[datetime]:
    """
    Discoveries older than this count as stale: `hours` before Netdisco's newest
    last_discover rather than before now, so the (zone-less) Netdisco clock is
    never compared with ours, and a Netdisco whose discovery stopped altogether
    doesn't flag its whole inventory. None when off or no device has a timestamp.
    """
    if not hours:
        return None
    newest = max(filter(None, (_parse_nd_time(d.get("last_discover")) for d in nd_devices)), default=None)
    return newest - timedelta(hours=hours) if newest else None


def reconcile_devices(
    nd: "NetdiscoClient",
    nb: "NetboxClient",
    max_queued: Optional[int] = 500,
    max_failed: Optional[int] = 500,
    max_enqueue: Optional[int] = None,
    offset: Optional[int] = None,
    roles: Optional[list] = None,  # empty/None = all roles
    statuses: Optional[list] = None,  # empty/None = ["active"]
    require_auth_tag: bool = False,
    auto_create_role: Optional[str] = None,
    auto_create_site: Optional[str] = None,
    auto_create_status: str = "active",
    auto_create_location: bool = False,
    iface_source_cf: Optional[str] = None,
    iface_source_value: str = "netdisco",
    liveness: Optional[dict] = None,   # from fetch_liveness(); None = no filtering
    liveness_key: str = "ip",          # device attribute matched against liveness keys: ip | name
    not_in_netbox_stale_hours: Optional[float] = 24,   # see _stale_cutoff; None/0 = off
) -> dict:
    """
    Compare Netbox active devices against Netdisco and enqueue discovery
    for any device present in Netbox but missing from Netdisco.

    If auto_create_role and auto_create_site are set, also creates NetBox
    devices for any Netdisco device not yet present in NetBox.

    If the Netdisco queue exceeds max_queued or max_failed (last 1h), the
    enqueue step is skipped to avoid overloading Netdisco.

    max_enqueue=0 disables enqueueing entirely (no discovery jobs are ever
    submitted) while still running the full not_in_netdisco/not_in_netbox
    gap scan — use this to monitor drift without triggering discovery.

    When `liveness` is given, each missing device gets a status (up/down/
    unknown) in the gap list and devices reported down are not enqueued —
    a dead device would only produce a failed Netdisco discovery job and
    burn max_failed budget. Devices absent from the liveness data count as
    "unknown" and are still enqueued (fail-open: not-yet-monitored gear
    must remain discoverable).

    Netdisco devices missing from Netbox whose last discovery is more than
    not_in_netbox_stale_hours older than Netdisco's newest one are gone (or
    unreachable): they stay in the gap list marked "stale": true, but don't count
    in not_in_netbox and are never auto-created.

    Returns counts: enqueued / skipped (no primary IP) / skipped_offline /
                    already_known / netbox_total / netdisco_total / aborted (bool).
    """
    log = logging.getLogger("discobox.reconcile")
    _aborted = {"aborted": True, "enqueued": 0, "skipped": 0, "already_known": 0, "netbox_total": 0, "netdisco_total": 0}

    if max_queued is not None or max_failed is not None:
        try:
            qs = nd.get_queue_status(since="1h")
        except requests.HTTPError as exc:
            log.error("Reconcile aborted: could not fetch Netdisco queue status: %s", exc)
            return _aborted
        queued, failed = qs.get("queued", 0), qs.get("failed", 0)
        if (max_queued is not None and queued > max_queued) or \
           (max_failed is not None and failed > max_failed):
            log.warning(
                "Reconcile aborted: queue too busy (queued=%d failed=%d, limits queued=%s failed=%s)",
                queued, failed, max_queued, max_failed,
            )
            return _aborted

    try:
        nd_all_devices = nd.get_all_devices()
        nd_by_ip = {d["ip"]: d for d in nd_all_devices if d.get("ip")}
    except requests.HTTPError as exc:
        log.error("Reconcile aborted: could not fetch Netdisco devices: %s", exc)
        return _aborted
    log.info("Netdisco knows %d devices", len(nd_by_ip))

    role_filter = {"role": roles} if roles else {}
    status_filter = statuses or ["active"]
    nb_total = nb.nb.dcim.devices.count(status=status_filter, has_primary_ip=True, **role_filter)
    counts = {"enqueued": 0, "skipped": 0, "skipped_offline": 0, "already_known": 0, "netdisco_total": len(nd_by_ip), "netbox_total": nb_total}
    if roles:
        log.info("Reconcile role filter: %s", roles)
    if statuses:
        log.info("Reconcile status filter: %s", statuses)

    not_in_netdisco: list[dict] = []
    tag_mismatches: list[dict] = []
    enqueue_cap_logged = False
    for device in nb.nb.dcim.devices.filter(status=status_filter, has_primary_ip=True, **role_filter):
        primary = device.primary_ip4
        if not primary:
            counts["skipped"] += 1
            continue
        ip = str(primary).split("/")[0]

        if ip in nd_by_ip:
            counts["already_known"] += 1
            cf = getattr(device, "custom_fields", {}) or {}
            nb_tag = cf.get("snmp_auth_profile") or None
            nd_tag = nd_by_ip[ip].get("device_auth_tag") or None
            # carried along so fix-tags' re-discover uses the device's own SNMP timeout
            snmp_timeout = cf.get("snmp_polling_timeout") or None
            if not nd_tag:
                tag_mismatches.append({"ip": ip, "name": device.name, "netbox_tag": nb_tag, "netdisco_tag": nd_tag,
                                       "reason": "missing", "snmp_polling_timeout": snmp_timeout})
            elif nb_tag and nd_tag != nb_tag:
                tag_mismatches.append({"ip": ip, "name": device.name, "netbox_tag": nb_tag, "netdisco_tag": nd_tag,
                                       "reason": "mismatch", "snmp_polling_timeout": snmp_timeout})
            continue

        entry = {"ip": ip, "name": device.name}
        if liveness is not None:
            alive = liveness.get(ip if liveness_key == "ip" else str(device.name or ""))
            entry["status"] = "unknown" if alive is None else ("up" if alive else "down")
        not_in_netdisco.append(entry)

        if entry.get("status") == "down":
            counts["skipped_offline"] += 1
            log.debug("Skipping %s (%s): liveness reports down", ip, device.name)
            continue

        cf = getattr(device, "custom_fields", {}) or {}
        device_auth_tag_hint = cf.get("snmp_auth_profile") or None
        if require_auth_tag and not device_auth_tag_hint:
            log.debug("Skipping %s (%s): no snmp_auth_profile set", ip, device.name)
            counts["skipped"] += 1
            continue
        snmp_timeout_us = discover_snmptimeout_us(cf.get("snmp_polling_timeout"))

        if offset and counts["enqueued"] + counts.get("offset_skipped", 0) < offset:
            counts["offset_skipped"] = counts.get("offset_skipped", 0) + 1
            continue

        if max_enqueue is not None and counts["enqueued"] >= max_enqueue:
            if not enqueue_cap_logged:
                enqueue_cap_logged = True
                log.info(
                    "Enqueue cap reached (%d): skipping further enqueues this run "
                    "(gap scan continues)", max_enqueue,
                )
            continue

        try:
            nd.enqueue_discover(ip, device_auth_tag_hint=device_auth_tag_hint, snmp_timeout_us=snmp_timeout_us)
            effective_timeout = snmp_timeout_us
            log.info("Enqueued discover for %s (%s) device_auth_tag_hint=%r timeout=%dus",
                     ip, device.name, device_auth_tag_hint, effective_timeout)
            counts["enqueued"] += 1
        except Exception as exc:
            log.error("Failed to enqueue discover for %s: %s", ip, exc)

    # Build full nb_all_ips across all statuses/roles for gap reporting and auto-create.
    # Deliberately NOT role_filter-scoped: the enqueue loop above only targets
    # reconcile's target roles, but a device under any other role still counts
    # as "in Netbox" for gap-reporting purposes.
    nb_all_ips = {
        str(d.primary_ip4).split("/")[0]
        for d in nb.nb.dcim.devices.filter(primary_ip4__isnull=False)
        if d.primary_ip4
    }
    cutoff = _stale_cutoff(nd_all_devices, not_in_netbox_stale_hours)
    not_in_netbox: list[dict] = []
    for d in nd_all_devices:
        if not d.get("ip") or d["ip"] in nb_all_ips:
            continue
        seen = _parse_nd_time(d.get("last_discover"))
        not_in_netbox.append({
            "ip": d["ip"], "name": d.get("name") or d.get("dns") or d["ip"],
            "last_discover": seen.strftime("%Y-%m-%d %H:%M") if seen else None,
            "stale": cutoff is not None and (seen is None or seen < cutoff),
        })
    stale_ips = {e["ip"] for e in not_in_netbox if e["stale"]}
    # Devices liveness reports down are expected to be missing from Netdisco
    # (discovery can't reach them): keep them in the list, not in the count.
    counts["not_in_netdisco"] = sum(1 for e in not_in_netdisco if e.get("status") != "down")
    counts["not_in_netbox"] = len(not_in_netbox) - len(stale_ips)
    counts["not_in_netbox_stale"] = len(stale_ips)
    counts["tag_mismatches"] = len(tag_mismatches)
    if not_in_netdisco or not_in_netbox:
        log.info("Gaps: not_in_netdisco=%d (+%d offline)  not_in_netbox=%d (+%d stale)",
                 counts["not_in_netdisco"], counts["skipped_offline"],
                 counts["not_in_netbox"], counts["not_in_netbox_stale"])
    if tag_mismatches:
        log.info("Auth tag mismatches: %d", len(tag_mismatches))

    # Auto-create: bootstrap NetBox devices for Netdisco entries not yet in NetBox
    if auto_create_role and auto_create_site:
        create_counts: dict[str, int] = {"created": 0, "failed": 0}
        for nd_dev in nd_all_devices:
            nd_ip = nd_dev.get("ip")
            if not nd_ip or nd_ip in nb_all_ips or nd_ip in stale_ips:
                continue
            try:
                nd_device_full = nd.get_device(nd_ip)
            except Exception as exc:
                log.error("Auto-create %s: could not fetch device info: %s", nd_ip, exc)
                create_counts["failed"] += 1
                continue
            ok = _create_device_from_nd(
                nb, nd, nd_ip, nd_device_full,
                role_slug=auto_create_role,
                site_slug=auto_create_site,
                status=auto_create_status,
                auto_create_location=auto_create_location,
                iface_source_cf=iface_source_cf,
                iface_source_value=iface_source_value,
            )
            create_counts["created" if ok else "failed"] += 1
        if any(create_counts.values()):
            log.info("Auto-create: %s", "  ".join(f"{k}: {v}" for k, v in create_counts.items() if v))
        counts.update(create_counts)

    log.info("Reconcile done: %s", counts)
    counts["not_in_netdisco_list"] = not_in_netdisco
    counts["not_in_netbox_list"] = not_in_netbox
    counts["tag_mismatches_list"] = tag_mismatches
    return counts
