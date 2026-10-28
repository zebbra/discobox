#!/usr/bin/env python3
"""
discobox server: FastAPI webhook receiver + Prometheus metrics.

Netdisco calls POST /sync after each discovery job.
Syncs run in a background thread pool; duplicate requests for the
same host are dropped while a sync is already in progress.

Endpoints (see /docs):
  POST     /sync             Trigger a device sync (the Netdisco hook)
  POST     /sync/all         Queue a sync for every device known to Netdisco
  POST     /rebuild          Rebuild one device's inventory to exactly match Netdisco (deletes stale entries)
  POST     /sync/pause       Hold queued syncs from starting
  POST     /sync/resume      Release the pause gate
  POST     /discover         Enqueue a Netdisco discover for one device (Netbox SNMP profile/timeout)
  GET      /jobs             Jobs: gaps, discover-missing, fix-tags, types-bare
  POST     /jobs/<name>/run  Run a job now
  GET      /gaps             The last gaps result (Netbox ↔ Netdisco)
  GET/POST /types/library    Enrich DeviceTypes from the devicetype-library (dry-run unless POST apply=true)
  GET      /config, /stats, /metrics, /health
"""

import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, timedelta
from functools import partial
from pathlib import Path
from typing import Annotated, Any, Optional

import uvicorn
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    generate_latest,
)
from pydantic import BaseModel
from requests.exceptions import HTTPError, ReadTimeout

from config import ConfigError, effective, env_flag, load_settings
from discobox import (
    NetboxClient,
    NetdiscoClient,
    __version__,
    _parse_snmp_timeout_us,
    _recently_touched,
    configure_ap_fingerprint,
    configure_discover_snmptimeout,
    configure_read_fields,
    discover_missing,
    discover_snmptimeout_us,
    fetch_liveness,
    find_gaps,
    fix_tag_mismatches,
    read_cf,
    sync_device,
    validate_ip,
)
from jobs import Job, JobRegistry, JobResult
from metrics import (
    _custom_registry,
    aps_total,
    circuit_breaker_trips_total,
    deprecated_requests_total,
    device_orphans_braked,
    device_sync_duration,
    device_sync_errors,
    device_sync_failed,
    device_sync_timestamp,
    discover_missing_enqueued_total,
    fix_tags_enqueued_total,
    gaps_ip_mismatches,
    gaps_netbox_devices,
    gaps_netdisco_devices,
    gaps_not_in_netbox,
    gaps_not_in_netbox_stale,
    gaps_not_in_netdisco,
    gaps_offline,
    gaps_tag_mismatches,
    ha_vip_total,
    hooks_received_total,
    interface_defaults_total,
    interfaces_total,
    ips_total,
    liveness_query_up,
    modules_total,
    netbox_requests_total,
    netdisco_requests_total,
    orphans_total,
    rebuilds_total,
    sfps_total,
    sync_duration,
    sync_errors_total,
    sync_in_progress,
    sync_paused,
    sync_retries_total,
    sync_running,
    sync_timeouts_total,
    syncs_skipped_total,
    syncs_total,
    types_bare,
    unknown_devices_total,
)
from typesync import Library, match_library, sync_types

# ── Logging ────────────────────────────────────────────────────────────────────

_LOG_FMT = "%(asctime)s %(levelname)1.1s %(name)-12s %(message)s"
_LOG_DATE = "%Y-%m-%dT%H:%M:%S"

logging.basicConfig(level=logging.INFO, format=_LOG_FMT, datefmt=_LOG_DATE)
logger = logging.getLogger("discobox.server")


class _CapturingHandler(logging.Handler):
    """Collects log records into a list for returning in a debug response."""
    def __init__(self, level: int = logging.DEBUG):
        super().__init__(level)
        self.setFormatter(logging.Formatter("%(levelname)1.1s %(name)-16s %(message)s"))
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))

# Rename uvicorn.error → uvicorn (the name is misleading; it's their general logger)
logging.getLogger("uvicorn.error").name = "uvicorn"
# Suppress per-request access lines: sync results are logged by _run_sync
logging.getLogger("uvicorn.access").propagate = False

_UVICORN_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"discobox": {"format": _LOG_FMT, "datefmt": _LOG_DATE}},
    "handlers": {"default": {"class": "logging.StreamHandler", "formatter": "discobox"}},
    "loggers": {
        "uvicorn":        {"handlers": ["default"], "level": "INFO",    "propagate": False},
        "uvicorn.error":  {"handlers": ["default"], "level": "INFO",    "propagate": False},
        "uvicorn.access": {"handlers": [],          "level": "WARNING", "propagate": False},
    },
}

# ── Persistent state directory ──────────────────────────────────────────────────
# Writable directory for state that survives a restart: job results, gap
# reports, pause flag, sync cooldown markers, unknown devices, AP fingerprints.

_STATE_DIR = os.environ.get("DISCOBOX_STATE_DIR", "/tmp/discobox")
os.makedirs(_STATE_DIR, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────

try:
    _S, _CFG_SOURCE = load_settings()
except ConfigError as exc:
    logging.getLogger("discobox").error("%s", exc)
    raise SystemExit(2)
logging.getLogger("discobox").info("Config loaded from %s", _CFG_SOURCE)

# ── Auth ───────────────────────────────────────────────────────────────────────

_AUTH_TOKEN: Optional[str] = os.getenv("DISCOBOX_AUTH_TOKEN") or _S.auth.token
_METRICS_PATH: str = _S.auth.metrics_path

# ── Sync defaults (overridable per-request) ────────────────────────────────────

_SYNC_COOLDOWN: int       = _S.sync.cooldown  # seconds; 0 = disabled
_DEFAULT_MAC              = not _S.sync.no_mac
_DEFAULT_IP               = not _S.sync.no_ip
_DEFAULT_MODULES          = not _S.sync.no_modules
_DEFAULT_SFP              = not _S.sync.no_sfp
_DEFAULT_POE              = not _S.sync.no_poe
_DEFAULT_HOUSEKEEPING     = _S.sync.housekeeping
_DEFAULT_LLDP_CLEAR_STALE = _S.sync.lldp_clear_stale
_VIP_MODE: str            = _S.sync.vip_mode
# Per-vendor override, e.g. {fortinet: threenode} while _VIP_MODE stays the
# fallback for everything else (VSS, HSRP, ...). Keyed by nd_device["vendor"],
# which is already lowercased at the source (see tests/samples/*-device.json).
_VIP_MODE_BY_VENDOR: dict[str, str] = {k.lower(): v.lower() for k, v in _S.sync.vip_mode_by_vendor.items()}
_PAUSE_ON_ERROR: bool     = _S.sync.pause_on_error

# Circuit breaker: trip when >= _CB_THRESHOLD timeouts occur within _CB_WINDOW seconds.
# New syncs are held for _CB_BACKOFF seconds, then automatically released.
_CB_WINDOW:    int = _S.sync.circuit_breaker.window  # look-back window (s)
_CB_THRESHOLD: int = _S.sync.circuit_breaker.threshold  # timeouts to trip
_CB_BACKOFF:   int = _S.sync.circuit_breaker.backoff  # pause duration (s)

# Per-device retry on isolated timeouts (circuit breaker not tripped).
# Delay doubles each attempt starting from RETRY_DELAY, capped at RETRY_MAX_DELAY:
# default sequence is 8m, 16m, 30m, 30m, 30m, 30m across 6 attempts (~2.5h span).
_RETRY_DELAY:     int = _S.sync.retry.delay  # seconds before first retry
_RETRY_MAX_DELAY: int = _S.sync.retry.max_delay  # cap on backoff growth
_RETRY_MAX:       int = _S.sync.retry.max  # max retries per device

_CF_NEIGHBOR_TEXT:   Optional[str] = _S.custom_fields.neighbor_text
_CF_NEIGHBOR_PORT:   Optional[str] = _S.custom_fields.neighbor_port
_CF_NEIGHBOR_DEVICE: Optional[str] = _S.custom_fields.neighbor_device
_CF_NEIGHBOR_IFACE:  Optional[str] = _S.custom_fields.neighbor_iface

_CABLE_SCOPE:        str            = _S.cabling.scope
_CABLE_SOURCE_CF:    Optional[str]  = _S.cabling.source_cf
_CABLE_SOURCE_VALUE: Optional[str]  = _S.cabling.source_value

_IFACE_SOURCE_CF:    Optional[str]  = _S.custom_fields.source
_IFACE_SOURCE_VALUE: str            = _S.custom_fields.source_value

_CF_OS_VERSION:      Optional[str]  = _S.custom_fields.os_version
_CF_OS_NAME:         Optional[str]  = _S.custom_fields.os_name
_CF_OS_RELEASE:      Optional[str]  = _S.custom_fields.os_release
_CF_STACK_MEMBERS:   Optional[str]  = _S.custom_fields.stack_members
_STACK_MEMBERS_ONLY_INCREASE: bool  = _S.custom_fields.stack_members_only_increase
_CF_TOUCH:           Optional[str]  = _S.custom_fields.touch
_CF_INVENTORY_REBUILD: Optional[str] = _S.custom_fields.inventory_rebuild
_CF_CONTROLLER:      Optional[str]  = _S.custom_fields.controller
_AP_PRUNE_INTERFACES: bool          = _S.aps.prune_interfaces
# Orphaned interfaces (in Netbox, not in Netdisco): discobox's own always go; the
# source-less (imported/manual) ones only with this. Unset → follows housekeeping.
_REMOVE_ORPHANED: Optional[bool]    = _S.sync.remove_orphaned_interfaces
_ORPHAN_DELETE_MAX_PERCENT: int     = _S.sync.orphan_delete_max_percent
_INTERFACE_DEFAULTS: list[dict]     = [r.model_dump() for r in _S.interface_defaults]
_AP_TYPE_CONFIRMED_UNTAG: list      = list(_S.aps.type_confirmed_untag)
_TOUCH_COOLDOWN_DAYS: int           = _S.sync.touch_cooldown_days

_TYPES_CREATE_MISSING: bool          = _S.types.create_missing

# Per-request snmptimeout policy for discobox-enqueued discover jobs (see
# discobox.DISCOVER_SNMPTIMEOUT): derived from snmp_polling_timeout, bounded.
configure_discover_snmptimeout(**_S.netdisco.discover_snmptimeout.model_dump())
configure_read_fields(auth_profile=_S.custom_fields.auth_profile, snmp_timeout=_S.custom_fields.snmp_timeout)
_TYPES_ALIAS_CF:      Optional[str]  = _S.types.alias_cf
_TYPES_DEVICE_ALIASES: dict          = _S.types.device_aliases
_TYPES_MODULE_ALIASES: dict          = _S.types.module_aliases

# DeviceType enrichment from the devicetype-library (typesync.py, /types/library)
_LIBRARY_PATH:    Optional[str] = _S.library.path
_LIBRARY_OVERLAY: Optional[str] = _S.library.overlay
_LIBRARY_ROLES:   list          = list(_S.library.roles)
_LIBRARY_MAPPING: dict          = _S.library.device_types
# discobox_types_bare refresh from Netbox: this often (0 = only after type creations / applies)
_LIBRARY_METRICS_INTERVAL: int  = _S.jobs.types_bare.interval
# source value /types/library apply sets on a library-filled type (custom_fields.source)
_LIBRARY_SOURCE_VALUE: str      = _S.library.source_value
_library: Optional[Library] = None
_library_lock = threading.Lock()


def _get_library() -> Library:
    global _library
    with _library_lock:
        if _library is None:
            if not _LIBRARY_PATH or not os.path.isdir(os.path.join(_LIBRARY_PATH, "device-types")):
                raise HTTPException(503, f"devicetype-library not found at {_LIBRARY_PATH!r} (library.path)")
            roots = [_LIBRARY_PATH] + ([_LIBRARY_OVERLAY] if _LIBRARY_OVERLAY else [])
            _library = Library([Path(r) for r in roots])
        return _library


async def require_auth(authorization: Annotated[str, Header()] = "") -> None:
    """Bearer token auth. Disabled if DISCOBOX_AUTH_TOKEN is not set."""
    if not _AUTH_TOKEN:
        return
    if authorization != f"Bearer {_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


# ── Reconcile loop ─────────────────────────────────────────────────────────────

_GAPS_INTERVAL:           int           = _S.jobs.gaps.interval
_GAPS_MAX_AGE:            int           = _S.jobs.gaps.max_age
_DISCOVER_MISSING_INTERVAL: int         = _S.jobs.discover_missing.interval
_RECONCILE_MAX_QUEUED:    Optional[int] = _S.reconcile.max_queued
_RECONCILE_MAX_FAILED:    Optional[int] = _S.reconcile.max_failed
_RECONCILE_MAX_ENQUEUE:   Optional[int] = _S.reconcile.max_enqueue
_RECONCILE_ROLES:         list[str]     = list(_S.reconcile.roles)
_RECONCILE_STATUSES:      list[str]     = list(_S.reconcile.statuses)
_RECONCILE_REQUIRE_AUTH_TAG: bool       = _S.reconcile.require_auth_tag
# Reconcile enqueues more discovery work on top of whatever's already syncing —
# the last thing a large hook-triggered backlog needs is reconcile piling on
# more load while it's still draining. Deferred (not skipped): re-checked every
# defer_seconds until the queue clears, so it still always eventually runs.
_RECONCILE_DEFER_IF_QUEUED: bool         = _S.reconcile.defer_if_queued
_RECONCILE_DEFER_SECONDS:   int          = _S.reconcile.defer_seconds
# Netdisco devices not in Netbox whose last discovery lags Netdisco's newest by
# more than this are listed as stale, not counted (and never auto-created).
_NOT_IN_NETBOX_STALE_HOURS: Optional[float] = _S.reconcile.not_in_netbox_stale_hours

# Liveness gate: before enqueueing discovery, ask a Prometheus-compatible API
# (VictoriaMetrics vmselect) which devices actually respond, and skip the dead
# ones. Disabled unless a URL is configured. Query failures fail open.
_LIVENESS_URL:   Optional[str] = _S.reconcile.liveness.url
_LIVENESS_QUERY: str = _S.reconcile.liveness.query
_LIVENESS_LABEL: str = _S.reconcile.liveness.label
_LIVENESS_KEY: str = _S.reconcile.liveness.key.lower()  # 'ip' (primary IP) or 'name' (device name/FQDN)
_LIVENESS_TIMEOUT: int = _S.reconcile.liveness.timeout
_LIVENESS_TLS_VERIFY: bool = _S.reconcile.liveness.tls_verify

# HA member resolution: when a device's SNMP-reported hostname doesn't match
# its Netbox name (e.g. a Fortinet cluster VIP), look up the correct Netbox
# primary IP from a Prometheus-compatible metric whose labels already carry
# it (e.g. fgHaStatsSyncStatus_info{fgHaStatsHostname=...}). Reuses the same
# query endpoint as the liveness gate; disabled unless that URL is set.
_HA_METRICS_URL:   Optional[str] = _S.sync.ha_resolve.url or _LIVENESS_URL
_HA_METRICS_METRIC:         str = _S.sync.ha_resolve.metric
_HA_METRICS_HOSTNAME_LABEL: str = _S.sync.ha_resolve.hostname_label
_HA_METRICS_TARGET_LABEL:   str = _S.sync.ha_resolve.target_label
_HA_METRICS_TIMEOUT:        int = _S.sync.ha_resolve.timeout
# instance_label matches a metric row to an already-known NetBox device FQDN
# (rather than a vendor hostname) -- used to also ensure each HA member's own
# management IP (not just the one currently being resolved) is set correctly.
_HA_METRICS_INSTANCE_LABEL: str = _S.sync.ha_resolve.instance_label
_HA_MGMT_IFACE_NAME:        str = _S.sync.ha_resolve.mgmt_iface_name

_AUTO_CREATE_ROLE:     Optional[str] = _S.auto_create.role
_AUTO_CREATE_SITE:     Optional[str] = _S.auto_create.site
_AUTO_CREATE_STATUS:   str           = _S.auto_create.status
_AUTO_CREATE_LOCATION: bool          = _S.auto_create.location


def _count_netdisco_request(method: str) -> None:
    netdisco_requests_total.labels(method=method).inc()


def _count_netbox_request(method: str) -> None:
    netbox_requests_total.labels(method=method).inc()


def _make_netdisco_client() -> NetdiscoClient:
    tls = env_flag("NETDISCO_TLS_VERIFY")
    token = os.getenv("NETDISCO_TOKEN")
    if token:
        return NetdiscoClient(base_url=os.environ["NETDISCO_URL"], token=token, verify_tls=tls, on_request=_count_netdisco_request)
    return NetdiscoClient(
        base_url=os.environ["NETDISCO_URL"],
        username=os.environ["NETDISCO_USERNAME"],
        password=os.environ["NETDISCO_PASSWORD"],
        verify_tls=tls,
        on_request=_count_netdisco_request,
    )


# Same rationale as _get_netbox_client(): reuse one requests.Session instead
# of opening a fresh TCP+TLS connection per sync. _get()/_post() already
# handle session expiry (401 -> _reauth()), so a long-lived shared instance
# is safe even for username/password auth.
_netdisco_client: Optional[NetdiscoClient] = None
_netdisco_client_lock = threading.Lock()


def _get_netdisco_client() -> NetdiscoClient:
    global _netdisco_client
    if _netdisco_client is None:
        with _netdisco_client_lock:
            if _netdisco_client is None:
                _netdisco_client = _make_netdisco_client()
    return _netdisco_client


# Shared across every sync/gaps call: constructing a fresh NetboxClient
# per request opened a brand-new requests.Session (and thus a brand-new
# TCP+TLS connection) every single time instead of reusing a warm connection
# pool — real, avoidable load on Netbox at 8-concurrent-syncs scale. Token
# auth has no session/login state to go stale, so sharing one instance for
# the process lifetime is safe (requests.Session + urllib3's connection pool
# are safe for concurrent use across threads).
_netbox_client: Optional[NetboxClient] = None
_netbox_client_lock = threading.Lock()


def _get_netbox_client() -> NetboxClient:
    global _netbox_client
    if _netbox_client is None:
        with _netbox_client_lock:
            if _netbox_client is None:
                _netbox_client = NetboxClient(
                    url=os.environ["NETBOX_URL"],
                    token=os.environ["NETBOX_TOKEN"],
                    verify_tls=env_flag("NETBOX_TLS_VERIFY"),
                    on_request=_count_netbox_request,
                    type_alias_cf=_TYPES_ALIAS_CF,
                    device_type_aliases=_TYPES_DEVICE_ALIASES,
                    module_type_aliases=_TYPES_MODULE_ALIASES,
                    create_missing_types=_TYPES_CREATE_MISSING,
                )
                _netbox_client.on_type_created = _on_type_created
                _netbox_client.type_source_cf = _IFACE_SOURCE_CF
                _netbox_client.type_source_value = _IFACE_SOURCE_VALUE
    return _netbox_client


def _library_has(manufacturer: str, model: str, part_number: str = "") -> str:
    """available / none / unknown (library not loadable)."""
    try:
        lib = _get_library()
        entry, _ = match_library(
            {"model": model, "part_number": part_number or model, "slug": model, "manufacturer": manufacturer},
            lib.index_for([manufacturer]), _LIBRARY_MAPPING,
        )
        return "available" if entry else "none"
    except Exception:
        return "unknown"


def _on_type_created(kind: str, manufacturer: str, model: str) -> None:
    """A sync minted a bare type: say so, and refresh the metric shortly (rare)."""
    library = _library_has(manufacturer, model) if kind == "device" else "unknown"
    logger.warning(
        "Created a bare %s type %s / %s (no templates)%s", kind, manufacturer, model,
        f": the library has it, run /types/library?type={model}&apply=true" if library == "available"
        else ": not in the library, fix it by hand (then set source=manual) or upstream it",
    )
    # 60s later: the sync assigns the device to the new type only after creating it
    threading.Timer(60, _refresh_types_bare_safe).start()


_types_bare_lock = threading.Lock()


def _refresh_types_bare() -> int:
    """
    discobox_types_bare from Netbox: every DeviceType/ModuleType with
    source=netdisco (fully replaced each time, so a type that got fixed, i.e.
    source changed or the type deleted, drops out). Returns the number found.
    """
    if not _IFACE_SOURCE_CF:
        return 0
    nb = _get_netbox_client()
    rows = []
    flt = {f"cf_{_IFACE_SOURCE_CF}": _IFACE_SOURCE_VALUE}
    if nb.cf_on(_IFACE_SOURCE_CF, "dcim.devicetype"):
        for dt in nb.nb.dcim.device_types.filter(**flt):
            mfr = getattr(dt.manufacturer, "name", "") or ""
            pn = getattr(dt, "part_number", "") or ""
            rows.append((("device", mfr, dt.model, pn, _library_has(mfr, dt.model, pn)),
                         getattr(dt, "device_count", 0) or 0))
    if nb.cf_on(_IFACE_SOURCE_CF, "dcim.moduletype"):
        for mt in nb.nb.dcim.module_types.filter(**flt):
            mfr = getattr(mt.manufacturer, "name", "") or ""
            rows.append((("module", mfr, mt.model, getattr(mt, "part_number", "") or "", "unknown"),
                         getattr(mt, "module_count", 0) or 0))
    with _types_bare_lock:
        types_bare.clear()
        for labels, value in rows:
            types_bare.labels(*labels).set(value)
    return len(rows)


def _refresh_types_bare_safe() -> None:
    """Refresh now (recorded as a types-bare run); after one in progress, don't double it."""
    if "types-bare" in _jobs:
        _jobs.run("types-bare", wait_if_running=True)


def _fetch_liveness_safe() -> Optional[dict]:
    """The liveness data for a gaps run; None (fail open) when off or the query fails."""
    if not _LIVENESS_URL:
        return None
    try:
        liveness = fetch_liveness(
            _LIVENESS_URL, _LIVENESS_QUERY,
            label=_LIVENESS_LABEL,
            timeout=_LIVENESS_TIMEOUT,
            verify_tls=_LIVENESS_TLS_VERIFY,
        )
    except Exception as exc:
        liveness_query_up.set(0)
        _save_liveness_status({"ok": False, "devices": 0, "error": str(exc), "checked": time.time()})
        logger.error("Liveness query failed (failing open: no device marked down): %s", exc)
        return None
    liveness_query_up.set(1)
    _save_liveness_status({"ok": True, "devices": len(liveness), "error": None, "checked": time.time()})
    logger.info("Liveness: %d devices matched by query", len(liveness))
    return liveness


def _run_gaps() -> dict:
    """Compare Netbox with Netdisco, keep the gap lists for the status page, discover-missing and fix-tags."""
    counts = find_gaps(
        _get_netdisco_client(), _get_netbox_client(),
        roles=_RECONCILE_ROLES,
        statuses=_RECONCILE_STATUSES,
        liveness=_fetch_liveness_safe(),
        liveness_key=_LIVENESS_KEY,
        not_in_netbox_stale_hours=_NOT_IN_NETBOX_STALE_HOURS,
    )
    gaps_netbox_devices.set(counts["netbox_total"])
    gaps_netdisco_devices.set(counts["netdisco_total"])
    gaps_not_in_netdisco.set(counts["not_in_netdisco"])
    gaps_offline.set(counts["offline"])
    gaps_not_in_netbox.set(counts["not_in_netbox"])
    gaps_not_in_netbox_stale.set(counts["not_in_netbox_stale"])
    gaps_tag_mismatches.set(counts["tag_mismatches"])
    gaps_ip_mismatches.set(counts.get("ip_mismatches", 0))
    summary = {k: v for k, v in counts.items() if not k.endswith("_list")}
    with _reconcile_gaps_lock:
        _save_gap(_NOT_IN_NETDISCO_FILE, counts["not_in_netdisco_list"])
        _save_gap(_NOT_IN_NETBOX_FILE, counts["not_in_netbox_list"])
        _save_gap(_TAG_MISMATCHES_FILE, counts["tag_mismatches_list"])
        _save_gap(_IP_MISMATCHES_FILE, counts.get("ip_mismatches_list", []))
        _save_gaps_counts(summary)
    return summary


def _gaps_too_old() -> Optional[str]:
    """Why the last gaps result can't be acted on (None: fresh enough)."""
    finished = _jobs.last_success("gaps")
    if not finished:
        return "no gaps result yet: run gaps first"
    age = time.time() - finished
    if age > _GAPS_MAX_AGE:
        return f"gaps result is {int(age // 60)}m old (max {_GAPS_MAX_AGE // 60}m): run gaps first"
    return None


# ── Jobs ───────────────────────────────────────────────────────────────────────

_jobs = JobRegistry(os.path.join(_STATE_DIR, "discobox.jobs.json"), registry=_custom_registry)


def _job_gaps() -> JobResult:
    c = _run_gaps()
    parts = [f"{c['not_in_netdisco']} not in Netdisco", f"{c['not_in_netbox']} not in Netbox"]
    if c["not_in_netbox_stale"]:
        parts[-1] += f" (+{c['not_in_netbox_stale']} stale)"
    if c["offline"]:
        parts.append(f"{c['offline']} offline")
    parts.append(f"{c['tag_mismatches']} tag mismatches")
    if c.get("ip_mismatches"):
        parts.append(f"{c['ip_mismatches']} IP mismatches")
    return JobResult(summary=", ".join(parts), data=c)


def _job_discover_missing(max_enqueue: Optional[int] = None, offset: Optional[int] = None,
                          manual: bool = False) -> JobResult:
    if _is_paused():
        return JobResult("skipped", "sync is paused", retry_in=_RECONCILE_DEFER_SECONDS)
    too_old = _gaps_too_old()
    if too_old:
        return JobResult("skipped", too_old, retry_in=_RECONCILE_DEFER_SECONDS)
    queued = len(_inflight_hosts())
    if _RECONCILE_DEFER_IF_QUEUED and queued and not manual:
        return JobResult("deferred", f"{queued} sync(s) still queued/in-flight", retry_in=_RECONCILE_DEFER_SECONDS)
    with _reconcile_gaps_lock:
        not_in_netdisco = _load_gap(_NOT_IN_NETDISCO_FILE)
        not_in_netbox = _load_gap(_NOT_IN_NETBOX_FILE)
    c = discover_missing(
        _get_netdisco_client(), _get_netbox_client(), not_in_netdisco, not_in_netbox,
        max_queued=_RECONCILE_MAX_QUEUED,
        max_failed=_RECONCILE_MAX_FAILED,
        max_enqueue=max_enqueue if max_enqueue is not None else _RECONCILE_MAX_ENQUEUE,
        offset=offset,
        require_auth_tag=_RECONCILE_REQUIRE_AUTH_TAG,
        auto_create_role=_AUTO_CREATE_ROLE,
        auto_create_site=_AUTO_CREATE_SITE,
        auto_create_status=_AUTO_CREATE_STATUS,
        auto_create_location=_AUTO_CREATE_LOCATION,
        iface_source_cf=_IFACE_SOURCE_CF,
        iface_source_value=_IFACE_SOURCE_VALUE,
    )
    if c["aborted"]:
        return JobResult("aborted", "Netdisco queue too busy or its status unreachable (see log)")
    discover_missing_enqueued_total.inc(c["enqueued"])
    parts = [f"{c['enqueued']} enqueued"]
    for key, label in (("skipped", "without auth tag"), ("skipped_offline", "offline"), ("created", "created"),
                       ("failed", "auto-create failed")):
        if c.get(key):
            parts.append(f"{c[key]} {label}")
    return JobResult(summary=", ".join(parts), data=c)


def _job_fix_tags(max_enqueue: Optional[int] = None) -> JobResult:
    if _is_paused():
        return JobResult("skipped", "sync is paused")
    too_old = _gaps_too_old()
    if too_old:
        return JobResult("skipped", too_old)
    with _reconcile_gaps_lock:
        mismatches = _load_gap(_TAG_MISMATCHES_FILE)
    result = fix_tag_mismatches(
        _get_netdisco_client(), mismatches,
        max_queued=_RECONCILE_MAX_QUEUED,
        max_failed=_RECONCILE_MAX_FAILED,
        max_enqueue=max_enqueue,
    )
    if result.get("aborted"):
        return JobResult("aborted", "Netdisco queue too busy or its status unreachable (see log)")
    fix_tags_enqueued_total.inc(result.get("enqueued", 0))
    return JobResult(summary=f"{result.get('enqueued', 0)} enqueued, {result.get('skipped', 0)} skipped, "
                             f"{result.get('errors', 0)} errors")


def _job_types_bare() -> JobResult:
    n = _refresh_types_bare()
    available = sum(1 for m in types_bare.collect() for smp in m.samples
                    if smp.name == "discobox_types_bare" and smp.labels.get("library") == "available")
    return JobResult(summary=f"{n} dummy types, {available} the library can fill")


_jobs.register(Job("gaps", _job_gaps, interval=_GAPS_INTERVAL, startup_delay=60,
                   description="Compare Netbox with Netdisco: the gap lists (read-only on both)"))
_jobs.register(Job("discover-missing", _job_discover_missing, interval=_DISCOVER_MISSING_INTERVAL, startup_delay=300,
                   description="From the last gaps result: enqueue Netdisco discovery for Netbox devices it doesn't "
                               "know, auto-create Netbox devices (auto_create)"))
_jobs.register(Job("fix-tags", _job_fix_tags,
                   description="From the last gaps result: re-enqueue discovery with Netbox's auth tag for tag mismatches"))
if _IFACE_SOURCE_CF:
    _jobs.register(Job("types-bare", _job_types_bare, interval=_LIBRARY_METRICS_INTERVAL,
                       description="Refresh discobox_types_bare (dummy types and what the library can fill)"))


def _drain_retries(now: float, submit) -> None:
    """One retry-loop tick: hand ready entries to submit(host, retry_count, entry).

    A full queue defers the entry (retry_count unchanged) instead of dropping it —
    queue pressure is transient and shouldn't burn a backoff attempt.
    """
    with _retry_lock:
        ready = {h: dict(v) for h, v in _retry_pending.items() if v["retry_after"] <= now}
        for h in ready:
            del _retry_pending[h]
    for host, entry in ready.items():
        retry_count = entry.pop("retry_count")
        entry.pop("retry_after", None)
        with _in_flight_lock:
            queue_full = len(_in_flight) >= _MAX_QUEUE
            if not queue_full:
                if not _claim_host(host):
                    logger.debug("Retry for %s: already in flight, dropping", host)
                    continue
        if queue_full:
            with _retry_lock:
                if host not in _retry_pending:
                    _retry_pending[host] = {"retry_count": retry_count, "retry_after": now + 60, **entry}
            logger.warning("Retry for %s deferred 60s: queue full", host)
            continue
        sync_in_progress.inc()
        logger.info("Retrying sync for %s (attempt %d/%d)", host, retry_count, _RETRY_MAX)
        submit(host, retry_count, entry)


async def _retry_loop() -> None:
    """Drain _retry_pending: re-queue timed-out devices once their backoff has elapsed."""
    loop = asyncio.get_running_loop()

    def submit(host: str, retry_count: int, entry: dict) -> None:
        loop.run_in_executor(None, partial(_run_sync, host, _retry_count=retry_count, **entry))

    while True:
        await asyncio.sleep(10)
        _drain_retries(time.time(), submit)


@asynccontextmanager
async def lifespan(app):
    tasks = [asyncio.create_task(_jobs.scheduler())]
    try:
        tasks.append(asyncio.create_task(_retry_loop()))
    except Exception as exc:
        logger.error("Failed to start retry loop: %s", exc)
    yield
    for task in tasks:
        task.cancel()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="discobox",
    description="Netdisco → Netbox sync webhook receiver",
    version=__version__,
    lifespan=lifespan,
    docs_url=None,      # served below from local assets
    redoc_url=None,
)

# Swagger UI assets baked into the image (Dockerfile), so /docs works without
# internet access; a checkout without them falls back to FastAPI's CDN copy.
_SWAGGER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "swagger")
if os.path.isfile(os.path.join(_SWAGGER_DIR, "swagger-ui-bundle.js")):
    app.mount("/static/swagger", StaticFiles(directory=_SWAGGER_DIR), name="swagger")
    _SWAGGER_ASSETS = {"swagger_js_url": "/static/swagger/swagger-ui-bundle.js",
                       "swagger_css_url": "/static/swagger/swagger-ui.css",
                       "swagger_favicon_url": "/static/swagger/favicon-32x32.png"}
else:
    _SWAGGER_ASSETS = {}


@app.get("/docs", include_in_schema=False)
async def swagger_ui() -> HTMLResponse:
    return get_swagger_ui_html(openapi_url=app.openapi_url, title="discobox API", **_SWAGGER_ASSETS)

# Actions are POST. GET still works on /sync and /rebuild until their callers
# use POST: deprecated, so logged, counted and marked.
_GET_DEPRECATED_PATHS = frozenset({"/sync", "/rebuild"})


@app.middleware("http")
async def _deprecated_get(request: Request, call_next):
    if request.method == "GET" and request.url.path in _GET_DEPRECATED_PATHS:
        deprecated_requests_total.labels(request.url.path).inc()
        logger.warning("Deprecated: GET %s, use POST (GET goes away in a later v2 release)", request.url.path)
        response = await call_next(request)
        response.headers["Deprecation"] = "true"
        return response
    return await call_next(request)

# Hosts queued or syncing. One process, so a set is the whole story.
_in_flight: set[str] = set()
_in_flight_lock = threading.Lock()


def _claim_host(host: str) -> bool:
    """Mark host as in flight; False when it already is. Callers hold _in_flight_lock."""
    if host in _in_flight:
        return False
    _in_flight.add(host)
    return True


def _release_host(host: str) -> None:
    with _in_flight_lock:
        _in_flight.discard(host)


def _inflight_hosts() -> list[str]:
    with _in_flight_lock:
        return sorted(_in_flight)


def _recently_synced(host: str) -> bool:
    """Return True if host was successfully synced within the cooldown window."""
    if not _SYNC_COOLDOWN:
        return False
    path = os.path.join(_STATE_DIR, f"discobox.synced.{host}")
    try:
        return (time.time() - os.stat(path).st_mtime) < _SYNC_COOLDOWN
    except FileNotFoundError:
        return False


def _mark_synced(host: str) -> None:
    path = os.path.join(_STATE_DIR, f"discobox.synced.{host}")
    try:
        open(path, "w").close()
    except OSError:
        pass

# Devices Netdisco sent a sync hook for but not in Netbox (kept across restarts).
# An entry goes once a sync finds the device in Netbox, or
# when no hook has come for it in _UNKNOWN_MAX_AGE_S (gone from Netdisco too).
_UNKNOWN_DEVICES_FILE: str = os.path.join(
    _STATE_DIR, "discobox.unknown.json"
)
_UNKNOWN_MAX_AGE_S = 14 * 86400
_unknown_devices_lock = threading.Lock()

def _load_unknown_devices() -> dict[str, dict]:
    try:
        with open(_UNKNOWN_DEVICES_FILE) as f:
            devices = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    cutoff = time.time() - _UNKNOWN_MAX_AGE_S
    return {k: d for k, d in devices.items() if d.get("last_seen", 0) >= cutoff}

def _forget_unknown_device(host: str, hostname: str) -> None:
    """Drop host (and any entry with the same hostname) once Netbox has the device."""
    with _unknown_devices_lock:
        devices = _load_unknown_devices()
        keep = {k: d for k, d in devices.items()
                if k != host and not (hostname and d.get("hostname") == hostname)}
        if len(keep) != len(devices):
            _save_unknown_devices(keep)

def _save_unknown_devices(devices: dict[str, dict]) -> None:
    with open(_UNKNOWN_DEVICES_FILE, "w") as f:
        json.dump(devices, f)

# Reconcile gap lists: replaced wholesale after each reconcile run.
_NOT_IN_NETDISCO_FILE: str = os.path.join(
    _STATE_DIR, "discobox.not_in_netdisco.json"
)
_NOT_IN_NETBOX_FILE: str = os.path.join(
    _STATE_DIR, "discobox.not_in_netbox.json"
)
_TAG_MISMATCHES_FILE: str = os.path.join(
    _STATE_DIR, "discobox.tag_mismatches.json"
)
_IP_MISMATCHES_FILE: str = os.path.join(
    _STATE_DIR, "discobox.ip_mismatches.json"
)
_reconcile_gaps_lock = threading.Lock()

# WLC AP pass: skip the APs while the WLC's AP list is unchanged (per-WLC state
# file here), full pass every aps.full_pass_days and on every rebuild.
configure_ap_fingerprint(_STATE_DIR if _S.aps.skip_unchanged else None, _S.aps.full_pass_days)

# Counts of the last successful gaps run, served by GET /gaps (the lists are
# in the gap files; when and how it ran is in the job state).
_GAPS_COUNTS_FILE: str = os.path.join(_STATE_DIR, "discobox.gaps.json")

def _load_gaps_counts() -> dict:
    try:
        with open(_GAPS_COUNTS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_gaps_counts(counts: dict) -> None:
    try:
        with open(_GAPS_COUNTS_FILE, "w") as f:
            json.dump(counts, f)
    except OSError as exc:
        logger.warning("Could not save the gaps counts: %s", exc)

def _load_gap(path: str) -> list[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_gap(path: str, devices: list[dict]) -> None:
    with open(path, "w") as f:
        json.dump(devices, f)

# Liveness query health, written on each reconcile run for the status page.
_LIVENESS_FILE: str = os.path.join(
    _STATE_DIR, "discobox.liveness.json"
)

def _load_liveness_status() -> dict:
    try:
        with open(_LIVENESS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_liveness_status(status: dict) -> None:
    with open(_LIVENESS_FILE, "w") as f:
        json.dump(status, f)

# Limit concurrent Netbox API load.
_MAX_CONCURRENT: int = _S.sync.max_concurrent
_sync_semaphore = threading.Semaphore(_MAX_CONCURRENT)
_MAX_QUEUE: int = _S.sync.max_queue

# Pause gate: a file in the state dir, so a pause survives a restart.
# Presence of the file = paused; absence = running.
_PAUSE_FILE: str = os.path.join(
    _STATE_DIR, "discobox.pause"
)

# Circuit breaker: intake held until this time (0: not tripped).
_cb_resume_at: float = 0.0
_recent_timeouts: deque = deque()
_timeout_lock = threading.Lock()

# Per-device retry queue: host → {retry_count, retry_after, **sync_kwargs}
_retry_pending: dict[str, dict] = {}
_retry_lock = threading.Lock()


def _cb_is_active() -> bool:
    return time.time() < _cb_resume_at


def _trip_circuit_breaker() -> None:
    global _cb_resume_at
    _cb_resume_at = time.time() + _CB_BACKOFF
    circuit_breaker_trips_total.inc()
    logger.warning(
        "Circuit breaker tripped: %d timeouts in %ds — holding intake for %ds",
        _CB_THRESHOLD, _CB_WINDOW, _CB_BACKOFF,
    )


def _is_overload_error(exc: Exception) -> bool:
    """True for ReadTimeout and HTTP 502/503/504 — both indicate netdisco is overloaded."""
    if isinstance(exc, ReadTimeout):
        return True
    if isinstance(exc, HTTPError):
        resp = getattr(exc, "response", None)
        return resp is not None and resp.status_code in (502, 503, 504)
    return False


def _on_timeout(host: str, retry_count: int, sync_kwargs: dict) -> None:
    """Record a timeout, trip the circuit breaker if threshold reached, schedule a retry."""
    sync_timeouts_total.inc()
    now = time.time()
    with _timeout_lock:
        _recent_timeouts.append(now)
        cutoff = now - _CB_WINDOW
        while _recent_timeouts and _recent_timeouts[0] < cutoff:
            _recent_timeouts.popleft()
        window_count = len(_recent_timeouts)
    if window_count >= _CB_THRESHOLD and not _cb_is_active():
        _trip_circuit_breaker()
    if retry_count >= _RETRY_MAX:
        logger.warning("Timeout on %s: max retries (%d) reached, dropping", host, _RETRY_MAX)
        return
    with _retry_lock:
        if host in _retry_pending:
            logger.debug("Timeout on %s: retry already pending, dropping", host)
            return
        delay = min(_RETRY_DELAY * (2 ** retry_count), _RETRY_MAX_DELAY)
        _retry_pending[host] = {"retry_count": retry_count + 1, "retry_after": now + delay, **sync_kwargs}
    sync_retries_total.inc()
    logger.info("Timeout on %s: retry %d/%d in %ds", host, retry_count + 1, _RETRY_MAX, delay)


def _is_paused() -> bool:
    return os.path.exists(_PAUSE_FILE) or _cb_is_active()

def _set_paused(paused: bool) -> None:
    if paused:
        open(_PAUSE_FILE, "w").close()
    else:
        try:
            os.unlink(_PAUSE_FILE)
        except FileNotFoundError:
            pass


# ── Request / response models ──────────────────────────────────────────────────

class SyncRequest(BaseModel):
    host: str
    sync_mac: bool = _DEFAULT_MAC
    sync_ip: bool = _DEFAULT_IP
    sync_modules: bool = _DEFAULT_MODULES
    sync_sfp: bool = _DEFAULT_SFP
    sync_poe: bool = _DEFAULT_POE
    housekeeping: bool = _DEFAULT_HOUSEKEEPING
    lldp_clear_stale: bool = _DEFAULT_LLDP_CLEAR_STALE


class SyncResponse(BaseModel):
    status: str
    host: str
    reason: Optional[str] = None


class RebuildResponse(BaseModel):
    status: str
    host: str
    dry_run: bool
    hostname: Optional[str] = None
    prune: dict = {}
    new_stack_members: Optional[int] = None
    reason: Optional[str] = None


# ── Background sync ────────────────────────────────────────────────────────────

def _run_sync(host: str, sync_mac: bool, sync_ip: bool, sync_modules: bool, sync_sfp: bool, sync_poe: bool, housekeeping: bool, lldp_clear_stale: bool = False, cf_neighbor_text: Optional[str] = None, cf_neighbor_port: Optional[str] = None, cf_neighbor_device: Optional[str] = None, cf_neighbor_iface: Optional[str] = None, cable_scope: str = "", cable_source_cf: Optional[str] = None, cable_source_value: Optional[str] = None, iface_source_cf: Optional[str] = None, iface_source_value: str = "netdisco", cf_os_version: Optional[str] = "os_version", cf_os_name: Optional[str] = "os_name", cf_os_release: Optional[str] = "os_release", cf_stack_members: Optional[str] = "stack_members", stack_members_only_increase: bool = True, cf_touch: Optional[str] = "netdisco_last_update", touch_cooldown_days: int = 1, ha_metrics_url: Optional[str] = None, ha_metrics_metric: str = "fgHaStatsSyncStatus_info", ha_metrics_hostname_label: str = "fgHaStatsHostname", ha_metrics_target_label: str = "netbox_primary_ip", ha_metrics_timeout: int = 15, ha_metrics_tls_verify: bool = True, ha_metrics_instance_label: str = "instance", ha_mgmt_iface_name: str = "mgmt1", _retry_count: int = 0) -> None:
    """Run sync_device in a background thread and record metrics."""
    while True:
        _sync_semaphore.acquire()
        if not _is_paused():
            break
        _sync_semaphore.release()
        time.sleep(5)
    sync_running.inc()
    start = time.time()
    status = "error"
    result: dict = {}
    try:
        nd = _get_netdisco_client()
        nb = _get_netbox_client()
        result = sync_device(
            nd=nd,
            nb=nb,
            ip=host,
            sync_mac=sync_mac,
            sync_ip=sync_ip,
            sync_modules=sync_modules,
            sync_sfp=sync_sfp,
            sync_poe=sync_poe,
            housekeeping=housekeeping,
            lldp_clear_stale=lldp_clear_stale,
            vip_mode=_VIP_MODE,
            vip_mode_by_vendor=_VIP_MODE_BY_VENDOR,
            cf_neighbor_text=cf_neighbor_text,
            cf_neighbor_port=cf_neighbor_port,
            cf_neighbor_device=cf_neighbor_device,
            cf_neighbor_iface=cf_neighbor_iface,
            cable_scope=cable_scope,
            cable_source_cf=cable_source_cf,
            cable_source_value=cable_source_value,
            iface_source_cf=iface_source_cf,
            iface_source_value=iface_source_value,
            cf_os_version=cf_os_version,
            cf_os_name=cf_os_name,
            cf_os_release=cf_os_release,
            cf_stack_members=cf_stack_members,
            stack_members_only_increase=stack_members_only_increase,
            cf_touch=cf_touch,
            touch_cooldown_days=touch_cooldown_days,
            ha_metrics_url=ha_metrics_url,
            ha_metrics_metric=ha_metrics_metric,
            ha_metrics_hostname_label=ha_metrics_hostname_label,
            ha_metrics_target_label=ha_metrics_target_label,
            ha_metrics_timeout=ha_metrics_timeout,
            ha_metrics_tls_verify=ha_metrics_tls_verify,
            ha_metrics_instance_label=ha_metrics_instance_label,
            ha_mgmt_iface_name=ha_mgmt_iface_name,
            cf_controller=_CF_CONTROLLER,
            ap_prune_interfaces=_AP_PRUNE_INTERFACES,
            ap_type_confirmed_untag=_AP_TYPE_CONFIRMED_UNTAG,
            remove_orphaned_interfaces=_REMOVE_ORPHANED,
            orphan_delete_max_percent=_ORPHAN_DELETE_MAX_PERCENT,
            interface_defaults=_INTERFACE_DEFAULTS,
        )
        status = "success" if result.get("ok") else "error"
        if result.get("reason") in ("discovery_incomplete", "recently_touched"):
            syncs_skipped_total.labels(reason=result["reason"]).inc()
        if result.get("reason") == "discovery_incomplete":
            # Transient: Netdisco still shows ifIndex placeholders as port names.
            # Not an error (no failed flag, no auto-pause) and no cooldown mark,
            # so the hook after the completed discovery syncs normally.
            status = "skipped"
        if result.get("reason") == "recently_touched":
            # Netbox-side touch field already shows a change within the cooldown
            # window: not an error, and no local cooldown mark needed since the
            # touch field itself is the throttle signal for the next attempt.
            status = "skipped"
        if result.get("reason") == "not_in_netdisco":
            # Netdisco 404s the IP (a /sync for a Netbox device Netdisco doesn't
            # know): an inventory gap like the one below, not a failed sync.
            # The reconcile lists these on /not-in-netdisco.
            status = "not_in_netdisco"
        if result.get("reason") == "device_not_found":
            # In Netdisco, not in Netbox: an inventory gap, not a failed sync.
            # Own status (no failed flag, no auto-pause, no cooldown mark), and
            # the device goes on the list shown on / and /unknown-devices.
            status = "not_in_netbox"
            unknown_devices_total.inc()
            with _unknown_devices_lock:
                devices = _load_unknown_devices()
                devices[host] = {
                    "ip": host,
                    "hostname": result.get("hostname") or "",
                    "last_seen": time.time(),
                }
                _save_unknown_devices(devices)
    except Exception as exc:
        logger.error("Sync failed for %s: %s", host, exc)
        status = "error"
        if _is_overload_error(exc):
            _on_timeout(host, _retry_count, {
                "sync_mac": sync_mac, "sync_ip": sync_ip, "sync_modules": sync_modules,
                "sync_sfp": sync_sfp, "sync_poe": sync_poe, "housekeeping": housekeeping,
                "lldp_clear_stale": lldp_clear_stale, "cf_neighbor_text": cf_neighbor_text,
                "cf_neighbor_port": cf_neighbor_port, "cf_neighbor_device": cf_neighbor_device,
                "cf_neighbor_iface": cf_neighbor_iface, "cable_scope": cable_scope,
                "cable_source_cf": cable_source_cf, "cable_source_value": cable_source_value,
                "iface_source_cf": iface_source_cf, "iface_source_value": iface_source_value,
                "cf_os_version": cf_os_version, "cf_os_name": cf_os_name, "cf_os_release": cf_os_release,
                "cf_stack_members": cf_stack_members, "stack_members_only_increase": stack_members_only_increase,
                "cf_touch": cf_touch, "touch_cooldown_days": touch_cooldown_days,
                "ha_metrics_url": ha_metrics_url, "ha_metrics_metric": ha_metrics_metric,
                "ha_metrics_hostname_label": ha_metrics_hostname_label,
                "ha_metrics_target_label": ha_metrics_target_label,
                "ha_metrics_timeout": ha_metrics_timeout, "ha_metrics_tls_verify": ha_metrics_tls_verify,
                "ha_metrics_instance_label": ha_metrics_instance_label, "ha_mgmt_iface_name": ha_mgmt_iface_name,
            })
    finally:
        sync_running.dec()
        _sync_semaphore.release()
        elapsed = time.time() - start
        syncs_total.labels(status=status).inc()
        sync_duration.observe(elapsed)
        sync_in_progress.dec()
        _release_host(host)
        # Per-device metrics: duration/timestamp only on success so a persistently
        # failing device goes stale and timestamp-based alerts fire correctly.
        # device_sync_failed is always updated so failure is immediately visible.
        instance = result.get("hostname") or host
        if status in ("success", "error"):
            device_sync_failed.labels(instance=instance).set(0 if status == "success" else 1)
        if status == "error" and _PAUSE_ON_ERROR and not _is_paused():
            _set_paused(True)
            sync_paused.set(1)
            logger.warning("Sync error for %s: auto-pausing intake (DISCOBOX_PAUSE_ON_ERROR)", host)
        if result.get("ok"):
            _forget_unknown_device(host, result.get("hostname") or "")
        if status == "success":
            _mark_synced(host)
            device_sync_duration.labels(instance=instance).set(elapsed)
            device_sync_timestamp.labels(instance=instance).set(start)
        # Record per-action counts from result dict
        for action, count in result.get("interfaces", {}).items():
            interfaces_total.labels(action=action).inc(count)
        for action, count in result.get("ips", {}).items():
            ips_total.labels(action=action).inc(count)
        for action, count in result.get("modules", {}).items():
            modules_total.labels(action=action).inc(count)
        for action, count in result.get("sfps", {}).items():
            sfps_total.labels(action=action).inc(count)
        for action, count in result.get("aps", {}).items():
            aps_total.labels(action=action).inc(count)
        for field, count in result.get("interface_defaults", {}).items():
            interface_defaults_total.labels(field=field).inc(count)
        if "errors" in result:
            sync_errors_total.inc(result["errors"])
            device_sync_errors.labels(instance=instance).set(result["errors"])
        if "orphans" in result:
            for action, count in result["orphans"].items():
                orphans_total.labels(action=action).inc(count)
            device_orphans_braked.labels(instance=instance).set(result["orphans"]["braked"])
        if result.get("ha_vip"):
            ha_vip_total.inc()
        logger.info("Sync %s for %s in %.1fs", status, instance, elapsed)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.api_route(
    "/sync",
    methods=["GET", "POST"],
    response_model=SyncResponse,
    status_code=202,
    dependencies=[Depends(require_auth)],
    summary="Trigger a device sync",
)
async def sync(
    request: Request,
    background_tasks: BackgroundTasks,
    host: Annotated[Optional[str], Query(description="Device management IP")] = None,
    sync_mac: Annotated[bool, Query(description="Sync MAC addresses")] = _DEFAULT_MAC,
    sync_ip: Annotated[bool, Query(description="Sync IP addresses")] = _DEFAULT_IP,
    sync_modules: Annotated[bool, Query(description="Sync module bays / modules")] = _DEFAULT_MODULES,
    sync_sfp: Annotated[bool, Query(description="Sync SFP inventory items")] = _DEFAULT_SFP,
    sync_poe: Annotated[bool, Query(description="Sync PoE mode")] = _DEFAULT_POE,
    housekeeping: Annotated[bool, Query(description="Remove stale device bays and empty dummy interfaces")] = _DEFAULT_HOUSEKEEPING,
    lldp_clear_stale: Annotated[bool, Query(description="Clear LLDP neighbor fields and delete stale discobox-owned cables when no neighbor is present")] = _DEFAULT_LLDP_CLEAR_STALE,
    debug: Annotated[bool, Query(description="Run synchronously and return debug logs as plain text")] = False,
    force: Annotated[bool, Query(description="Ignore the sync cooldown window")] = False,
    body: Optional[SyncRequest] = None,
) -> SyncResponse:
    """
    Queue a sync for the given device IP.

    `host` can be passed as a query parameter or in the JSON body (POST only).
    All flags default to their normal values; set to `false` to skip a step.
    Returns immediately (202); sync runs in the background.
    Duplicate requests for the same host are dropped.
    """
    hooks_received_total.inc()
    resolved_host = host or (body.host if body else None)
    if not resolved_host:
        raise HTTPException(status_code=400, detail="host parameter required")

    try:
        resolved_host = validate_ip(resolved_host)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Body fields override query params when provided
    if body:
        sync_mac = body.sync_mac
        sync_ip = body.sync_ip
        sync_modules = body.sync_modules
        sync_sfp = body.sync_sfp
        sync_poe = body.sync_poe
        housekeeping = body.housekeeping
        lldp_clear_stale = body.lldp_clear_stale

    caller = request.headers.get("x-forwarded-for", "").split(",")[0].strip() \
             or (request.client.host if request.client else "unknown")

    if not force and _recently_synced(resolved_host):
        logger.debug("hook from %s: %s  cooldown active: skipping", caller, resolved_host)
        syncs_skipped_total.labels(reason="cooldown").inc()
        return SyncResponse(status="skipped", host=resolved_host, reason="cooldown")

    with _in_flight_lock:
        if len(_in_flight) >= _MAX_QUEUE:
            logger.warning("hook from %s: %s  queue full (%d/%d): dropping", caller, resolved_host, len(_in_flight), _MAX_QUEUE)
            syncs_skipped_total.labels(reason="queue_full").inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="queue full")
        if not _claim_host(resolved_host):
            logger.info("hook from %s: %s  already in progress: dropping", caller, resolved_host)
            syncs_skipped_total.labels(reason="in_progress").inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="already in progress")

    sync_in_progress.inc()

    if debug:
        cap = _CapturingHandler()
        discobox_log = logging.getLogger("discobox")
        prev_level = discobox_log.level
        discobox_log.setLevel(logging.DEBUG)
        discobox_log.addHandler(cap)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, _run_sync,
                resolved_host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe, housekeeping, lldp_clear_stale,
                _CF_NEIGHBOR_TEXT, _CF_NEIGHBOR_PORT, _CF_NEIGHBOR_DEVICE, _CF_NEIGHBOR_IFACE,
                _CABLE_SCOPE, _CABLE_SOURCE_CF, _CABLE_SOURCE_VALUE,
                _IFACE_SOURCE_CF, _IFACE_SOURCE_VALUE,
                _CF_OS_VERSION, _CF_OS_NAME, _CF_OS_RELEASE, _CF_STACK_MEMBERS, _STACK_MEMBERS_ONLY_INCREASE,
                _CF_TOUCH, 0 if force else _TOUCH_COOLDOWN_DAYS,
                _HA_METRICS_URL, _HA_METRICS_METRIC, _HA_METRICS_HOSTNAME_LABEL, _HA_METRICS_TARGET_LABEL,
                _HA_METRICS_TIMEOUT, _LIVENESS_TLS_VERIFY, _HA_METRICS_INSTANCE_LABEL, _HA_MGMT_IFACE_NAME,
            )
        finally:
            discobox_log.removeHandler(cap)
            discobox_log.setLevel(prev_level)
        return PlainTextResponse("\n".join(cap.lines))

    background_tasks.add_task(_run_sync, resolved_host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe, housekeeping, lldp_clear_stale, _CF_NEIGHBOR_TEXT, _CF_NEIGHBOR_PORT, _CF_NEIGHBOR_DEVICE, _CF_NEIGHBOR_IFACE, _CABLE_SCOPE, _CABLE_SOURCE_CF, _CABLE_SOURCE_VALUE, _IFACE_SOURCE_CF, _IFACE_SOURCE_VALUE, _CF_OS_VERSION, _CF_OS_NAME, _CF_OS_RELEASE, _CF_STACK_MEMBERS, _STACK_MEMBERS_ONLY_INCREASE, _CF_TOUCH, 0 if force else _TOUCH_COOLDOWN_DAYS, _LIVENESS_URL, _HA_METRICS_METRIC, _HA_METRICS_HOSTNAME_LABEL, _HA_METRICS_TARGET_LABEL, _HA_METRICS_TIMEOUT, _LIVENESS_TLS_VERIFY, _HA_METRICS_INSTANCE_LABEL, _HA_MGMT_IFACE_NAME)
    logger.info("hook from %s: %s  queued", caller, resolved_host)
    return SyncResponse(status="queued", host=resolved_host)


@app.api_route(
    "/rebuild",
    methods=["GET", "POST"],
    response_model=RebuildResponse,
    dependencies=[Depends(require_auth)],
    summary="Rebuild one device's inventory to exactly match Netdisco (deletes stale interfaces/modules/inventory)",
)
async def rebuild(
    host: Annotated[str, Query(description="Device management IP")],
    dry_run: Annotated[bool, Query(description="Report what would be deleted without deleting anything")] = True,
) -> RebuildResponse:
    """
    Reconcile one device's interfaces, chassis/blade modules, fans/PSUs, SFPs,
    and stack_members to exactly what Netdisco reports right now — including
    deletions. Unlike /sync this is destructive, so it's a separate, explicit,
    synchronous call rather than something the regular background sync/hook
    path ever does on its own.

    Only discobox-owned interfaces (the `source` custom field) are ever
    deleted. Modules/fans/PSUs/SFPs have no ownership marker, so a rebuild
    trusts the current Netdisco snapshot as truth for those — except a module
    bay whose installed module still has a non-netdisco-owned interface
    attached is kept and logged as a warning/skip rather than deleted.

    Bypasses the sync cooldown and touch-field throttle (a deliberate manual
    action, not a hook), but still claims the host like /sync does so it
    can't race a concurrent sync for the same device, and still respects the
    global pause flag (skips rather than blocking if sync is paused).

    Defaults to dry_run=true: nothing is deleted unless dry_run=false is
    passed explicitly.

    dry_run=true previews the rebuild's deletions, but the sync part still
    runs, exactly like a normal sync (configured housekeeping, AP skip as
    usual): it never writes more than a normal sync would. dry_run=false
    forces housekeeping and a full WLC AP pass, and stamps
    inventory_last_rebuild. Source-less orphaned interfaces follow
    sync.remove_orphaned_interfaces either way.
    """
    try:
        resolved_host = validate_ip(host)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if _is_paused():
        return RebuildResponse(status="skipped", host=resolved_host, dry_run=dry_run, reason="paused")

    with _in_flight_lock:
        if not _claim_host(resolved_host):
            return RebuildResponse(
                status="skipped", host=resolved_host, dry_run=dry_run, reason="already in progress",
            )

    nd = _get_netdisco_client()
    nb = _get_netbox_client()

    def _do_rebuild() -> dict:
        _sync_semaphore.acquire()
        try:
            return sync_device(
                nd=nd, nb=nb, ip=resolved_host,
                sync_mac=_DEFAULT_MAC, sync_ip=_DEFAULT_IP, sync_modules=_DEFAULT_MODULES,
                sync_sfp=_DEFAULT_SFP, sync_poe=_DEFAULT_POE,
                # dry-run: the sync part runs exactly like a normal sync (same settings,
                # no forced housekeeping, no forced AP pass); only the rebuild's own
                # deletions are previewed. Source-less orphans follow the config either way.
                housekeeping=_DEFAULT_HOUSEKEEPING if dry_run else True,
                remove_orphaned_interfaces=_REMOVE_ORPHANED, ap_full=not dry_run,
                lldp_clear_stale=_DEFAULT_LLDP_CLEAR_STALE,
                vip_mode=_VIP_MODE, vip_mode_by_vendor=_VIP_MODE_BY_VENDOR,
                cf_neighbor_text=_CF_NEIGHBOR_TEXT, cf_neighbor_port=_CF_NEIGHBOR_PORT,
                cf_neighbor_device=_CF_NEIGHBOR_DEVICE, cf_neighbor_iface=_CF_NEIGHBOR_IFACE,
                cable_scope=_CABLE_SCOPE, cable_source_cf=_CABLE_SOURCE_CF, cable_source_value=_CABLE_SOURCE_VALUE,
                iface_source_cf=_IFACE_SOURCE_CF, iface_source_value=_IFACE_SOURCE_VALUE,
                cf_os_version=_CF_OS_VERSION, cf_os_name=_CF_OS_NAME, cf_os_release=_CF_OS_RELEASE,
                cf_stack_members=_CF_STACK_MEMBERS, stack_members_only_increase=_STACK_MEMBERS_ONLY_INCREASE,
                cf_touch=_CF_TOUCH, touch_cooldown_days=0, cf_inventory_rebuild=_CF_INVENTORY_REBUILD,
                ha_metrics_url=_HA_METRICS_URL, ha_metrics_metric=_HA_METRICS_METRIC,
                ha_metrics_hostname_label=_HA_METRICS_HOSTNAME_LABEL,
                ha_metrics_target_label=_HA_METRICS_TARGET_LABEL,
                ha_metrics_timeout=_HA_METRICS_TIMEOUT, ha_metrics_tls_verify=_LIVENESS_TLS_VERIFY,
                ha_metrics_instance_label=_HA_METRICS_INSTANCE_LABEL, ha_mgmt_iface_name=_HA_MGMT_IFACE_NAME,
                cf_controller=_CF_CONTROLLER,
                ap_prune_interfaces=_AP_PRUNE_INTERFACES,
                ap_type_confirmed_untag=_AP_TYPE_CONFIRMED_UNTAG,
                orphan_delete_max_percent=_ORPHAN_DELETE_MAX_PERCENT,
                interface_defaults=_INTERFACE_DEFAULTS,
                prune=True, dry_run=dry_run,
            )
        finally:
            _sync_semaphore.release()

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _do_rebuild)
    except HTTPException:
        raise
    except Exception as exc:
        # one line in the log and a readable answer instead of an ASGI traceback
        logger.error("rebuild failed for %s%s: %s", resolved_host, " [dry-run]" if dry_run else "", exc)
        rebuilds_total.labels(status="error", dry_run=str(dry_run).lower()).inc()
        raise HTTPException(status_code=502, detail=f"rebuild failed: {exc}")
    finally:
        _release_host(resolved_host)

    reason = result.get("reason")
    status = "skipped" if reason else ("ok" if result.get("ok") else "error")
    rebuilds_total.labels(status="success" if status == "ok" else status, dry_run=str(dry_run).lower()).inc()
    if "errors" in result and result.get("hostname"):
        sync_errors_total.inc(result["errors"])
        device_sync_errors.labels(instance=result["hostname"]).set(result["errors"])
    if "orphans" in result and result.get("hostname") and not dry_run:
        orphans_total.labels(action="removed").inc(result["orphans"]["removed"])
        # the prune pass deletes the braked own ones (source-less ones stay; the next sync re-reports them)
        device_orphans_braked.labels(instance=result["hostname"]).set(0)
    prune_result = result.get("prune", {})
    logger.info(
        "rebuild %s for %s%s: %s", status, resolved_host, " [dry-run]" if dry_run else "",
        prune_result,
    )
    return RebuildResponse(
        status=status, host=resolved_host, dry_run=dry_run,
        hostname=result.get("hostname"), prune=prune_result,
        new_stack_members=(prune_result.get("stack_members") or {}).get("now"),
        reason=reason,
    )


def _enqueue_all(hosts: list, submit, limit: Optional[int] = None, force: bool = False) -> dict[str, int]:
    """Queue syncs for many hosts through the standard guards.

    submit(host) is called for each host that passes the cooldown, queue-cap
    and in-flight checks — the same gates the /sync webhook applies.
    Returns counters for the response body.
    """
    counts = {"queued": 0, "cooldown": 0, "queue_full": 0, "in_progress": 0, "invalid": 0}
    for host in hosts:
        if limit is not None and counts["queued"] >= limit:
            break
        try:
            host = validate_ip(host)
        except ValueError:
            counts["invalid"] += 1
            continue
        if not force and _recently_synced(host):
            counts["cooldown"] += 1
            syncs_skipped_total.labels(reason="cooldown").inc()
            continue
        with _in_flight_lock:
            if len(_in_flight) >= _MAX_QUEUE:
                counts["queue_full"] += 1
                syncs_skipped_total.labels(reason="queue_full").inc()
                continue
            if not _claim_host(host):
                counts["in_progress"] += 1
                syncs_skipped_total.labels(reason="in_progress").inc()
                continue
        sync_in_progress.inc()
        submit(host)
        counts["queued"] += 1
    return counts


# Query params consumed by /sync/all itself; anything else is forwarded to the
# Netbox device filter (e.g. last_updated__lt=..., name__ic=..., site=..., role=...).
_SYNC_ALL_PARAMS = {
    "limit", "force", "sync_mac", "sync_ip", "sync_modules", "sync_sfp",
    "sync_poe", "housekeeping", "lldp_clear_stale",
}


@app.api_route(
    "/sync/all",
    methods=["POST"],
    status_code=202,
    dependencies=[Depends(require_auth)],
    summary="Queue a sync for every device known to Netdisco",
)
async def sync_all(
    request: Request,
    limit: Annotated[Optional[int], Query(description="Max devices to enqueue (for batching)")] = None,
    force: Annotated[bool, Query(description="Ignore the sync cooldown window")] = False,
    sync_mac: Annotated[bool, Query(description="Sync MAC addresses")] = _DEFAULT_MAC,
    sync_ip: Annotated[bool, Query(description="Sync IP addresses")] = _DEFAULT_IP,
    sync_modules: Annotated[bool, Query(description="Sync module bays / modules")] = _DEFAULT_MODULES,
    sync_sfp: Annotated[bool, Query(description="Sync SFP inventory items")] = _DEFAULT_SFP,
    sync_poe: Annotated[bool, Query(description="Sync PoE mode")] = _DEFAULT_POE,
    housekeeping: Annotated[bool, Query(description="Remove stale device bays and empty dummy interfaces")] = _DEFAULT_HOUSEKEEPING,
    lldp_clear_stale: Annotated[bool, Query(description="Clear LLDP neighbor fields and delete stale discobox-owned cables when no neighbor is present")] = _DEFAULT_LLDP_CLEAR_STALE,
) -> dict:
    """
    Queue a sync for many devices, applying the same guards as the /sync
    webhook (cooldown, queue cap, in-flight dedup). Returns immediately
    with per-guard counters.

    By default the device list comes from Netdisco. Any additional query
    parameter is forwarded to the Netbox device filter instead — e.g.
    `?last_updated__lt=2026-10-01`, `?name__ic=foo`, `?site=zrh&role=switch`
    — and the selection is then limited to devices Netdisco knows (others
    are counted as not_in_netdisco; they cannot sync anyway).

    Syncs drain at DISCOBOX_MAX_CONCURRENT_SYNCS, so a full run over a large
    inventory takes a while — use `limit` to batch, `force` to bypass the
    cooldown for devices synced recently.
    """
    nb_filter: dict[str, Any] = {}
    for k, v in request.query_params.multi_items():
        if k in _SYNC_ALL_PARAMS:
            continue
        if k in nb_filter:
            if not isinstance(nb_filter[k], list):
                nb_filter[k] = [nb_filter[k]]
            nb_filter[k].append(v)
        else:
            nb_filter[k] = v

    loop = asyncio.get_running_loop()
    nd = _get_netdisco_client()
    try:
        devices = await loop.run_in_executor(None, nd.get_all_devices)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"could not fetch Netdisco device list: {exc}")
    nd_ips = {d["ip"] for d in devices if d.get("ip")}

    not_in_netdisco = 0
    if nb_filter:
        def _netbox_hosts() -> list[str]:
            nb = _get_netbox_client()
            hosts = []
            for d in nb.nb.dcim.devices.filter(**{"has_primary_ip": True, **nb_filter}):
                if not d.primary_ip4:
                    continue
                if not force and _CF_TOUCH:
                    cf = dict(getattr(d, "custom_fields", {}) or {})
                    if _recently_touched(cf.get(_CF_TOUCH), _TOUCH_COOLDOWN_DAYS):
                        continue
                hosts.append(str(d.primary_ip4).split("/")[0])
            return hosts
        try:
            selected = await loop.run_in_executor(None, _netbox_hosts)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Netbox device filter failed: {exc}")
        hosts = [h for h in selected if h in nd_ips]
        not_in_netdisco = len(selected) - len(hosts)
    else:
        hosts = sorted(nd_ips)

    def submit(host: str) -> None:
        loop.run_in_executor(None, partial(
            _run_sync, host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe,
            housekeeping, lldp_clear_stale,
            _CF_NEIGHBOR_TEXT, _CF_NEIGHBOR_PORT, _CF_NEIGHBOR_DEVICE, _CF_NEIGHBOR_IFACE,
            _CABLE_SCOPE, _CABLE_SOURCE_CF, _CABLE_SOURCE_VALUE,
            _IFACE_SOURCE_CF, _IFACE_SOURCE_VALUE,
            _CF_OS_VERSION, _CF_OS_NAME, _CF_OS_RELEASE, _CF_STACK_MEMBERS, _STACK_MEMBERS_ONLY_INCREASE,
            _CF_TOUCH, 0 if force else _TOUCH_COOLDOWN_DAYS,
            _HA_METRICS_URL, _HA_METRICS_METRIC, _HA_METRICS_HOSTNAME_LABEL, _HA_METRICS_TARGET_LABEL,
            _HA_METRICS_TIMEOUT, _LIVENESS_TLS_VERIFY, _HA_METRICS_INSTANCE_LABEL, _HA_MGMT_IFACE_NAME,
        ))

    counts = _enqueue_all(hosts, submit, limit=limit, force=force)
    logger.info("sync/all: %s (selected=%d filter=%r)", counts, len(hosts), nb_filter or "netdisco")
    result = {"status": "queued", "selected": len(hosts), "netdisco_total": len(nd_ips), **counts}
    if nb_filter:
        result["netbox_filter"] = nb_filter
        result["not_in_netdisco"] = not_in_netdisco
    return result


@app.get(_METRICS_PATH, include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(_custom_registry), media_type=CONTENT_TYPE_LATEST)


@app.api_route("/sync/pause", methods=["POST"], dependencies=[Depends(require_auth)], summary="Pause queued syncs")
async def pause() -> dict:
    """Hold queued syncs from starting. Already-running syncs finish."""
    _set_paused(True)
    sync_paused.set(1)
    logger.warning("Sync paused: %d task(s) queued", len(_in_flight))
    return {"status": "paused", "queued": len(_in_flight)}


@app.api_route("/sync/resume", methods=["POST"], dependencies=[Depends(require_auth)], summary="Resume queued syncs")
async def resume() -> dict:
    """Release the pause gate; queued syncs start draining (up to MAX_CONCURRENT at a time)."""
    _set_paused(False)
    sync_paused.set(0)
    logger.info("Sync resumed: %d task(s) queued", len(_in_flight))
    return {"status": "running", "queued": len(_in_flight)}


def _fmt_ts(ts: Optional[float]) -> str:
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts)) if ts else ""


def _fmt_ago(ts: Optional[float]) -> str:
    """5m ago / in 3h (rounded to the largest unit)."""
    if not ts:
        return ""
    delta = ts - time.time()
    secs = abs(delta)
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs >= size:
            text = f"{int(secs // size)}{unit}"
            break
    else:
        text = f"{int(secs)}s"
    return f"in {text}" if delta > 0 else f"{text} ago"


def _fmt_every(seconds: Optional[float]) -> str:
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds and seconds >= size and seconds % size == 0:
            return f"every {int(seconds // size)}{unit}"
    return f"every {int(seconds or 0)}s"


_templates = Jinja2Templates(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"))
_templates.env.filters.update(ts=_fmt_ts, ago=_fmt_ago, every=_fmt_every)


def _types_summary() -> Optional[dict]:
    """Dummy type counts from the last types-bare refresh (None: no source CF, so no tracking)."""
    if "types-bare" not in _jobs:
        return None
    samples = [smp for m in types_bare.collect() for smp in m.samples if smp.name == "discobox_types_bare"]
    return {
        "dummies": len(samples),
        "available": sum(1 for smp in samples if smp.labels.get("library") == "available"),
        "checked": (_jobs.last("types-bare") or {}).get("finished"),
    }


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index(request: Request) -> HTMLResponse:
    with _unknown_devices_lock:
        unknown = sorted(_load_unknown_devices().values(), key=lambda d: d["last_seen"], reverse=True)
    with _reconcile_gaps_lock:
        not_in_netdisco = _load_gap(_NOT_IN_NETDISCO_FILE)
        not_in_netbox = _load_gap(_NOT_IN_NETBOX_FILE)
        tag_mismatches = _load_gap(_TAG_MISMATCHES_FILE)
        ip_mismatches = _load_gap(_IP_MISMATCHES_FILE)
    ls = _load_liveness_status() if _LIVENESS_URL else {}
    return _templates.TemplateResponse(request, "index.html", {
        "version": __version__,
        "paused": _is_paused(),
        "breaker": _cb_is_active(),
        "in_flight": _inflight_hosts(),
        "retries": _retries_snapshot(),
        "liveness": {"enabled": bool(_LIVENESS_URL), "ok": ls.get("ok"), "devices": ls.get("devices", 0),
                     "checked": ls.get("checked"), "error": ls.get("error")},
        "jobs": _jobs.snapshot(),
        "missing": [d for d in not_in_netdisco if d.get("status") != "down"],
        "offline": [d for d in not_in_netdisco if d.get("status") == "down"],
        "not_in_netbox": [d for d in not_in_netbox if not d.get("stale")],
        "stale": [d for d in not_in_netbox if d.get("stale")],
        "stale_hours": f"{_NOT_IN_NETBOX_STALE_HOURS:g}" if _NOT_IN_NETBOX_STALE_HOURS else "",
        "tag_mismatches": tag_mismatches,
        "ip_mismatches": ip_mismatches,
        "unknown": unknown,
        "types": _types_summary(),
        "yesterday": (date.today() - timedelta(days=1)).isoformat(),
    })


def _retries_snapshot() -> dict:
    """The per-device retry queue (drained every 10s, not a job): pending count and the next one due."""
    with _retry_lock:
        due = [v["retry_after"] for v in _retry_pending.values()]
    return {"pending": len(due), "next_due": min(due) if due else None}


@app.get("/jobs", summary="Every job: schedule, last run and result, next run; plus the retry queue")
def jobs_list() -> dict:
    return {"jobs": _jobs.snapshot(), "retries": _retries_snapshot()}


@app.post(
    "/jobs/{name}/run",
    dependencies=[Depends(require_auth)],
    summary="Run a job now: 202 in the background, or with ?wait=true its result (409 while it's running)",
    status_code=202,
)
def jobs_run(
    name: str,
    response: Response,
    wait: Annotated[bool, Query(description="Run in this request and answer with the result")] = False,
    max_enqueue: Annotated[Optional[int], Query(description="discover-missing / fix-tags: max devices to enqueue")] = None,
    offset: Annotated[Optional[int], Query(description="discover-missing: skip the first N missing devices")] = None,
) -> dict:
    # Plain def: a ?wait=true run blocks FastAPI's threadpool, not the event loop.
    if name not in _jobs:
        raise HTTPException(404, f"unknown job {name!r}, known: {', '.join(_jobs.names())}")
    kwargs: dict = {}
    if name == "discover-missing":
        kwargs = {"max_enqueue": max_enqueue, "offset": offset, "manual": True}
    elif name == "fix-tags":
        kwargs = {"max_enqueue": max_enqueue}
    if wait:
        run = _jobs.run(name, **kwargs)
        if run is None:
            raise HTTPException(409, f"job {name} is already running")
        response.status_code = 200
        return {"job": name, **{k: v for k, v in run.items() if k != "data"}}
    if not _jobs.start(name, **kwargs):
        raise HTTPException(409, f"job {name} is already running")
    return {"job": name, "status": "started"}


@app.get("/config", dependencies=[Depends(require_auth)], summary="Effective settings (secrets masked) and where they came from")
def config_view() -> dict:
    return effective(_S, _CFG_SOURCE)


@app.get("/health", summary="Liveness check")
async def health() -> dict:
    return {"status": "ok", "version": __version__, "paused": _is_paused(), "in_flight": _inflight_hosts()}


@app.get("/stats", summary="Aggregated operational status as JSON (e.g. for a Netbox backend-status widget)")
async def stats() -> dict:
    with _unknown_devices_lock:
        unknown_devices_count = len(_load_unknown_devices())
    with _reconcile_gaps_lock:
        not_in_netdisco_list = _load_gap(_NOT_IN_NETDISCO_FILE)
        not_in_netbox_list = _load_gap(_NOT_IN_NETBOX_FILE)
        tag_mismatches_count = len(_load_gap(_TAG_MISMATCHES_FILE))
    return {
        "version": __version__,
        "paused": _is_paused(),
        "in_flight": len(_inflight_hosts()),
        "last_reconcile": _jobs.last_success("gaps"),      # when the gap counts below are from
        "last_discover_missing": _jobs.last_success("discover-missing"),
        "unknown_devices_count": unknown_devices_count,
        "not_in_netdisco_count": sum(1 for d in not_in_netdisco_list if d.get("status") != "down"),
        "not_in_netdisco_offline_count": sum(1 for d in not_in_netdisco_list if d.get("status") == "down"),
        "not_in_netbox_count": sum(1 for d in not_in_netbox_list if not d.get("stale")),
        "not_in_netbox_stale_count": sum(1 for d in not_in_netbox_list if d.get("stale")),
        "tag_mismatches_count": tag_mismatches_count,
        "liveness_enabled": bool(_LIVENESS_URL),
        "liveness": _load_liveness_status() or None,
    }


def _nb_device_for_host(nb: NetboxClient, host: str, ip: Optional[str]):
    """Netbox device for a /discover host: by IP (incl. hostname/serial fallbacks) or by name/short name."""
    if ip:
        return nb.find_device_by_ip(ip)
    hits = list(nb.nb.dcim.devices.filter(name__ie=host))
    if not hits:
        short = host.split(".", 1)[0].lower()
        hits = [d for d in nb.nb.dcim.devices.filter(name__isw=short) if (d.name or "").split(".", 1)[0].lower() == short]
    return hits[0] if len(hits) == 1 else None


@app.api_route(
    "/discover",
    methods=["POST"],
    dependencies=[Depends(require_auth)],
    summary="Enqueue a Netdisco discover for one device, with its Netbox SNMP profile and timeout",
)
def discover(
    host: Annotated[str, Query(description="Device IP, or its Netbox name (then its primary IP is used)")],
    tag: Annotated[Optional[str], Query(description="Netdisco device_auth tag hint (default: Netbox snmp_auth_profile)")] = None,
    timeout: Annotated[Optional[str], Query(description="Per-request SNMP timeout, e.g. 10s, used as-is (default: derived from Netbox snmp_polling_timeout, bounded; see netdisco.discover_snmptimeout)")] = None,
) -> dict:
    """
    One Netdisco discover job, the way reconcile enqueues them. With tag and
    timeout both given (and an IP as host), nothing is looked up; otherwise the
    Netbox device supplies whatever is missing. A device unknown to Netbox is
    still enqueued (no hint, default timeout) when host is an IP.

    An explicit ?timeout= is the per-request value and is used as-is, not
    clamped (a deliberate manual override, e.g. to test 10s on one device);
    the CF snmp_polling_timeout is a whole-device budget and gets derived and
    bounded (discover_snmptimeout_us).
    """
    try:
        ip: Optional[str] = validate_ip(host)
    except ValueError:
        ip = None
    timeout_us = None
    if timeout is not None:
        timeout_us = _parse_snmp_timeout_us(timeout)
        if timeout_us is None:
            raise HTTPException(400, f"invalid timeout {timeout!r} (e.g. 30s, 3m, 500ms)")
    # where each value came from: param | netbox | default (none/3s)
    tag_source = "param" if tag is not None else "default"
    timeout_source = "param" if timeout is not None else "default"
    device_name = None
    if ip is None or tag is None or timeout is None:
        dev = _nb_device_for_host(_get_netbox_client(), host, ip)
        if dev is not None:
            device_name = dev.name
            cf = dict(getattr(dev, "custom_fields", {}) or {})
            if tag is None and read_cf(cf, "auth_profile"):
                tag, tag_source = read_cf(cf, "auth_profile"), "netbox"
            budget = read_cf(cf, "snmp_timeout")
            if timeout is None and _parse_snmp_timeout_us(budget) is not None:
                timeout_us, timeout_source = discover_snmptimeout_us(budget), "netbox"
            if ip is None and dev.primary_ip4:
                ip = str(dev.primary_ip4).split("/")[0]
        elif ip is None:
            raise HTTPException(404, f"{host!r} is no IP and no (unique) Netbox device")
    if ip is None:
        raise HTTPException(422, f"Netbox device {device_name!r} has no primary IPv4 to discover")
    if timeout_us is None:
        timeout_us = discover_snmptimeout_us(None)     # the policy floor
    try:
        _get_netdisco_client().enqueue_discover(ip, device_auth_tag_hint=tag, snmp_timeout_us=timeout_us)
    except Exception as exc:
        raise HTTPException(502, f"Netdisco enqueue failed: {exc}")
    effective_timeout = timeout_us
    logger.info("Discover enqueued for %s (%s) device_auth_tag_hint=%r [%s] timeout=%dus [%s]",
                ip, device_name or host, tag, tag_source, effective_timeout, timeout_source)
    return {"status": "enqueued", "host": ip, "device": device_name,
            "tag": tag, "tag_source": tag_source,
            "timeout_us": effective_timeout, "timeout_source": timeout_source}


@app.get(
    "/gaps",
    dependencies=[Depends(require_auth)],
    summary="The last gaps result (Netbox ↔ Netdisco); ?refresh=true runs the gaps job in the background",
)
def gaps_view(
    lists: Annotated[bool, Query(description="Include the device lists, not only the counts")] = False,
    refresh: Annotated[bool, Query(description="Start the gaps job in the background (unless it's running)")] = False,
    wait: Annotated[bool, Query(description="Run the gaps job now and answer when done (slow: may exceed proxy timeouts)")] = False,
) -> dict:
    """
    The last gaps result, immediately. A run takes longer than a proxy/route
    timeout, so it runs in the background: ?refresh starts one (also started
    when there's no result yet), `running` says one is in progress.
    """
    # Plain def: runs in FastAPI's threadpool
    if wait:
        run = _jobs.run("gaps", wait_if_running=True)
        if run and run["status"] == "error":
            raise HTTPException(502, f"gaps failed: {run['error']}")
    last = _jobs.last("gaps")
    started = False
    if not wait and (refresh or not last):
        started = _jobs.start("gaps")
    result: dict = _load_gaps_counts()
    success = _jobs.last_success("gaps")
    result.update({
        "finished": success,
        "age_s": round(time.time() - success) if success else None,
        "last_run": {k: last.get(k) for k in ("finished", "duration_s", "status", "error")} if last else None,
        "running": _jobs.is_running("gaps"),
        "refresh_started": started,
    })
    if lists:
        with _reconcile_gaps_lock:
            result["not_in_netdisco_list"] = _load_gap(_NOT_IN_NETDISCO_FILE)
            result["not_in_netbox_list"] = _load_gap(_NOT_IN_NETBOX_FILE)
            result["tag_mismatches_list"] = _load_gap(_TAG_MISMATCHES_FILE)
            result["ip_mismatches_list"] = _load_gap(_IP_MISMATCHES_FILE)
    return result


@app.api_route(
    "/types/library",
    methods=["GET", "POST"],
    dependencies=[Depends(require_auth)],
    summary="Enrich DeviceTypes from the devicetype-library (fill-blank; dry-run unless POST apply=true)",
)
def types_library(
    request: Request,
    role: Annotated[Optional[list[str]], Query(description="Device role slug(s), name(s) or id(s) whose DeviceTypes to include (default: library.roles)")] = None,
    type: Annotated[Optional[list[str]], Query(description="DeviceType model/slug/part_number(s) to include")] = None,
    bare: Annotated[bool, Query(description="Include every type discobox created without templates (source=netdisco, or its creation comment)")] = False,
    apply: Annotated[bool, Query(description="Write the changes (POST only)")] = False,
) -> dict:
    # Plain def: FastAPI runs it in its threadpool, the Netbox calls don't block the event loop.
    if apply and request.method != "POST":
        raise HTTPException(405, "apply=true needs POST")
    types = type or []
    # library.roles is only the default selection: an explicit type= alone must not pull it in
    roles = role if role is not None else ([] if types or bare else _LIBRARY_ROLES)
    if not roles and not types and not bare:
        raise HTTPException(400, "Nothing selected: pass role=, type= and/or bare=true (or set library.roles)")
    try:
        result = sync_types(
            _get_netbox_client(), _get_library(), _LIBRARY_MAPPING, roles, types, apply=apply, bare=bare,
            source_cf=_IFACE_SOURCE_CF, bare_value=_IFACE_SOURCE_VALUE, library_value=_LIBRARY_SOURCE_VALUE,
        )
        if apply:
            _refresh_types_bare_safe()
        return result
    except ValueError as exc:            # e.g. an unknown role
        raise HTTPException(400, str(exc))
    except Exception as exc:
        logger.error("types/library failed: %s", exc)
        raise HTTPException(502, f"types/library failed: {exc}")


@app.get("/unknown-devices", summary="Devices Netdisco sent a sync hook for but not in Netbox")
async def unknown_devices() -> list:
    with _unknown_devices_lock:
        devices = _load_unknown_devices()
    return sorted(devices.values(), key=lambda d: d["last_seen"], reverse=True)


@app.get("/not-in-netdisco", summary="Active Netbox devices not found in Netdisco (last reconcile; includes liveness-down entries, see 'status')")
async def not_in_netdisco() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_NOT_IN_NETDISCO_FILE)


@app.get("/not-in-netbox", summary="Netdisco devices not found in Netbox (last reconcile), with last_discover and stale")
async def not_in_netbox() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_NOT_IN_NETBOX_FILE)


@app.get("/tag-mismatches", summary="Netdisco devices with a missing or Netbox-mismatched auth tag (last reconcile)")
async def tag_mismatches() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_TAG_MISMATCHES_FILE)


@app.get("/ip-mismatches", summary="Devices in both by hostname but under a different IP in Netbox and Netdisco (last gaps run)")
async def ip_mismatches() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_IP_MISMATCHES_FILE)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import socket
    port = int(os.getenv("DISCOBOX_LISTEN_PORT", "8080"))
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"
    logger.info("discobox v%s starting on port %d: outbound IP: %s", __version__, port, local_ip)
    # the app object, not "server:app": one import of this module, one process
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=_UVICORN_LOG_CONFIG)
