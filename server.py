#!/usr/bin/env python3
"""
discobox server: FastAPI webhook receiver + Prometheus metrics.

Netdisco calls POST /sync after each discovery job.
Syncs run in a background thread pool; duplicate requests for the
same host are dropped while a sync is already in progress.

Endpoints:
  POST /sync             Trigger a device sync
  POST /sync/pause       Hold queued syncs from starting
  POST /sync/resume      Release the pause gate
  GET  /metrics          Prometheus metrics
  GET  /health           Liveness check
  GET  /docs             Swagger UI (auto-generated)
"""

import asyncio
from collections import deque
from functools import partial
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Optional

from requests.exceptions import HTTPError, ReadTimeout

import yaml

import uvicorn
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.multiprocess import MultiProcessCollector
from pydantic import BaseModel

from discobox import NetboxClient, NetdiscoClient, reconcile_devices, sync_device, validate_ip

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

# ── Prometheus metrics ─────────────────────────────────────────────────────────
# Multi-worker support: set PROMETHEUS_MULTIPROC_DIR to a writable directory so
# all uvicorn workers share metrics via the filesystem. Without it, each worker
# has its own in-memory state and the scrape target rotates between them.

_MULTIPROC_DIR = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus")
os.makedirs(_MULTIPROC_DIR, exist_ok=True)
os.environ["PROMETHEUS_MULTIPROC_DIR"] = _MULTIPROC_DIR

# Always register metrics to a private registry so the default global REGISTRY
# (and its built-in process/GC collectors) is never touched. In multiprocess mode,
# prometheus_client writes metric values to files via _ValueClass regardless of
# which registry the metric is attached to, so MultiProcessCollector at scrape
# time picks them up correctly.
_custom_registry = CollectorRegistry()
_reg = {"registry": _custom_registry}

hooks_received_total = Counter(
    "discobox_hooks_received_total",
    "Total webhook POST /sync calls received (including skipped/invalid)",
    **_reg,
)
syncs_total = Counter(
    "discobox_syncs_total",
    "Completed device syncs",
    ["status"],   # success | error
    **_reg,
)
sync_duration = Histogram(
    "discobox_sync_duration_seconds",
    "Time spent syncing a device",
    buckets=[5, 10, 30, 60, 120, 300],
    **_reg,
)
sync_in_progress = Gauge(
    "discobox_sync_queued",
    "Device syncs in flight: waiting for a slot + actively running",
    multiprocess_mode="livesum",
    **_reg,
)
sync_running = Gauge(
    "discobox_sync_running",
    "Device syncs actively running (semaphore slot acquired)",
    multiprocess_mode="livesum",
    **_reg,
)
interfaces_total = Counter(
    "discobox_interfaces_total",
    "Interfaces processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    **_reg,
)
ips_total = Counter(
    "discobox_ips_total",
    "IP addresses processed across all syncs",
    ["action"],   # created | fixed | moved | unchanged | skipped | error
    **_reg,
)
modules_total = Counter(
    "discobox_modules_total",
    "Modules processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    **_reg,
)
sfps_total = Counter(
    "discobox_sfps_total",
    "SFP inventory items processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    **_reg,
)
syncs_skipped_total = Counter(
    "discobox_syncs_skipped_total",
    "Sync requests dropped because the host was already being synced",
    **_reg,
)
ha_vip_total = Counter(
    "discobox_ha_vip_total",
    "HA VIP redirections detected and handled",
    **_reg,
)
device_sync_duration = Gauge(
    "discobox_device_last_sync_duration_seconds",
    "Duration of the last completed sync for each device",
    ["instance"],
    multiprocess_mode="livemax",
    **_reg,
)
device_sync_timestamp = Gauge(
    "discobox_device_last_sync_timestamp_seconds",
    "Unix timestamp of the last completed sync for each device",
    ["instance"],
    multiprocess_mode="livemax",
    **_reg,
)
sync_paused = Gauge(
    "discobox_sync_paused",
    "1 if sync intake is paused, 0 if running",
    multiprocess_mode="livemax",
    **_reg,
)
device_sync_failed = Gauge(
    "discobox_device_last_sync_failed",
    "1 if the last sync attempt for this device failed, 0 if it succeeded",
    ["instance"],
    multiprocess_mode="livemax",
    **_reg,
)
reconcile_netbox_devices = Gauge(
    "discobox_reconcile_netbox_devices",
    "Active Netbox devices with a primary IP seen during last reconcile",
    multiprocess_mode="livemax",
    **_reg,
)
reconcile_netdisco_devices = Gauge(
    "discobox_reconcile_netdisco_devices",
    "Devices known to Netdisco seen during last reconcile",
    multiprocess_mode="livemax",
    **_reg,
)
reconcile_enqueued_total = Counter(
    "discobox_reconcile_enqueued_total",
    "Devices enqueued for Netdisco discovery by the reconcile loop",
    **_reg,
)
reconcile_runs_total = Counter(
    "discobox_reconcile_runs_total",
    "Reconcile loop runs",
    ["status"],   # success | error
    **_reg,
)
reconcile_last_run_timestamp = Gauge(
    "discobox_reconcile_last_run_timestamp_seconds",
    "Unix timestamp of the last completed reconcile run",
    multiprocess_mode="livemax",
    **_reg,
)
reconcile_aborted_total = Counter(
    "discobox_reconcile_aborted_total",
    "Reconcile runs skipped because the Netdisco queue exceeded thresholds",
    **_reg,
)
unknown_devices_total = Counter(
    "discobox_unknown_devices_total",
    "Sync webhooks received for devices not found in Netbox",
    **_reg,
)
reconcile_not_in_netdisco = Gauge(
    "discobox_reconcile_not_in_netdisco",
    "Active Netbox devices not found in Netdisco (last reconcile)",
    multiprocess_mode="livemax",
    **_reg,
)
reconcile_not_in_netbox = Gauge(
    "discobox_reconcile_not_in_netbox",
    "Netdisco devices not found in Netbox (last reconcile)",
    multiprocess_mode="livemax",
    **_reg,
)
sync_timeouts_total = Counter(
    "discobox_sync_timeouts_total",
    "Syncs that failed with a timeout or gateway error",
    **_reg,
)
circuit_breaker_trips_total = Counter(
    "discobox_circuit_breaker_trips_total",
    "Times the circuit breaker tripped due to repeated overload errors",
    **_reg,
)
sync_retries_total = Counter(
    "discobox_sync_retries_total",
    "Per-device sync retries scheduled after a timeout",
    **_reg,
)

# ── Config ─────────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    path = os.getenv("DISCOBOX_CONFIG") or "discobox.yaml"
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        logging.getLogger("discobox").info("Config loaded from %s", path)
        return data
    except FileNotFoundError:
        if os.getenv("DISCOBOX_CONFIG"):
            logging.getLogger("discobox").warning("Config file not found: %s", path)
        return {}
    except Exception as exc:
        logging.getLogger("discobox").error("Failed to load config %s: %s", path, exc)
        return {}

_MISSING = object()

def _c(cfg: dict, *keys: str, default: Any = None) -> Any:
    """Navigate nested config dict; return default when key is missing.
    Explicit null/~ in YAML returns None, not the default."""
    node = cfg
    for k in keys:
        if not isinstance(node, dict) or k not in node:
            return default
        node = node[k]
    return node

def _cstr(cfg: dict, *keys: str, default: Optional[str] = None) -> Optional[str]:
    """Like _c but returns None for empty strings (disables the feature).
    Explicit null/~ disables; missing key falls back to default."""
    val = _c(cfg, *keys, default=_MISSING)
    if val is _MISSING:
        return default
    if val is None:
        return None
    return str(val).strip() or None

def _cbool(cfg: dict, *keys: str, default: bool = False) -> bool:
    val = _c(cfg, *keys, default=_MISSING)
    if val is _MISSING or val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("1", "true", "yes")

def _clist(cfg: dict, *keys: str) -> list[str]:
    """Return a list of strings from a YAML list or comma-separated string; empty list if missing."""
    val = _c(cfg, *keys, default=None)
    if not val:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if str(v).strip()]
    return [v.strip() for v in str(val).split(",") if v.strip()]

_CFG = _load_config()

# ── Auth ───────────────────────────────────────────────────────────────────────

_AUTH_TOKEN: Optional[str] = os.getenv("DISCOBOX_AUTH_TOKEN") or _cstr(_CFG, "auth", "token")
_METRICS_PATH: str = _c(_CFG, "auth", "metrics_path", default=os.getenv("DISCOBOX_METRICS_PATH", "/metrics"))

# ── Sync defaults (overridable per-request) ────────────────────────────────────

_SYNC_COOLDOWN: int       = int(_c(_CFG, "sync", "cooldown", default=3600))  # seconds; 0 = disabled
_DEFAULT_MAC              = not _cbool(_CFG, "sync", "no_mac",          default=False)
_DEFAULT_IP               = not _cbool(_CFG, "sync", "no_ip",           default=False)
_DEFAULT_MODULES          = not _cbool(_CFG, "sync", "no_modules",      default=False)
_DEFAULT_SFP              = not _cbool(_CFG, "sync", "no_sfp",          default=False)
_DEFAULT_POE              = not _cbool(_CFG, "sync", "no_poe",          default=False)
_DEFAULT_HOUSEKEEPING     =     _cbool(_CFG, "sync", "housekeeping",    default=False)
_DEFAULT_LLDP_CLEAR_STALE =     _cbool(_CFG, "sync", "lldp_clear_stale", default=False)
_VIP_MODE: str            = _c   (_CFG, "sync", "vip_mode",            default="threenode")
_PAUSE_ON_ERROR: bool     = _cbool(_CFG, "sync", "pause_on_error",     default=False)

# Circuit breaker: trip when >= _CB_THRESHOLD timeouts occur within _CB_WINDOW seconds.
# New syncs are held for _CB_BACKOFF seconds, then automatically released.
_CB_WINDOW:    int = int(os.getenv("DISCOBOX_CB_WINDOW",    "120"))  # look-back window (s)
_CB_THRESHOLD: int = int(os.getenv("DISCOBOX_CB_THRESHOLD", "3"))    # timeouts to trip
_CB_BACKOFF:   int = int(os.getenv("DISCOBOX_CB_BACKOFF",   "120"))  # pause duration (s)

# Per-device retry on isolated timeouts (circuit breaker not tripped).
_RETRY_DELAY: int = int(os.getenv("DISCOBOX_RETRY_DELAY", "60"))  # seconds before retry
_RETRY_MAX:   int = int(os.getenv("DISCOBOX_RETRY_MAX",   "2"))   # max retries per device

_CF_NEIGHBOR_TEXT:   Optional[str] = _cstr(_CFG, "custom_fields", "neighbor_text",   default="neighbor")
_CF_NEIGHBOR_PORT:   Optional[str] = _cstr(_CFG, "custom_fields", "neighbor_port",   default="neighbor_port")
_CF_NEIGHBOR_DEVICE: Optional[str] = _cstr(_CFG, "custom_fields", "neighbor_device", default="neighbor_device")
_CF_NEIGHBOR_IFACE:  Optional[str] = _cstr(_CFG, "custom_fields", "neighbor_iface",  default="neighbor_iface")

_CABLE_SCOPE:        str            = _c   (_CFG, "cabling", "scope",        default="site")
_CABLE_SOURCE_CF:    Optional[str]  = _cstr(_CFG, "cabling", "source_cf",    default="source")
_CABLE_SOURCE_VALUE: Optional[str]  = _cstr(_CFG, "cabling", "source_value", default="netdisco")

_IFACE_SOURCE_CF:    Optional[str]  = _cstr(_CFG, "custom_fields", "source",       default="source")
_IFACE_SOURCE_VALUE: str            = _c   (_CFG, "custom_fields", "source_value", default="netdisco")

_CF_OS_VERSION:      Optional[str]  = _cstr(_CFG, "custom_fields", "os_version", default="os_version")
_CF_OS_NAME:         Optional[str]  = _cstr(_CFG, "custom_fields", "os_name",    default="os_name")
_CF_OS_RELEASE:      Optional[str]  = _cstr(_CFG, "custom_fields", "os_release", default="os_release")


async def require_auth(authorization: Annotated[str, Header()] = "") -> None:
    """Bearer token auth. Disabled if DISCOBOX_AUTH_TOKEN is not set."""
    if not _AUTH_TOKEN:
        return
    if authorization != f"Bearer {_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


# ── Reconcile loop ─────────────────────────────────────────────────────────────

_RECONCILE_INTERVAL:      int           = int(_c(_CFG, "reconcile", "interval",       default=24 * 3600))
_RECONCILE_MAX_QUEUED:    Optional[int] = _c (_CFG, "reconcile", "max_queued",     default=None)
_RECONCILE_MAX_FAILED:    Optional[int] = _c (_CFG, "reconcile", "max_failed",     default=None)
_RECONCILE_MAX_ENQUEUE:   Optional[int] = _c (_CFG, "reconcile", "max_enqueue",    default=None)
_RECONCILE_ROLES:         list[str]     = _clist(_CFG, "reconcile", "roles") or ["router", "switch", "firewall"]
_RECONCILE_REQUIRE_AUTH_TAG: bool       = _cbool(_CFG, "reconcile", "require_auth_tag", default=False)

_AUTO_CREATE_ROLE:     Optional[str] = _cstr (_CFG, "auto_create", "role",     default=None)
_AUTO_CREATE_SITE:     Optional[str] = _cstr (_CFG, "auto_create", "site",     default=None)
_AUTO_CREATE_STATUS:   str           = _c    (_CFG, "auto_create", "status",   default="active")
_AUTO_CREATE_LOCATION: bool          = _cbool(_CFG, "auto_create", "location", default=False)


def _make_netdisco_client() -> NetdiscoClient:
    tls = os.getenv("NETDISCO_TLS_VERIFY", "true").lower() != "false"
    token = os.getenv("NETDISCO_TOKEN")
    if token:
        return NetdiscoClient(base_url=os.environ["NETDISCO_URL"], token=token, verify_tls=tls)
    return NetdiscoClient(
        base_url=os.environ["NETDISCO_URL"],
        username=os.environ["NETDISCO_USERNAME"],
        password=os.environ["NETDISCO_PASSWORD"],
        verify_tls=tls,
    )


def _run_reconcile(max_enqueue: Optional[int] = None, offset: Optional[int] = None) -> None:
    if _is_paused():
        logger.info("Reconcile skipped: sync is paused")
        return
    nd = _make_netdisco_client()
    nb = NetboxClient(
        url=os.environ["NETBOX_URL"],
        token=os.environ["NETBOX_TOKEN"],
        verify_tls=os.getenv("NETBOX_TLS_VERIFY", "true").lower() != "false",
    )
    effective_max = max_enqueue if max_enqueue is not None else _RECONCILE_MAX_ENQUEUE
    counts = reconcile_devices(
        nd, nb,
        max_queued=_RECONCILE_MAX_QUEUED,
        max_failed=_RECONCILE_MAX_FAILED,
        max_enqueue=effective_max,
        offset=offset,
        roles=_RECONCILE_ROLES,
        require_auth_tag=_RECONCILE_REQUIRE_AUTH_TAG,
        auto_create_role=_AUTO_CREATE_ROLE,
        auto_create_site=_AUTO_CREATE_SITE,
        auto_create_status=_AUTO_CREATE_STATUS,
        auto_create_location=_AUTO_CREATE_LOCATION,
        iface_source_cf=_IFACE_SOURCE_CF,
        iface_source_value=_IFACE_SOURCE_VALUE,
    )
    if counts.get("aborted"):
        reconcile_aborted_total.inc()
        return
    reconcile_netbox_devices.set(counts.get("netbox_total", 0))
    reconcile_netdisco_devices.set(counts.get("netdisco_total", 0))
    reconcile_enqueued_total.inc(counts.get("enqueued", 0))
    reconcile_not_in_netdisco.set(counts.get("not_in_netdisco", 0))
    reconcile_not_in_netbox.set(counts.get("not_in_netbox", 0))
    with _reconcile_gaps_lock:
        _save_gap(_NOT_IN_NETDISCO_FILE, counts.get("not_in_netdisco_list", []))
        _save_gap(_NOT_IN_NETBOX_FILE, counts.get("not_in_netbox_list", []))
    reconcile_runs_total.labels(status="success").inc()
    reconcile_last_run_timestamp.set(time.time())


async def _reconcile_loop() -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(_RECONCILE_INTERVAL)
        try:
            await loop.run_in_executor(None, _run_reconcile)
        except Exception as exc:
            logger.error("Reconcile run failed: %s", exc)
            reconcile_runs_total.labels(status="error").inc()


async def _retry_loop() -> None:
    """Drain _retry_pending: re-queue timed-out devices once their backoff has elapsed."""
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(10)
        now = time.time()
        with _retry_lock:
            ready = {h: dict(v) for h, v in _retry_pending.items() if v["retry_after"] <= now}
            for h in ready:
                del _retry_pending[h]
        for host, entry in ready.items():
            retry_count = entry.pop("retry_count")
            entry.pop("retry_after", None)
            with _in_flight_lock:
                if len(_in_flight) >= _MAX_QUEUE:
                    logger.warning("Retry for %s dropped: queue full", host)
                    continue
                if not _claim_host(host):
                    logger.debug("Retry for %s: already in flight, dropping", host)
                    continue
                _in_flight.add(host)
            sync_in_progress.inc()
            logger.info("Retrying sync for %s (attempt %d/%d)", host, retry_count, _RETRY_MAX)
            loop.run_in_executor(None, partial(_run_sync, host, _retry_count=retry_count, **entry))


@asynccontextmanager
async def lifespan(app):
    for f in os.scandir(_INFLIGHT_DIR):
        if f.name.startswith("discobox.inflight."):
            try:
                os.unlink(f.path)
            except OSError:
                pass
    tasks = []
    is_reconcile_leader = _acquire_reconcile_leadership()
    if is_reconcile_leader:
        try:
            tasks.append(asyncio.create_task(_reconcile_loop()))
        except Exception as exc:
            logger.error("Failed to start reconcile loop: %s", exc)
    else:
        logger.info("Reconcile loop not started: another worker already holds leadership")
    try:
        tasks.append(asyncio.create_task(_retry_loop()))
    except Exception as exc:
        logger.error("Failed to start retry loop: %s", exc)
    yield
    for task in tasks:
        task.cancel()
    if is_reconcile_leader:
        _release_reconcile_leadership()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="discobox",
    description="Netdisco → Netbox sync webhook receiver",
    version="1.0.0",
    lifespan=lifespan,
)

# Hosts currently being synced.
# _in_flight is per-process (queue-size tracking / listing only).
# _claim_host / _release_host use O_CREAT|O_EXCL files for cross-worker dedup.
_in_flight: set[str] = set()
_in_flight_lock = threading.Lock()
_INFLIGHT_DIR: str = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp")


def _claim_host(host: str) -> bool:
    """Atomically mark host as in-flight across all workers. Returns False if already claimed."""
    path = os.path.join(_INFLIGHT_DIR, f"discobox.inflight.{host}")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _release_host(host: str) -> None:
    path = os.path.join(_INFLIGHT_DIR, f"discobox.inflight.{host}")
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# Reconcile leader election: with DISCOBOX_WORKERS>1, lifespan() runs once per worker
# process, so only one worker may run _reconcile_loop() or every run's max_enqueue/gap
# scan gets multiplied by the worker count. The lock stores the leader's PID so a crashed
# leader (without a clean shutdown) doesn't permanently wedge reconcile for other workers.
_RECONCILE_LEADER_FILE: str = os.path.join(_INFLIGHT_DIR, "discobox.reconcile-leader.pid")


def _acquire_reconcile_leadership() -> bool:
    for _ in range(2):
        try:
            fd = os.open(_RECONCILE_LEADER_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            return True
        except FileExistsError:
            try:
                with open(_RECONCILE_LEADER_FILE) as f:
                    leader_pid = int(f.read().strip())
                os.kill(leader_pid, 0)
                return False  # leader still alive
            except (OSError, ValueError):
                pass  # stale lock (leader pid gone or file unreadable): reclaim it
            try:
                os.unlink(_RECONCILE_LEADER_FILE)
            except FileNotFoundError:
                pass
    return False


def _release_reconcile_leadership() -> None:
    try:
        os.unlink(_RECONCILE_LEADER_FILE)
    except FileNotFoundError:
        pass


def _recently_synced(host: str) -> bool:
    """Return True if host was successfully synced within the cooldown window."""
    if not _SYNC_COOLDOWN:
        return False
    path = os.path.join(_INFLIGHT_DIR, f"discobox.synced.{host}")
    try:
        return (time.time() - os.stat(path).st_mtime) < _SYNC_COOLDOWN
    except FileNotFoundError:
        return False


def _mark_synced(host: str) -> None:
    path = os.path.join(_INFLIGHT_DIR, f"discobox.synced.{host}")
    try:
        open(path, "w").close()
    except OSError:
        pass

# Devices seen in Netdisco webhooks but not found in Netbox
# Unknown devices: file-backed so all workers share state.
_UNKNOWN_DEVICES_FILE: str = os.path.join(
    os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.unknown.json"
)
_unknown_devices_lock = threading.Lock()

def _load_unknown_devices() -> dict[str, dict]:
    try:
        with open(_UNKNOWN_DEVICES_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_unknown_devices(devices: dict[str, dict]) -> None:
    with open(_UNKNOWN_DEVICES_FILE, "w") as f:
        json.dump(devices, f)

# Reconcile gap lists: replaced wholesale after each reconcile run.
_NOT_IN_NETDISCO_FILE: str = os.path.join(
    os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.not_in_netdisco.json"
)
_NOT_IN_NETBOX_FILE: str = os.path.join(
    os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.not_in_netbox.json"
)
_reconcile_gaps_lock = threading.Lock()

def _load_gap(path: str) -> list[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_gap(path: str, devices: list[dict]) -> None:
    with open(path, "w") as f:
        json.dump(devices, f)

# Limit concurrent Netbox API load: all workers share this semaphore via the
# threading module (workers are forked from the same parent process).
_MAX_CONCURRENT: int = int(os.getenv("DISCOBOX_MAX_CONCURRENT_SYNCS", "3"))
_sync_semaphore = threading.Semaphore(_MAX_CONCURRENT)
_MAX_QUEUE: int = int(os.getenv("DISCOBOX_MAX_QUEUE", "1000"))

# Pause gate: file-based so all workers see it regardless of which handled the request.
# Presence of the file = paused; absence = running.
_PAUSE_FILE: str = os.path.join(
    os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.pause"
)

# Circuit-breaker state: file stores resume-at timestamp (float JSON).
_CB_FILE: str = os.path.join(os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.cb.json")
_recent_timeouts: deque = deque()
_timeout_lock = threading.Lock()

# Per-device retry queue: host → {retry_count, retry_after, **sync_kwargs}
_retry_pending: dict[str, dict] = {}
_retry_lock = threading.Lock()


def _cb_is_active() -> bool:
    try:
        with open(_CB_FILE) as f:
            resume_at = json.load(f)
        if time.time() < resume_at:
            return True
        try:
            os.unlink(_CB_FILE)
        except FileNotFoundError:
            pass
        return False
    except (FileNotFoundError, json.JSONDecodeError):
        return False


def _trip_circuit_breaker() -> None:
    resume_at = time.time() + _CB_BACKOFF
    with open(_CB_FILE, "w") as f:
        json.dump(resume_at, f)
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
        delay = _RETRY_DELAY * (2 ** retry_count)
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


# ── Background sync ────────────────────────────────────────────────────────────

def _run_sync(host: str, sync_mac: bool, sync_ip: bool, sync_modules: bool, sync_sfp: bool, sync_poe: bool, housekeeping: bool, lldp_clear_stale: bool = False, cf_neighbor_text: Optional[str] = None, cf_neighbor_port: Optional[str] = None, cf_neighbor_device: Optional[str] = None, cf_neighbor_iface: Optional[str] = None, cable_scope: str = "", cable_source_cf: Optional[str] = None, cable_source_value: Optional[str] = None, iface_source_cf: Optional[str] = None, iface_source_value: str = "netdisco", cf_os_version: Optional[str] = "os_version", cf_os_name: Optional[str] = "os_name", cf_os_release: Optional[str] = "os_release", _retry_count: int = 0) -> None:
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
        nd = _make_netdisco_client()
        nb = NetboxClient(
            url=os.environ["NETBOX_URL"],
            token=os.environ["NETBOX_TOKEN"],
            verify_tls=os.getenv("NETBOX_TLS_VERIFY", "true").lower() != "false",
            changelog_message="DiscoBox Hook",
        )
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
        )
        status = "success" if result.get("ok") else "error"
        if result.get("reason") == "device_not_found":
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
            _on_timeout(host, _retry_count, dict(
                sync_mac=sync_mac, sync_ip=sync_ip, sync_modules=sync_modules,
                sync_sfp=sync_sfp, sync_poe=sync_poe, housekeeping=housekeeping,
                lldp_clear_stale=lldp_clear_stale, cf_neighbor_text=cf_neighbor_text,
                cf_neighbor_port=cf_neighbor_port, cf_neighbor_device=cf_neighbor_device,
                cf_neighbor_iface=cf_neighbor_iface, cable_scope=cable_scope,
                cable_source_cf=cable_source_cf, cable_source_value=cable_source_value,
                iface_source_cf=iface_source_cf, iface_source_value=iface_source_value,
                cf_os_version=cf_os_version, cf_os_name=cf_os_name, cf_os_release=cf_os_release,
            ))
    finally:
        sync_running.dec()
        _sync_semaphore.release()
        elapsed = time.time() - start
        syncs_total.labels(status=status).inc()
        sync_duration.observe(elapsed)
        sync_in_progress.dec()
        _release_host(host)
        with _in_flight_lock:
            _in_flight.discard(host)
        # Per-device metrics: duration/timestamp only on success so a persistently
        # failing device goes stale and timestamp-based alerts fire correctly.
        # device_sync_failed is always updated so failure is immediately visible.
        instance = result.get("hostname") or host
        device_sync_failed.labels(instance=instance).set(0 if status == "success" else 1)
        if status == "error" and _PAUSE_ON_ERROR and not _is_paused():
            _set_paused(True)
            sync_paused.set(1)
            logger.warning("Sync error for %s: auto-pausing intake (DISCOBOX_PAUSE_ON_ERROR)", host)
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
    lldp_clear_stale: Annotated[bool, Query(description="Clear LLDP neighbor fields when no neighbor is present")] = _DEFAULT_LLDP_CLEAR_STALE,
    debug: Annotated[bool, Query(description="Run synchronously and return debug logs as plain text")] = False,
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

    if _recently_synced(resolved_host):
        logger.debug("hook from %s: %s  cooldown active: skipping", caller, resolved_host)
        syncs_skipped_total.inc()
        return SyncResponse(status="skipped", host=resolved_host, reason="cooldown")

    with _in_flight_lock:
        if len(_in_flight) >= _MAX_QUEUE:
            logger.warning("hook from %s: %s  queue full (%d/%d): dropping", caller, resolved_host, len(_in_flight), _MAX_QUEUE)
            syncs_skipped_total.inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="queue full")
        if not _claim_host(resolved_host):
            logger.info("hook from %s: %s  already in progress: dropping", caller, resolved_host)
            syncs_skipped_total.inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="already in progress")
        _in_flight.add(resolved_host)

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
                _CF_OS_VERSION, _CF_OS_NAME, _CF_OS_RELEASE,
            )
        finally:
            discobox_log.removeHandler(cap)
            discobox_log.setLevel(prev_level)
        return PlainTextResponse("\n".join(cap.lines))

    background_tasks.add_task(_run_sync, resolved_host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe, housekeeping, lldp_clear_stale, _CF_NEIGHBOR_TEXT, _CF_NEIGHBOR_PORT, _CF_NEIGHBOR_DEVICE, _CF_NEIGHBOR_IFACE, _CABLE_SCOPE, _CABLE_SOURCE_CF, _CABLE_SOURCE_VALUE, _IFACE_SOURCE_CF, _IFACE_SOURCE_VALUE, _CF_OS_VERSION, _CF_OS_NAME, _CF_OS_RELEASE)
    logger.info("hook from %s: %s  queued", caller, resolved_host)
    return SyncResponse(status="queued", host=resolved_host)


@app.get(_METRICS_PATH, include_in_schema=False)
async def metrics() -> Response:
    if _MULTIPROC_DIR:
        reg = CollectorRegistry()
        MultiProcessCollector(reg)
        content = generate_latest(reg)
    else:
        content = generate_latest(_custom_registry)
    return Response(content=content, media_type=CONTENT_TYPE_LATEST)


@app.api_route("/sync/pause", methods=["GET", "POST"], dependencies=[Depends(require_auth)], summary="Pause queued syncs")
async def pause() -> dict:
    """Hold queued syncs from starting across all workers. Already-running syncs finish."""
    _set_paused(True)
    sync_paused.set(1)
    logger.warning("Sync paused: %d task(s) queued", len(_in_flight))
    return {"status": "paused", "queued": len(_in_flight)}


@app.api_route("/sync/resume", methods=["GET", "POST"], dependencies=[Depends(require_auth)], summary="Resume queued syncs")
async def resume() -> dict:
    """Release the pause gate; queued syncs start draining (up to MAX_CONCURRENT at a time)."""
    _set_paused(False)
    sync_paused.set(0)
    logger.info("Sync resumed: %d task(s) queued", len(_in_flight))
    return {"status": "running", "queued": len(_in_flight)}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> str:
    paused = _is_paused()
    with _unknown_devices_lock:
        unknown_count = len(_load_unknown_devices())
    in_flight = list(_in_flight)
    last_reconcile = reconcile_last_run_timestamp._value.get() if hasattr(reconcile_last_run_timestamp, "_value") else 0
    last_reconcile_str = (
        time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(last_reconcile))
        if last_reconcile else "never"
    )
    status_color = "#e74c3c" if paused else "#2ecc71"
    status_label = "PAUSED" if paused else "running"
    unknown_rows = ""
    with _unknown_devices_lock:
        for d in sorted(_load_unknown_devices().values(), key=lambda x: x["last_seen"], reverse=True):
            ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(d["last_seen"]))
            unknown_rows += f"<tr><td>{d['ip']}</td><td>{d['hostname']}</td><td>{ts}</td></tr>"
    unknown_section = f"""
    <h2>Unknown devices ({unknown_count})</h2>
    <table><tr><th>IP</th><th>Hostname</th><th>Last seen</th></tr>{unknown_rows}</table>
    """ if unknown_count else "<h2>Unknown devices</h2><p>None</p>"

    with _reconcile_gaps_lock:
        not_in_netdisco_list = _load_gap(_NOT_IN_NETDISCO_FILE)
        not_in_netbox_list = _load_gap(_NOT_IN_NETBOX_FILE)

    def _gap_table(devices: list[dict]) -> str:
        rows = "".join(f"<tr><td>{d['ip']}</td><td>{d['name']}</td></tr>" for d in devices)
        return f"<table><tr><th>IP</th><th>Name</th></tr>{rows}</table>"

    not_in_netdisco_section = (
        f"<h2>In Netbox, not in Netdisco ({len(not_in_netdisco_list)})</h2>{_gap_table(not_in_netdisco_list)}"
        if not_in_netdisco_list else "<h2>In Netbox, not in Netdisco</h2><p>None</p>"
    )
    not_in_netbox_section = (
        f"<h2>In Netdisco, not in Netbox ({len(not_in_netbox_list)})</h2>{_gap_table(not_in_netbox_list)}"
        if not_in_netbox_list else "<h2>In Netdisco, not in Netbox</h2><p>None</p>"
    )

    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>discobox</title>
<style>
  body {{ font-family: monospace; max-width: 860px; margin: 2em auto; padding: 0 1em; background: #1a1a2e; color: #eee; }}
  h1 {{ color: #a29bfe; }} h2 {{ color: #74b9ff; border-bottom: 1px solid #444; padding-bottom: .3em; }}
  .badge {{ display:inline-block; padding: .2em .7em; border-radius: 4px; font-weight: bold; background: {status_color}; color: #fff; }}
  table {{ border-collapse: collapse; width: 100%; }} th, td {{ text-align: left; padding: .4em .8em; border-bottom: 1px solid #333; }}
  th {{ color: #a29bfe; }}
  .endpoints td:first-child {{ color: #55efc4; }} .endpoints td:nth-child(2) {{ color: #fdcb6e; }}
  a {{ color: #74b9ff; }}
</style></head><body>
<h1>discobox</h1>
<p>Status: <span class="badge">{status_label}</span>
&nbsp; In-flight: <b>{len(in_flight)}</b>
&nbsp; Unknown devices: <b>{unknown_count}</b>
&nbsp; Last reconcile: <b>{last_reconcile_str}</b></p>

<h2>Endpoints</h2>
<table class="endpoints">
  <tr><th>Method</th><th>Path</th><th>Description</th></tr>
  <tr><td>POST</td><td><a href=/docs#/default/sync_sync_post>/sync</a></td><td>Netdisco webhook: trigger device sync</td></tr>
  <tr><td>POST</td><td>/sync/pause</td><td>Pause queued syncs</td></tr>
  <tr><td>POST</td><td>/sync/resume</td><td>Resume queued syncs</td></tr>
  <tr><td>POST</td><td>/reconcile</td><td>Trigger reconcile run manually</td></tr>
  <tr><td>GET</td><td><a href=/unknown-devices>/unknown-devices</a></td><td>Devices seen via LLDP but not found in Netbox (JSON)</td></tr>
  <tr><td>GET</td><td><a href=/not-in-netdisco>/not-in-netdisco</a></td><td>Active Netbox devices not in Netdisco (JSON)</td></tr>
  <tr><td>GET</td><td><a href=/not-in-netbox>/not-in-netbox</a></td><td>Netdisco devices not in Netbox (JSON)</td></tr>
  <tr><td>GET</td><td><a href=/metrics>/metrics</a></td><td>Prometheus metrics</td></tr>
  <tr><td>GET</td><td><a href=/health>/health</a></td><td>Liveness check</td></tr>
  <tr><td>GET</td><td><a href=/docs>/docs</a></td><td>Swagger UI</td></tr>
</table>
{unknown_section}
{not_in_netdisco_section}
{not_in_netbox_section}
</body></html>"""


@app.get("/health", summary="Liveness check")
async def health() -> dict:
    return {"status": "ok", "paused": _is_paused(), "in_flight": list(_in_flight)}


@app.api_route("/reconcile", methods=["GET", "POST"], dependencies=[Depends(require_auth)], summary="Trigger reconcile run manually")
async def trigger_reconcile(
    background_tasks: BackgroundTasks,
    max_enqueue: Annotated[Optional[int], Query(description="Max devices to enqueue (overrides DISCOBOX_RECONCILE_MAX_ENQUEUE)")] = None,
    offset: Annotated[Optional[int], Query(description="Skip first N missing devices (for manual pagination)")] = None,
) -> dict:
    effective_max = max_enqueue if max_enqueue is not None else _RECONCILE_MAX_ENQUEUE
    background_tasks.add_task(_run_reconcile, max_enqueue=effective_max, offset=offset)
    return {"status": "reconcile queued", "max_enqueue": effective_max, "offset": offset}


@app.get("/unknown-devices", summary="Devices seen in Netdisco webhooks but not found in Netbox")
async def unknown_devices() -> list:
    with _unknown_devices_lock:
        devices = _load_unknown_devices()
    return sorted(devices.values(), key=lambda d: d["last_seen"], reverse=True)


@app.get("/not-in-netdisco", summary="Active Netbox devices not found in Netdisco (last reconcile)")
async def not_in_netdisco() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_NOT_IN_NETDISCO_FILE)


@app.get("/not-in-netbox", summary="Netdisco devices not found in Netbox (last reconcile)")
async def not_in_netbox() -> list:
    with _reconcile_gaps_lock:
        return _load_gap(_NOT_IN_NETBOX_FILE)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import socket
    port = int(os.getenv("DISCOBOX_LISTEN_PORT", os.getenv("DISCOBOX_PORT", "8080")))
    workers = int(os.getenv("DISCOBOX_WORKERS", "4"))
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"
    logger.info("discobox server starting on port %d (%d workers): outbound IP: %s", port, workers, local_ip)
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=port,
        workers=workers,
        log_config=_UVICORN_LOG_CONFIG,
    )
