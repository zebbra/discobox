#!/usr/bin/env python3
"""
discobox server — FastAPI webhook receiver + Prometheus metrics.

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
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Annotated, Optional

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

# Rename uvicorn.error → uvicorn (the name is misleading; it's their general logger)
logging.getLogger("uvicorn.error").name = "uvicorn"
# Suppress per-request access lines — sync results are logged by _run_sync
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

# ── Auth ───────────────────────────────────────────────────────────────────────

_AUTH_TOKEN: Optional[str] = os.getenv("DISCOBOX_AUTH_TOKEN")
_METRICS_PATH: str = os.getenv("DISCOBOX_METRICS_PATH", "/metrics")

# ── Sync defaults (overridable per-request) ────────────────────────────────────

def _flag(env: str) -> bool:
    """Return True if env var is set to a truthy value."""
    return os.getenv(env, "").lower() in ("1", "true", "yes")

# DISCOBOX_NO_* disables a feature; DISCOBOX_HOUSEKEEPING enables it.
_DEFAULT_MAC          = not _flag("DISCOBOX_NO_MAC")
_DEFAULT_IP           = not _flag("DISCOBOX_NO_IP")
_DEFAULT_MODULES      = not _flag("DISCOBOX_NO_MODULES")
_DEFAULT_SFP          = not _flag("DISCOBOX_NO_SFP")
_DEFAULT_POE          = not _flag("DISCOBOX_NO_POE")
_DEFAULT_HOUSEKEEPING     =     _flag("DISCOBOX_HOUSEKEEPING")
_DEFAULT_LLDP_CLEAR_STALE =     _flag("DISCOBOX_LLDP_CLEAR_STALE")
_VIP_MODE: str = os.getenv("DISCOBOX_VIP_MODE", "threenode").lower()  # threenode | soft | hard | off
_PAUSE_ON_ERROR: bool = _flag("DISCOBOX_PAUSE_ON_ERROR")


async def require_auth(authorization: Annotated[str, Header()] = "") -> None:
    """Bearer token auth. Disabled if DISCOBOX_AUTH_TOKEN is not set."""
    if not _AUTH_TOKEN:
        return
    if authorization != f"Bearer {_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


# ── Reconcile loop ─────────────────────────────────────────────────────────────

_RECONCILE_INTERVAL: int = int(os.getenv("DISCOBOX_RECONCILE_INTERVAL", str(24 * 3600)))
_RECONCILE_MAX_QUEUED: Optional[int] = int(v) if (v := os.getenv("DISCOBOX_RECONCILE_MAX_QUEUED")) else None
_RECONCILE_MAX_FAILED: Optional[int] = int(v) if (v := os.getenv("DISCOBOX_RECONCILE_MAX_FAILED")) else None
_RECONCILE_MAX_ENQUEUE: Optional[int] = int(v) if (v := os.getenv("DISCOBOX_RECONCILE_MAX_ENQUEUE")) else None


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
        logger.info("Reconcile skipped — sync is paused")
        return
    nd = _make_netdisco_client()
    nb = NetboxClient(
        url=os.environ["NETBOX_URL"],
        token=os.environ["NETBOX_TOKEN"],
        verify_tls=os.getenv("NETBOX_TLS_VERIFY", "true").lower() != "false",
    )
    effective_max = max_enqueue if max_enqueue is not None else _RECONCILE_MAX_ENQUEUE
    counts = reconcile_devices(nd, nb, max_queued=_RECONCILE_MAX_QUEUED, max_failed=_RECONCILE_MAX_FAILED, max_enqueue=effective_max, offset=offset)
    if counts.get("aborted"):
        reconcile_aborted_total.inc()
        return
    reconcile_netbox_devices.set(counts.get("netbox_total", 0))
    reconcile_netdisco_devices.set(counts.get("netdisco_total", 0))
    reconcile_enqueued_total.inc(counts.get("enqueued", 0))
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


@asynccontextmanager
async def lifespan(app):
    try:
        task = asyncio.create_task(_reconcile_loop())
    except Exception as exc:
        logger.error("Failed to start reconcile loop: %s", exc)
        task = None
    yield
    if task:
        task.cancel()


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="discobox",
    description="Netdisco → Netbox sync webhook receiver",
    version="1.0.0",
    lifespan=lifespan,
)

# Hosts currently being synced — guards against duplicate concurrent syncs
_in_flight: set[str] = set()
_in_flight_lock = threading.Lock()

# Devices seen in Netdisco webhooks but not found in Netbox
_unknown_devices: dict[str, dict] = {}
_unknown_devices_lock = threading.Lock()

# Limit concurrent Netbox API load — all workers share this semaphore via the
# threading module (workers are forked from the same parent process).
_MAX_CONCURRENT: int = int(os.getenv("DISCOBOX_MAX_CONCURRENT_SYNCS", "3"))
_sync_semaphore = threading.Semaphore(_MAX_CONCURRENT)
_MAX_QUEUE: int = int(os.getenv("DISCOBOX_MAX_QUEUE", "100"))

# Pause gate — file-based so all workers see it regardless of which handled the request.
# Presence of the file = paused; absence = running.
_PAUSE_FILE: str = os.path.join(
    os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp"), "discobox.pause"
)

def _is_paused() -> bool:
    return os.path.exists(_PAUSE_FILE)

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

def _run_sync(host: str, sync_mac: bool, sync_ip: bool, sync_modules: bool, sync_sfp: bool, sync_poe: bool, housekeeping: bool, lldp_clear_stale: bool = False) -> None:
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
        )
        status = "success" if result.get("ok") else "error"
        if result.get("reason") == "device_not_found":
            unknown_devices_total.inc()
            with _unknown_devices_lock:
                _unknown_devices[host] = {
                    "ip": host,
                    "hostname": result.get("hostname") or "",
                    "last_seen": time.time(),
                }
    except Exception as exc:
        logger.error("Sync failed for %s: %s", host, exc)
        status = "error"
    finally:
        sync_running.dec()
        _sync_semaphore.release()
        elapsed = time.time() - start
        syncs_total.labels(status=status).inc()
        sync_duration.observe(elapsed)
        sync_in_progress.dec()
        with _in_flight_lock:
            _in_flight.discard(host)
        # Per-device metrics — duration/timestamp only on success so a persistently
        # failing device goes stale and timestamp-based alerts fire correctly.
        # device_sync_failed is always updated so failure is immediately visible.
        instance = result.get("hostname") or host
        device_sync_failed.labels(instance=instance).set(0 if status == "success" else 1)
        if status == "error" and _PAUSE_ON_ERROR and not _is_paused():
            _set_paused(True)
            sync_paused.set(1)
            logger.warning("Sync error for %s — auto-pausing intake (DISCOBOX_PAUSE_ON_ERROR)", host)
        if status == "success":
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
        logger.info("Sync %s for %s in %.1fs", status, host, elapsed)


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

    with _in_flight_lock:
        if resolved_host in _in_flight:
            logger.info("hook from %s: %s  already in progress — skipping", caller, resolved_host)
            syncs_skipped_total.inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="already in progress")
        if len(_in_flight) >= _MAX_QUEUE:
            logger.warning("hook from %s: %s  queue full (%d/%d) — skipping", caller, resolved_host, len(_in_flight), _MAX_QUEUE)
            syncs_skipped_total.inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="queue full")
        _in_flight.add(resolved_host)

    sync_in_progress.inc()
    background_tasks.add_task(_run_sync, resolved_host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe, housekeeping, lldp_clear_stale)
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
    logger.warning("Sync paused — %d task(s) queued", len(_in_flight))
    return {"status": "paused", "queued": len(_in_flight)}


@app.api_route("/sync/resume", methods=["GET", "POST"], dependencies=[Depends(require_auth)], summary="Resume queued syncs")
async def resume() -> dict:
    """Release the pause gate; queued syncs start draining (up to MAX_CONCURRENT at a time)."""
    _set_paused(False)
    sync_paused.set(0)
    logger.info("Sync resumed — %d task(s) queued", len(_in_flight))
    return {"status": "running", "queued": len(_in_flight)}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> str:
    paused = _is_paused()
    with _unknown_devices_lock:
        unknown_count = len(_unknown_devices)
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
        for d in sorted(_unknown_devices.values(), key=lambda x: x["last_seen"], reverse=True):
            ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(d["last_seen"]))
            unknown_rows += f"<tr><td>{d['ip']}</td><td>{d['hostname']}</td><td>{ts}</td></tr>"
    unknown_section = f"""
    <h2>Unknown devices ({unknown_count})</h2>
    <table><tr><th>IP</th><th>Hostname</th><th>Last seen</th></tr>{unknown_rows}</table>
    """ if unknown_count else "<h2>Unknown devices</h2><p>None</p>"

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
  <tr><td>POST</td><td><a href=/docs#/default/sync_sync_post>/sync</a></td><td>Netdisco webhook — trigger device sync</td></tr>
  <tr><td>POST</td><td>/sync/pause</td><td>Pause queued syncs</td></tr>
  <tr><td>POST</td><td>/sync/resume</td><td>Resume queued syncs</td></tr>
  <tr><td>POST</td><td>/reconcile</td><td>Trigger reconcile run manually</td></tr>
  <tr><td>GET</td><td><a href=/unknown-devices>/unknown-devices</a></td><td>Devices in Netdisco not found in Netbox (JSON)</td></tr>
  <tr><td>GET</td><td><a href=/metrics>/metrics</a></td><td>Prometheus metrics</td></tr>
  <tr><td>GET</td><td><a href=/health>/health</a></td><td>Liveness check</td></tr>
  <tr><td>GET</td><td><a href=/docs>/docs</a></td><td>Swagger UI</td></tr>
</table>
{unknown_section}
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
        return sorted(_unknown_devices.values(), key=lambda d: d["last_seen"], reverse=True)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import socket
    port = int(os.getenv("DISCOBOX_LISTEN_PORT", os.getenv("DISCOBOX_PORT", "8080")))
    workers = int(os.getenv("DISCOBOX_WORKERS", "4"))
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"
    logger.info("discobox server starting on port %d (%d workers) — outbound IP: %s", port, workers, local_ip)
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=port,
        workers=workers,
        log_config=_UVICORN_LOG_CONFIG,
    )
