#!/usr/bin/env python3
"""
discobox server — FastAPI webhook receiver + Prometheus metrics.

Netdisco calls POST /sync after each discovery job.
Syncs run in a background thread pool; duplicate requests for the
same host are dropped while a sync is already in progress.

Endpoints:
  POST /sync             Trigger a device sync
  GET  /metrics          Prometheus metrics
  GET  /health           Liveness check
  GET  /docs             Swagger UI (auto-generated)
"""

import logging
import os
import threading
import time
from typing import Annotated, Optional

import uvicorn
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from pydantic import BaseModel

from discobox import NetboxClient, NetdiscoClient, sync_device, validate_ip

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
# Cardinality note: with ~30k hosts, per-host labels are avoided on gauges and
# histograms. Per-host last-sync info is served via /health JSON instead.

registry = CollectorRegistry()

hooks_received_total = Counter(
    "discobox_hooks_received_total",
    "Total webhook POST /sync calls received (including skipped/invalid)",
    registry=registry,
)
syncs_total = Counter(
    "discobox_syncs_total",
    "Completed device syncs",
    ["status"],   # success | error — no host label
    registry=registry,
)
sync_duration = Histogram(
    "discobox_sync_duration_seconds",
    "Time spent syncing a device",
    buckets=[5, 10, 30, 60, 120, 300],
    registry=registry,
)
sync_in_progress = Gauge(
    "discobox_sync_in_progress",
    "Number of device syncs currently running",
    registry=registry,
)
interfaces_total = Counter(
    "discobox_interfaces_total",
    "Interfaces processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    registry=registry,
)
ips_total = Counter(
    "discobox_ips_total",
    "IP addresses processed across all syncs",
    ["action"],   # created | fixed | moved | unchanged | skipped | error
    registry=registry,
)
modules_total = Counter(
    "discobox_modules_total",
    "Modules processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    registry=registry,
)
sfps_total = Counter(
    "discobox_sfps_total",
    "SFP inventory items processed across all syncs",
    ["action"],   # created | updated | unchanged | error
    registry=registry,
)
syncs_skipped_total = Counter(
    "discobox_syncs_skipped_total",
    "Sync requests dropped because the host was already being synced",
    registry=registry,
)
ha_vip_total = Counter(
    "discobox_ha_vip_total",
    "HA VIP redirections detected and handled",
    registry=registry,
)
device_sync_duration = Gauge(
    "discobox_device_last_sync_duration_seconds",
    "Duration of the last completed sync for each device",
    ["instance"],
    registry=registry,
)
device_sync_timestamp = Gauge(
    "discobox_device_last_sync_timestamp_seconds",
    "Unix timestamp of the last completed sync for each device",
    ["instance"],
    registry=registry,
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
_DEFAULT_HOUSEKEEPING =     _flag("DISCOBOX_HOUSEKEEPING")
_VIP_MODE: str = os.getenv("DISCOBOX_VIP_MODE", "threenode").lower()  # threenode | soft | hard | off


async def require_auth(authorization: Annotated[str, Header()] = "") -> None:
    """Bearer token auth. Disabled if DISCOBOX_AUTH_TOKEN is not set."""
    if not _AUTH_TOKEN:
        return
    if authorization != f"Bearer {_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="discobox",
    description="Netdisco → Netbox sync webhook receiver",
    version="1.0.0",
)

# Hosts currently being synced — guards against duplicate concurrent syncs
_in_flight: set[str] = set()
_in_flight_lock = threading.Lock()


# ── Request / response models ──────────────────────────────────────────────────

class SyncRequest(BaseModel):
    host: str
    sync_mac: bool = _DEFAULT_MAC
    sync_ip: bool = _DEFAULT_IP
    sync_modules: bool = _DEFAULT_MODULES
    sync_sfp: bool = _DEFAULT_SFP
    sync_poe: bool = _DEFAULT_POE
    housekeeping: bool = _DEFAULT_HOUSEKEEPING


class SyncResponse(BaseModel):
    status: str
    host: str
    reason: Optional[str] = None


# ── Background sync ────────────────────────────────────────────────────────────

def _run_sync(host: str, sync_mac: bool, sync_ip: bool, sync_modules: bool, sync_sfp: bool, sync_poe: bool, housekeeping: bool) -> None:
    """Run sync_device in a background thread and record metrics."""
    start = time.time()
    status = "error"
    result: dict = {}
    try:
        nd = NetdiscoClient(
            base_url=os.environ["NETDISCO_URL"],
            username=os.environ["NETDISCO_USERNAME"],
            password=os.environ["NETDISCO_PASSWORD"],
            verify_tls=os.getenv("NETDISCO_TLS_VERIFY", "true").lower() != "false",
        )
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
            vip_mode=_VIP_MODE,
        )
        status = "success" if result.get("ok") else "error"
    except Exception as exc:
        logger.error("Sync failed for %s: %s", host, exc)
        status = "error"
    finally:
        elapsed = time.time() - start
        syncs_total.labels(status=status).inc()
        sync_duration.observe(elapsed)
        sync_in_progress.dec()
        with _in_flight_lock:
            _in_flight.discard(host)
        # Per-device metrics — only on success so a persistently failing device
        # goes stale and timestamp-based alerts fire correctly.
        if status == "success":
            instance = result.get("hostname") or host
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

    caller = request.headers.get("x-forwarded-for", "").split(",")[0].strip() \
             or (request.client.host if request.client else "unknown")

    with _in_flight_lock:
        if resolved_host in _in_flight:
            logger.info("hook from %s: %s  already in progress — skipping", caller, resolved_host)
            syncs_skipped_total.inc()
            return SyncResponse(status="skipped", host=resolved_host, reason="already in progress")
        _in_flight.add(resolved_host)

    sync_in_progress.inc()
    background_tasks.add_task(_run_sync, resolved_host, sync_mac, sync_ip, sync_modules, sync_sfp, sync_poe, housekeeping)
    logger.info("hook from %s: %s  queued", caller, resolved_host)
    return SyncResponse(status="queued", host=resolved_host)


@app.get(_METRICS_PATH, include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", summary="Liveness check")
async def health() -> dict:
    return {"status": "ok", "in_flight": list(_in_flight)}


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("DISCOBOX_PORT", "8080"))
    workers = int(os.getenv("DISCOBOX_WORKERS", "4"))
    logger.info("discobox server starting on port %d (%d workers)", port, workers)
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=port,
        workers=workers,
        log_config=_UVICORN_LOG_CONFIG,
    )
