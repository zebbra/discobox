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
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("discobox.server")

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

# ── Auth ───────────────────────────────────────────────────────────────────────

_AUTH_TOKEN: Optional[str] = os.getenv("DISCOBOX_AUTH_TOKEN")


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


class SyncResponse(BaseModel):
    status: str
    host: str
    reason: Optional[str] = None


# ── Background sync ────────────────────────────────────────────────────────────

def _run_sync(host: str) -> None:
    """Run sync_device in a background thread and record metrics."""
    start = time.time()
    status = "error"
    result: dict = {}
    try:
        nd = NetdiscoClient(
            base_url=os.environ["NETDISCO_URL"],
            username=os.environ["NETDISCO_USERNAME"],
            password=os.environ["NETDISCO_PASSWORD"],
        )
        nb = NetboxClient(
            url=os.environ["NETBOX_URL"],
            token=os.environ["NETBOX_TOKEN"],
            change_reason="DiscoBox Hook",
        )
        result = sync_device(
            nd=nd,
            nb=nb,
            ip=host,
            sync_mac=True,
            sync_ip=True,
            sync_modules=True,
            sync_sfp=True,
            housekeeping=False,
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
        # Record per-action counts from result dict
        for action, count in result.get("interfaces", {}).items():
            interfaces_total.labels(action=action).inc(count)
        for action, count in result.get("ips", {}).items():
            ips_total.labels(action=action).inc(count)
        for action, count in result.get("modules", {}).items():
            modules_total.labels(action=action).inc(count)
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
    background_tasks: BackgroundTasks,
    host: Annotated[Optional[str], Query(description="Device management IP")] = None,
    body: Optional[SyncRequest] = None,
) -> SyncResponse:
    """
    Queue a sync for the given device IP.

    `host` can be passed as a query parameter or in the JSON body (POST only).
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

    with _in_flight_lock:
        if resolved_host in _in_flight:
            logger.info("Sync for %s already in progress — skipping", resolved_host)
            return SyncResponse(status="skipped", host=resolved_host, reason="already in progress")
        _in_flight.add(resolved_host)

    sync_in_progress.inc()
    background_tasks.add_task(_run_sync, resolved_host)
    logger.info("Sync queued for %s", resolved_host)
    return SyncResponse(status="queued", host=resolved_host)


@app.get("/metrics", include_in_schema=False)
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
        log_config=None,  # use our logging config
    )
