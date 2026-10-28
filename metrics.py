"""
Prometheus metrics of discobox's server, on a private registry so the default
global REGISTRY (and its built-in collectors) is never touched. One process,
plain in-memory values; the job metrics are registered by jobs.JobRegistry.
"""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector

_custom_registry = CollectorRegistry()
_reg = {"registry": _custom_registry}

# process_resident_memory_bytes / process_cpu_seconds_total / process_start_time_seconds, ...
ProcessCollector(registry=_custom_registry)

hooks_received_total = Counter(
    "discobox_hooks_received_total",
    "Total webhook POST /sync calls received (including skipped/invalid)",
    **_reg,
)
syncs_total = Counter(
    "discobox_syncs_total",
    "Completed device syncs",
    ["status"],   # success | error | skipped | not_in_netbox | not_in_netdisco
    **_reg,
)
sync_duration = Histogram(
    "discobox_sync_duration_seconds",
    "Time spent syncing a device",
    buckets=[5, 10, 30, 60, 120, 300, 600, 1200],
    **_reg,
)
sync_in_progress = Gauge(
    "discobox_sync_queued",
    "Device syncs in flight: waiting for a slot + actively running",
    **_reg,
)
sync_running = Gauge(
    "discobox_sync_running",
    "Device syncs actively running (semaphore slot acquired)",
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
aps_total = Counter(
    "discobox_aps_total",
    "Access points processed across all WLC syncs",
    ["action"],   # updated | unchanged | not_found | skipped | error
    **_reg,
)
rebuilds_total = Counter(
    "discobox_rebuilds_total",
    "/rebuild calls by outcome (error = the rebuild aborted, e.g. Netbox/Netdisco failed)",
    ["status", "dry_run"],   # success | error | skipped
    **_reg,
)
sync_errors_total = Counter(
    "discobox_sync_errors_total",
    "Errors inside completed syncs, all parts (interfaces, IPs, modules, SFPs, PoE, APs, stack cables); "
    "a sync with some of these can still count as success in discobox_syncs_total",
    **_reg,
)
device_sync_errors = Gauge(
    "discobox_device_last_sync_errors",
    "Errors inside the last completed sync of each device (all parts; 0 = clean)",
    ["instance"],
    **_reg,
)
orphans_total = Counter(
    "discobox_orphans_total",
    "Orphaned interfaces (in Netbox, not in Netdisco): removed, or braked (kept by the "
    "mass-deletion brake, sync.orphan_delete_max_percent; counted again on every sync of that device)",
    ["action"],   # removed | braked
    **_reg,
)
device_orphans_braked = Gauge(
    "discobox_device_orphans_braked",
    "Orphaned interfaces the mass-deletion brake kept in the last sync of each device (0 = none; "
    "> 0: Netdisco discovery incomplete, or Netbox stale — check the device or run a rebuild)",
    ["instance"],
    **_reg,
)
syncs_skipped_total = Counter(
    "discobox_syncs_skipped_total",
    "Syncs not done, by reason: cooldown | in_progress | queue_full (hook dropped before queuing; "
    "queue_full means the device waits for its next hook or discover-missing), discovery_incomplete | "
    "recently_touched (sync started, then skipped; also in discobox_syncs_total{status=\"skipped\"})",
    ["reason"],
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
    **_reg,
)
device_sync_timestamp = Gauge(
    "discobox_device_last_sync_timestamp_seconds",
    "Unix timestamp of the last completed sync for each device",
    ["instance"],
    **_reg,
)
sync_paused = Gauge(
    "discobox_sync_paused",
    "1 if sync intake is paused, 0 if running",
    **_reg,
)
device_sync_failed = Gauge(
    "discobox_device_last_sync_failed",
    "1 if the last sync attempt for this device failed, 0 if it succeeded",
    ["instance"],
    **_reg,
)
gaps_netbox_devices = Gauge(
    "discobox_gaps_netbox_devices",
    "Netbox devices (reconcile.roles/statuses, with a primary IP) seen by the last gaps run",
    **_reg,
)
gaps_netdisco_devices = Gauge(
    "discobox_gaps_netdisco_devices",
    "Devices known to Netdisco, last gaps run",
    **_reg,
)
discover_missing_enqueued_total = Counter(
    "discobox_discover_missing_enqueued_total",
    "Devices enqueued for Netdisco discovery by discover-missing",
    **_reg,
)
unknown_devices_total = Counter(
    "discobox_unknown_devices_total",
    "Sync webhooks received for devices in Netdisco but not in Netbox",
    **_reg,
)
gaps_not_in_netdisco = Gauge(
    "discobox_gaps_not_in_netdisco",
    "Netbox devices not found in Netdisco, excluding those liveness reports down (last gaps run)",
    **_reg,
)
gaps_offline = Gauge(
    "discobox_gaps_offline",
    "Netbox devices not found in Netdisco that liveness reports down (last gaps run)",
    **_reg,
)
types_bare = Gauge(
    "discobox_types_bare",
    "Bare DeviceTypes/ModuleTypes (source=netdisco: created by discobox without templates) in Netbox, "
    "value = devices/modules using it; library: whether the devicetype-library has the model "
    "(available: /types/library can fill it, none: fix by hand or upstream, unknown: not checked)",
    ["kind", "manufacturer", "model", "part_number", "library"],
    **_reg,
)

liveness_query_up = Gauge(
    "discobox_liveness_query_up",
    "1 if the liveness query of the last gaps run succeeded, 0 on error",
    **_reg,
)

gaps_not_in_netbox = Gauge(
    "discobox_gaps_not_in_netbox",
    "Netdisco devices not found in Netbox, excluding stale ones (last gaps run)",
    **_reg,
)
gaps_not_in_netbox_stale = Gauge(
    "discobox_gaps_not_in_netbox_stale",
    "Netdisco devices not in Netbox whose last discovery lags Netdisco's newest by more than "
    "reconcile.not_in_netbox_stale_hours: listed, not counted (last gaps run)",
    **_reg,
)
gaps_ip_mismatches = Gauge(
    "discobox_gaps_ip_mismatches",
    "Devices in both by hostname but under a different IP in Netbox and Netdisco (last gaps run)",
    **_reg,
)
gaps_tag_mismatches = Gauge(
    "discobox_gaps_tag_mismatches",
    "Netdisco devices with a missing or Netbox-mismatched auth tag (last gaps run)",
    **_reg,
)
interface_defaults_total = Counter(
    "discobox_interface_defaults_total",
    "Interface custom fields filled from interface_defaults (empty ones only), by field",
    ["field"],
    **_reg,
)
fix_tags_enqueued_total = Counter(
    "discobox_fix_tags_enqueued_total",
    "Devices re-enqueued for Netdisco discovery to correct an auth tag mismatch",
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
netbox_requests_total = Counter(
    "discobox_netbox_requests_total",
    "Outbound HTTP requests made to Netbox",
    ["method"],
    **_reg,
)
netdisco_requests_total = Counter(
    "discobox_netdisco_requests_total",
    "Outbound HTTP requests made to Netdisco",
    ["method"],
    **_reg,
)

deprecated_requests_total = Counter(
    "discobox_deprecated_requests_total",
    "Requests using a deprecated form (GET on an action endpoint), by path",
    ["path"],
    **_reg,
)
