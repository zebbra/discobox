# Operations

Jobs, endpoints, overload protection, metrics and sample logs.

## Jobs

Periodic and on-demand work runs as jobs, listed on the status page and at `/jobs`:

| Job | Default schedule | What it does |
|---|---|---|
| `gaps` | every 1h | Compare Netbox with Netdisco, read-only on both: Netbox devices (`reconcile.roles` / `statuses`, with a primary IP) Netdisco doesn't know, Netdisco devices missing from Netbox, auth tag mismatches. Devices are matched by IP (Netbox primary IPv4 = Netdisco device IP), then by hostname without domain (case-insensitive): a device found by name under a different IP is listed as an IP mismatch with both IPs (`netdisco_duplicate` when Netdisco has the Netbox IP too, i.e. a leftover entry to delete there), never enqueued, auto-created or counted as missing. Also runs the liveness query (status line, `discobox_liveness_query_up`). Saves the gap lists the next two jobs act on (`GET /gaps`) |
| `discover-missing` | every 24h | From the last gaps result: enqueue Netdisco discovery for the Netbox devices it doesn't know (not those liveness reports down; with `reconcile.require_auth_tag` only those with an auth tag; at most `reconcile.max_enqueue`), and with `auto_create` create Netbox devices for the non-stale Netdisco-only ones. Skipped when the gaps result is missing or older than `jobs.gaps.max_age` and while paused, deferred while syncs are queued (`reconcile.defer_if_queued`, retried after `reconcile.defer_seconds`), aborted while Netdisco's queue is over `reconcile.max_queued` / `max_failed` |
| `fix-tags` | on demand | From the last gaps result: re-enqueue discovery with Netbox's auth tag for the tag mismatches (same freshness rule) |
| `types-bare` | every 1h | Refresh `discobox_types_bare` (only with `custom_fields.source` set) |

```yaml
jobs:
  gaps:
    interval: 3600      # seconds; 0 = on demand only
    max_age: 7200       # discover-missing and fix-tags skip an older gaps result
  discover-missing:
    interval: 86400
  types-bare:
    interval: 3600
```

Each job's last result (start, end, status `ok | deferred | skipped | aborted | error`, summary) is kept in `discobox.jobs.json` in the state dir, so after a restart the next run follows the last one; an overdue job runs shortly after startup (gaps after 1 minute, discover-missing after 5 minutes, types-bare after 30 s). Any job also runs on demand: the status page's "run now", or `POST /jobs/<name>/run` (e.g. from an external scheduler). Metrics per job: `discobox_job_last_run_timestamp_seconds`, `discobox_job_last_success_timestamp_seconds`, `discobox_job_duration_seconds`, `discobox_job_next_run_timestamp_seconds`, `discobox_job_runs_total{status}`.

---

## Server Endpoints

Actions are POST. `GET /sync` and `GET /rebuild` still work but are deprecated: logged, counted in `discobox_deprecated_requests_total{path}` and answered with a `Deprecation: true` header.

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/` | GET | no | Status page: status, jobs, gaps, dummy types, actions (POST buttons) |
| `/sync?host=<ip>` | POST | yes | Trigger device sync (the Netdisco hook) |
| `/sync/all` | POST | yes | Queue a sync for every device known to Netdisco; optional `?limit=N&force=true` (force bypasses the cooldown window). Any other query param is forwarded to the Netbox device filter, e.g. `?last_updated__lt=2026-10-01`, `?name__ic=foo`, `?site=zrh&role=switch` — selection is then intersected with Netdisco's device list |
| `/rebuild?host=<ip>` | POST | yes | Rebuild one device's inventory to match Netdisco: also deletes discobox-owned stale interfaces, modules, inventory items and SFPs, and force-corrects `stack_members`. `dry_run=true` (default) previews those deletions; the sync part still runs like a normal sync, so it never writes more than one. `dry_run=false` also forces housekeeping and a full WLC AP pass and stamps `inventory_last_rebuild`. Synchronous: a large WLC can exceed a proxy timeout while the rebuild finishes in the background |
| `/sync/pause` | POST | yes | Pause queued syncs (discover-missing and fix-tags are skipped while paused) |
| `/sync/resume` | POST | yes | Resume queued syncs |
| `/discover` | POST | yes | Enqueue a Netdisco discover for one device: `?host=<ip or Netbox name>`, tag hint from `snmp_auth_profile` and per-request SNMP timeout derived from `snmp_polling_timeout` (`netdisco.discover_snmptimeout`), or `&tag=` / `&timeout=` (per-request, used as-is) |
| `/jobs` | GET | no | Every job: schedule, last run and result, next run; plus the retry queue (JSON) |
| `/jobs/<name>/run` | POST | yes | Run a job now: `202` in the background, `?wait=true` answers with the result; `409` while it's running. discover-missing and fix-tags take `?max_enqueue=N`, discover-missing `&offset=N` |
| `/gaps` | GET | yes | The last gaps result as JSON, answered immediately. `?refresh=true` starts the gaps job in the background (`running` shows it's in progress; the first call without a result starts one too), `?lists=true` adds the device lists, `?wait=true` runs it synchronously (may exceed proxy timeouts) |
| `/not-in-netdisco`, `/not-in-netbox`, `/tag-mismatches`, `/ip-mismatches`, `/unknown-devices` | GET | no | The gap lists (JSON) |
| `/types/library` | GET, POST | yes | DeviceType enrichment report; `POST …&apply=true` writes (see [Features](features.md)) |
| `/config` | GET | yes | Effective settings (secrets masked) and the env vars in use |
| `/stats` | GET | no | Aggregated status as JSON (for external status dashboards) |
| `/metrics` | GET | no | Prometheus metrics |
| `/health` | GET | no | Liveness check + in-flight hosts |
| `/docs` | GET | no | Swagger UI, served from the image (no CDN, works air-gapped) |

All sync flags are available as query parameters or JSON body fields, defaulting to the env var values:

```
POST /sync?host=10.0.0.1&housekeeping=true&sync_mac=false
POST /sync  {"host": "10.0.0.1", "housekeeping": true, "sync_mac": false}
```

Syncs run in a background thread pool. Duplicate requests for the same host are dropped while a sync is in progress. Concurrent syncs log under `discobox.<ip>` so they are distinguishable in the log stream.

---

## Overload Protection

discobox distinguishes between isolated device timeouts and global Netdisco overload, and handles each differently.

**Errors covered:** `ReadTimeout` (TCP read timeout) and HTTP `502`/`503`/`504` (gateway/proxy errors). Both indicate Netdisco is unable to serve the request.

### Circuit breaker

When ≥ `sync.circuit_breaker.threshold` (default 3) timeout errors occur within `sync.circuit_breaker.window` seconds (default 120), the circuit breaker trips:

- All new and queued syncs are held for `sync.circuit_breaker.backoff` seconds (default 120)
- After the backoff the breaker resets automatically — no manual intervention needed
- discover-missing and fix-tags also skip while the breaker is active
- Metric: `discobox_circuit_breaker_trips_total`

This is distinct from `sync.pause_on_error` / `/sync/pause`, which pause permanently until manually resumed.

### Per-device retry

When a sync times out but the circuit breaker has **not** tripped (isolated failure), the device is automatically retried:

- Retry is scheduled after `sync.retry.delay` seconds (default 480 = 8m), doubling each attempt
  up to a cap of `sync.retry.max_delay` (default 1800 = 30m)
- Up to `sync.retry.max` retries (default 6: 8m, 16m, 30m, 30m, 30m, 30m — then dropped)
- If a retry fires while the circuit breaker is active, it waits until the breaker clears
- If the queue is full when a retry becomes due, it is deferred 60s and re-attempted
  (the attempt is not consumed) rather than dropped
- Metric: `discobox_sync_retries_total`
- A hook for a device that is in Netdisco but not in Netbox is no error and no skip: it counts as `discobox_syncs_total{status="not_in_netbox"}` (plus `discobox_unknown_devices_total`), sets no failed flag, doesn't auto-pause, and lists the device on `/` and `/unknown-devices` until a sync finds it in Netbox (or no hook came for it in 14 days)
- Orphaned interfaces: `discobox_orphans_total{action=removed|braked}`; per device `discobox_device_orphans_braked{instance}` = orphans the mass-deletion brake kept in its last sync (> 0: discovery incomplete or Netbox stale — check the device or run a rebuild; a rebuild resets it to 0). Worth an alert: `discobox_device_orphans_braked > 0`
- Skips: `discobox_syncs_skipped_total{reason}`. Before queuing, a hook is dropped on `cooldown`, `in_progress` (already being synced) or `queue_full` (the device then waits for its next hook or discover-missing: worth an alert). A sync that started can still skip on `discovery_incomplete` (ifIndex placeholders as port names) or `recently_touched` (Netbox touch field within the cooldown); those two also count as `discobox_syncs_total{status="skipped"}`
- Likewise a sync for a device Netdisco doesn't know (404, and no other Netdisco IP matches): `discobox_syncs_total{status="not_in_netdisco"}`; the gaps job lists these on `/not-in-netdisco`
- Errors inside a sync that still completes (an AP, IP, module, … that failed; `errors=` in the `sync done` line) don't make it an `error` in `discobox_syncs_total`: they count in `discobox_sync_errors_total` and per device in `discobox_device_last_sync_errors{instance}`; APs also in `discobox_aps_total{action}`. The errors are every ERROR line a sync logs (plus counted ones not logged as errors), so a failed note, fan or PSU update counts too. `/rebuild` outcomes: `discobox_rebuilds_total{status=success|error|skipped,dry_run}`. Reads (GET/HEAD) to Netbox and Netdisco are retried up to 3 times on a dropped connection or a 500/502/503, after 2, 4 and 8 s (plus up to 1 s jitter), never on 504 (a gateway timeout usually repeats) and never for writes

---

## Sample Log Output

<details>
<summary><strong>Traditional stack — Cisco C9300 (3-member)</strong></summary>

```
INFO     discobox  ── device 10.10.1.1
INFO     discobox  Netdisco  hostname='SWCORE01-STK.corp.example.com'  ports=167
INFO     discobox  Netbox    device='swcore01-stk.corp.example.com'  id=443
INFO     discobox  Device fields updated — serial='FOC0000X001' os_name='ios-xe' os_ver=None os_release='Amsterdam'
INFO     discobox    stack (root)  'c93xx Stack'  model=C9300-24UX  serial=FOC0000X001
INFO     discobox    ├── chassis  'Switch 1'  model=C9300-24UX  serial=FOC0000X001
INFO     discobox    ├── chassis  'Switch 2'  model=C9300-24T  serial=FCW0000X002
INFO     discobox    └── chassis  'Switch 3'  model=C9300-24T  serial=FCW0000X003
INFO     discobox  Modules   chassis=3  topology=stack
INFO     discobox    Switch 1  C9300-24UX  serial=FOC0000X001  created
INFO     discobox    Switch 2  C9300-24T  serial=FCW0000X002  created
INFO     discobox    Switch 3  C9300-24T  serial=FCW0000X003  created
INFO     discobox  Modules — updated=3 unchanged=0 errors=0
INFO     discobox  PSUs      entries: 3
INFO     discobox    PSU Switch 2 - Power Supply A  model=PWR-C1-350WAC-P  serial=DCC0000X004  created
INFO     discobox    PSU Switch 3 - Power Supply A  model=PWR-C1-350WAC-P  serial=DCC0000X005  created
INFO     discobox    PSU Switch 1 - Power Supply A  model=PWR-C1-1100WAC-P  serial=DCC0000X006  created
INFO     discobox  PSUs — created=3 updated=0 unchanged=0 errors=0
INFO     discobox    GigabitEthernet2/0/4                     created
INFO     discobox    Vlan653                                  created
INFO     discobox    TenGigabitEthernet1/0/12                 updated
INFO     discobox    GigabitEthernet3/0/15                    created
...
INFO     discobox    TenGigabitEthernet1/0/3.10               created
WARNING  discobox    StackPort1/1                             in Netbox but not in Netdisco
WARNING  discobox    StackPort1/2                             in Netbox but not in Netdisco
INFO     discobox  Subinterfaces — linked 1 parent(s)
INFO     discobox  Interface→Module — updated=124 unchanged=0 skipped=44
WARNING  discobox    IP 198.51.100.11/24     already in Netbox (id=2688, assigned to unassigned) — skipping
INFO     discobox    IP 203.0.113.1/26       → created on Vlan720
...
INFO     discobox  IPs — created=22 fixed=1 moved=0 unchanged=0 skipped=12 errors=0
INFO     discobox  SFPs      entries: 12
INFO     discobox    SFP Te2/1/3              model=SFP-10G-SR-S         serial=ACW0000X007 → created
INFO     discobox    SFP Te3/1/8              model=SFP-10G-SR           serial=AGA0000X008 → created
...
INFO     discobox  SFPs — created=12 updated=0 unchanged=0 errors=0
INFO     discobox  PoE — updated=0 unchanged=24 skipped=0 errors=0
INFO     discobox  ── done 10.10.1.1  created=140 updated=26 unchanged=0 errors=0
INFO     discobox.server  Sync success for 10.10.1.1 in 214.7s
```

</details>

<details>
<summary><strong>StackWise Virtual — Cisco C9500 (2-member VSS)</strong></summary>

```
INFO     discobox.server  hook from 10.10.0.5: 10.10.2.87  queued
INFO     discobox  ── device 10.10.2.87
INFO     discobox  Netdisco  hostname='SWDIST01-VSS1.corp.example.com'  ports=106
INFO     discobox  Netbox    device='swdist01-vss1.corp.example.com'  id=507
INFO     discobox  Device fields updated — serial='FDO0000X009' os_name='ios-xe' os_ver=None os_release='Amsterdam'
INFO     discobox    stack (root)  'Virtual Stack'  model=  serial=
INFO     discobox    ├── chassis  'Switch 2 Chassis'  model=C9500-24Y4C  serial=FDO0000X010
INFO     discobox    └── chassis  'Switch 1 Chassis'  model=C9500-24Y4C  serial=FDO0000X009
INFO     discobox  Modules   chassis=2  topology=vss
INFO     discobox    DeviceType → C9500-24Y4C  serial=FDO0000X009  updated
INFO     discobox    VSS partner found by hostname 'swdist01-vss2.corp.example.com'
INFO     discobox    VSS partner DeviceType → C9500-24Y4C  serial=FDO0000X010  updated
INFO     discobox    VirtualChassis 'SWDIST01-VSS1' — created
INFO     discobox  Modules — updated=2 unchanged=0 errors=0
...
INFO     discobox  ── done 10.10.2.87  created=0 updated=0 unchanged=105 errors=0
INFO     discobox.server  Sync success for 10.10.2.87 in 20.3s
```

</details>

<details>
<summary><strong>Modular chassis — Cisco Nexus 9508 (blades + PSUs)</strong></summary>

```
INFO     discobox.server  hook from 10.10.0.6: 10.10.3.21  queued
INFO     discobox  ── device 10.10.3.21
INFO     discobox  Netdisco  hostname='NXCORE01-DC1'  ports=570
INFO     discobox  Netbox    device='nxcore01-dc1.corp.example.com'  id=534
WARNING  discobox  Hostname mismatch for 10.10.3.21 — Netdisco='NXCORE01-DC1'  Netbox='nxcore01-dc1.corp.example.com'
INFO     discobox  Device fields updated — serial='FGE0000X011' os_name='nx-os' os_ver=None os_release=None
WARNING  discobox    Dummy interface 'mgmt0' still has 1 IP(s) — skipping deletion
INFO     discobox  Housekeeping — deleted 0 stale device bay(s), 0 empty dummy interface(s)
INFO     discobox    chassis (root)  'Nexus9000 C9508 (8 Slot) Chassis'  type=cevChassisN9Kc9508  model=N9K-C9508  serial=FGE0000X011
INFO     discobox    └── chassis  'Nexus9000 C9508 (8 Slot) Chassis'  model=N9K-C9508  serial=FGE0000X011
INFO     discobox  Modules   chassis=1  topology=standalone
INFO     discobox    DeviceType → N9K-C9508  serial=FGE0000X011  updated
INFO     discobox  Modules — updated=1 unchanged=0 errors=0
INFO     discobox  PSUs      entries: 6
INFO     discobox    PSU PSU 1  model=N9K-PAC-3000W-B  serial=ART0000X012  created
INFO     discobox    PSU PSU 2  model=N9K-PAC-3000W-B  serial=ART0000X013  created
INFO     discobox    PSU PSU 3  model=N9K-PAC-3000W-B  serial=ART0000X014  created
INFO     discobox    PSU PSU 4  model=N9K-PAC-3000W-B  serial=ART0000X015  created
INFO     discobox    PSU PSU 5  model=N9K-PAC-3000W-B  serial=ART0000X016  created
INFO     discobox    PSU PSU 6  model=N9K-PAC-3000W-B  serial=DTM0000X017  created
INFO     discobox  PSUs — created=6 updated=0 unchanged=0 errors=0
INFO     discobox  Blades    entries: 12
INFO     discobox    Blade Supervisor Module 1       model=N9K-SUP-B+        serial=FOC0000X018  created
INFO     discobox    Blade Supervisor Module 2       model=N9K-SUP-B+        serial=FOC0000X019  created
INFO     discobox    Blade 32p 40/100G Ethernet Module 1  model=N9K-X9732C-EX  serial=FOC0000X020  created
INFO     discobox    Blade 32p 40/100G Ethernet Module 2  model=N9K-X9732C-EX  serial=FOC0000X021  created
INFO     discobox    Blade 32p 40/100G Ethernet Module 3  model=N9K-X9732C-EX  serial=FOC0000X022  created
INFO     discobox    Blade 32p 40/100G Ethernet Module 4  model=N9K-X9732C-EX  serial=FOC0000X023  created
INFO     discobox    Blade Fabric Module 2  model=N9K-C9508-FM-E  serial=FOC0000X024  created
INFO     discobox    Blade Fabric Module 3  model=N9K-C9508-FM-E  serial=FOC0000X025  created
INFO     discobox    Blade Fabric Module 4  model=N9K-C9508-FM-E  serial=FOC0000X026  created
INFO     discobox    Blade Fabric Module 6  model=N9K-C9508-FM-E  serial=FOC0000X027  created
INFO     discobox    Blade System Controller 1  model=N9K-SC-A  serial=FOC0000X028  created
INFO     discobox    Blade System Controller 2  model=N9K-SC-A  serial=FOC0000X029  created
INFO     discobox  Blades — created=12 updated=0 unchanged=0 errors=0
INFO     discobox    Ethernet4/20                             created
INFO     discobox    Tunnel26                                 created
INFO     discobox    Tunnel211                                created
...
```

</details>

<details>
<summary><strong>HA pair / VIP — Fortinet (threenode mode)</strong></summary>

```
INFO     discobox  Netbox    device='fwcluster01p1h.corp.example.com'  id=24761
INFO     discobox.10.10.4.98  HA VIP device 'fwcluster01p0h.corp.example.com' added as VC member pos=0
INFO     discobox    VC member fwcluster01p1h.corp.example.com          pos=1  unchanged
INFO     discobox    VC member fwcluster01p2h.corp.example.com          pos=2  unchanged
INFO     discobox    VC member fwcluster01p0h.corp.example.com          pos=0  unchanged
INFO     discobox.10.10.4.98  HA peer VirtualChassis 'fwcluster01' — unchanged  (topology=standalone per Netdisco; VC is Netbox-side)
INFO     discobox.10.10.4.98  VIP device 'fwcluster01p0h.corp.example.com' — device_type updated
INFO     discobox.10.10.4.98  VIP device 'fwcluster01p0h.corp.example.com' — primary IP 10.10.4.97/24 role set to vip
INFO     discobox  Device fields updated — serial='FG00000000000030' os_name='fortios' os_ver=None os_release=None
```

</details>
