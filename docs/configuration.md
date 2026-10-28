# Configuration

Environment variables and `discobox.yaml`.

Connections and secrets come from the environment (`.env.example`), everything else from `discobox.yaml` (path in `DISCOBOX_CONFIG`; the annotated `discobox.yaml` in this repo lists every key with its default). The file is checked at startup: an unknown key or a wrong type stops discobox with the offending path, e.g. `sync.cooldwn: Extra inputs are not permitted`. `GET /config` shows the effective settings, secrets masked.

```env
NETDISCO_URL=https://netdisco.example.com
NETDISCO_TOKEN=your-long-lived-token    # preferred (Bearer prefix added automatically)
NETDISCO_USERNAME=admin                 # fallback if NETDISCO_TOKEN is not set
NETDISCO_PASSWORD=secret
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your-token-here
NETBOX_TLS_VERIFY=true                  # false for self-signed certificates
NETDISCO_TLS_VERIFY=true

DISCOBOX_LISTEN_PORT=8080               # default: 8080
DISCOBOX_AUTH_TOKEN=secret              # bearer token for the action endpoints; unset = no auth
DISCOBOX_CONFIG=/app/discobox.yaml      # default: ./discobox.yaml if present, else built-in defaults
DISCOBOX_STATE_DIR=/tmp/discobox        # writable dir for job results, gap reports, pause flag, sync
                                        # cooldown markers, AP fingerprints; default: /tmp/discobox. On a
                                        # volume that outlives the pod, job schedules survive a redeploy too
```

The main `discobox.yaml` sections:

| Section | What it sets |
|---|---|
| `sync` | Feature defaults per sync (`no_mac`, `no_ip`, `no_modules`, `no_sfp`, `no_poe`, `housekeeping`, `lldp_clear_stale`; per request overridable), cooldown, VIP handling (`vip_mode`, `vip_mode_by_vendor`), concurrency (`max_concurrent`, `max_queue`), `retry`, `circuit_breaker`, `pause_on_error`, orphan removal, Fortinet HA resolution (`ha_resolve`) |
| `custom_fields`, `cabling` | Custom field names discobox writes and reads; null disables one |
| `types`, `library` | DeviceType resolution (aliases, `create_missing`) and devicetype-library enrichment |
| `aps` | WLC access point pass |
| `reconcile`, `auto_create` | Device selection for the gaps job, the `liveness` gate, discover-missing limits, auto-created devices |
| `interface_defaults` | Rules that fill an empty interface custom field on devices whose device custom field matches (e.g. `interface_severity: warning` for `cf_service_team` netops/campus); see [Features](features.md) |
| `jobs` | Job schedules (see [Jobs](operations.md#jobs)) |
| `netdisco` | Per-request SNMP timeout of the discover jobs discobox enqueues |
| `auth` | `metrics_path`; `token` (prefer `DISCOBOX_AUTH_TOKEN`) |
