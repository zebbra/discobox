# discobox

![DiscoBox](banner.png)

[![CI](https://github.com/zebbra/discobox/actions/workflows/ci.yml/badge.svg)](https://github.com/zebbra/discobox/actions/workflows/ci.yml)
[![Image](https://img.shields.io/badge/quay.io-zebbra%2Fdiscobox-blue)](https://quay.io/repository/zebbra/discobox)

Syncs network device inventory from [Netdisco](https://github.com/netdisco/netdisco) into [Netbox](https://netbox.dev/), enriching existing Netbox device records with live data discovered by Netdisco.

Devices are matched by management IP. Hostnames are verified and a warning is logged on mismatch, but the sync proceeds regardless.

Netbox is written to directly via [pynetbox](https://github.com/netbox-community/pynetbox) for simplicity. Migration to [Netbox Diode](https://github.com/netbox-community/netbox-diode) is a future consideration once its entity coverage matures.

---

## Architecture

```mermaid
flowchart LR
    DEV["Network Devices"]
    ND["Netdisco"]
    DB["discobox"]
    NB["Netbox"]
    VM["VictoriaMetrics"]

    DEV -- SNMP --> ND
    ND -- "webhook after discovery" --> DB
    DB -- "fetch device data" --> ND
    DB -- "write inventory" --> NB
    DB -. "discover-missing: enqueue missing devices" .-> ND
    DB -. "gaps: liveness check" .-> VM
```

---

## Features

- **Device fields**: serial, OS version, OS name, OS release ([features](docs/features.md), [mapping](docs/netbox-mapping.md#device))
- **Interfaces**: physical and logical interfaces, type by name prefix, subinterface parents ([mapping](docs/netbox-mapping.md#interfaces))
- **PoE**: `poe_mode = pse` on PoE-capable ports ([features](docs/features.md))
- **MAC addresses**: `dcim.mac-addresses` as primary MAC per interface ([features](docs/features.md))
- **IP addresses**: interface IPs, prefix fixes, moves off dummy interfaces ([mapping](docs/netbox-mapping.md#ip-addresses))
- **Module bays & modules**: chassis members as module bays with modules ([mapping](docs/netbox-mapping.md#module-bays--modules))
- **Blades**: linecards, supervisors, fabric modules as module bays ([mapping](docs/netbox-mapping.md#blades-linecards--supervisors))
- **Device type auto-creation**: manufacturer and device type if missing, [type aliases](docs/netbox-mapping.md#type-aliases) first ([features](docs/features.md))
- **SFP / transceiver inventory**: inventory items linked to their interface ([mapping](docs/netbox-mapping.md#sfp--transceivers))
- **PSU inventory**: inventory items for power supplies ([features](docs/features.md))
- **HA / VIP detection**: VIP by hostname mismatch, Virtual Chassis of the HA members ([topologies](docs/netbox-mapping.md#supported-topologies))
- **Wireless APs**: APs reported by a WLC updated during its sync ([features](docs/features.md))
- **WLC HA pairs**: Cisco 9800 HA SSO pair as one device per unit in a Virtual Chassis ([features](docs/features.md))
- **DeviceType enrichment**: fill-blank from the devicetype-library, `GET`/`POST /types/library` ([features](docs/features.md))
- **Housekeeping**: stale template device bays and empty dummy interfaces removed ([features](docs/features.md))
- **Orphaned interfaces**: interfaces Netdisco doesn't report removed, with a mass-deletion brake ([features](docs/features.md))
- **Interface defaults**: fill empty interface custom fields by device custom field, e.g. `interface_severity` per team ([features](docs/features.md))

---

## Quick start

A FastAPI server that receives webhook calls from Netdisco after each discovery and runs syncs in the background. Exposes a status page, Prometheus metrics and a Swagger UI. One process: jobs, in-flight tracking and metrics live in it. At runtime it needs no internet access: the devicetype-library and the Swagger UI assets are baked into the image at build time (build args `DEVICETYPE_LIBRARY_*`, `SWAGGER_UI_VERSION` / `SWAGGER_UI_SHA256`).

Connections and secrets come from the environment (`.env.example`), everything else from `discobox.yaml` (the annotated [`discobox.yaml`](discobox.yaml) in this repo lists every key with its default). See [Configuration](docs/configuration.md).

```env
NETDISCO_URL=https://netdisco.example.com
NETDISCO_TOKEN=your-long-lived-token    # preferred (Bearer prefix added automatically)
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your-token-here
DISCOBOX_AUTH_TOKEN=secret              # bearer token for the action endpoints; unset = no auth
```

```bash
# Docker
docker compose up -d discobox

# venv
python server.py
```

**venv setup:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a && source .env && set +a
```

A one-off sync of one device: `curl -X POST "http://discobox:8080/sync?host=10.0.0.1"` (add `&debug=true` to get its log as plain text).

---

## Netdisco Hook

Add to your Netdisco `config.yml` to trigger a sync automatically after each device discovery:

```yaml
hooks:
  - event: discover
    action: HTTP
    url: "http://discobox:8080/sync?host=[% device.ip %]"
    method: POST
    headers:
      Authorization: "Bearer your-token"
    timeout: 30000
```

---

## Interface defaults

`interface_defaults` fills an interface custom field where it's empty, on every sync of a device whose `device_field` has one of `device_values` (case-insensitive). `device_field` is `cf_<name>` for a custom field (select, multi-select, object or text), like NetBox's API filters, or a device field such as `status`, `role`, `site`, `tenant` or `platform` (matched by value, label, name or slug). A value already set is never overwritten, a deliberate `none` included; a value cleared by hand is filled again on the next sync, so set `none` to opt a port out. `physical_only` (default `true`) skips virtual (SVI, tunnel, loopback), LAG, bridge and wireless interfaces. Rules for the same field: the first matching one wins. A rule whose field doesn't exist on Interface is skipped, and `value` must be a valid choice of that field.

One severity for two teams:

```yaml
interface_defaults:
  - field: interface_severity
    value: warning
    device_field: cf_service_team
    device_values: [netops, campus]
```

Tiered, the first matching rule wins (core switches of netops get `critical`, the rest `warning`):

```yaml
interface_defaults:
  - field: interface_severity
    value: critical
    device_field: cf_device_tier
    device_values: [core, datacenter]
  - field: interface_severity
    value: warning
    device_field: cf_service_team
    device_values: [netops]
```

Two independent fields, one by device status and including virtual interfaces:

```yaml
interface_defaults:
  - field: monitoring_profile
    value: standard
    device_field: status
    device_values: [active]
    physical_only: false
  - field: interface_severity
    value: warning
    device_field: cf_service_team
    device_values: [campus]
```

Existing devices get their values on their next sync; to backfill right away: `curl -X POST "http://discobox:8080/sync/all?cf_service_team=netops&force=true"`. Metric `discobox_interface_defaults_total{field}`. Why a rule didn't apply shows in a debug sync (`/sync?host=...&force=true&debug=true`): `Interface defaults: ... skipped, ...`, or a warning when the field doesn't exist on Interface.

---

## Documentation

- [Configuration](docs/configuration.md): environment variables, `discobox.yaml` sections
- [Features](docs/features.md): what a sync does, per feature
- [Netbox Mapping](docs/netbox-mapping.md): field mapping, supported topologies, custom fields, type aliases
- [Operations](docs/operations.md): jobs, server endpoints, overload protection, metrics, sample log output
