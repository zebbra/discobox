# discobox

![DiscoBox](banner.png)

Syncs network device inventory from [Netdisco](https://github.com/netdisco/netdisco) into [Netbox](https://netbox.dev/), enriching existing Netbox device records with live data discovered by Netdisco.

Devices are matched by management IP. Hostnames are verified and a warning is logged on mismatch, but the sync proceeds regardless.

Netbox is written to directly via [pynetbox](https://github.com/netbox-community/pynetbox) for simplicity. Migration to [Netbox Diode](https://github.com/netbox-community/netbox-diode) is a future consideration once its entity coverage matures.

![Flow](flow.svg)

---

## Run Modes

discobox can be run in two modes:

### Server mode (recommended)

A FastAPI server that receives webhook calls from Netdisco after each discovery and runs syncs in the background. Exposes Prometheus metrics and a Swagger UI.

```bash
# Docker
docker compose up -d discobox

# venv
python server.py
```

### One-shot CLI

Sync a single device by IP directly from the command line. Useful for manual runs, testing, or scripting.

```bash
# Docker
docker compose run --rm cli --host 10.0.0.1

# venv
python cli.py --host 10.0.0.1
```

```
--host <IP>        Management IP of the device to sync (required)
--no-mac           Skip MAC address sync
--no-ip            Skip IP address sync
--no-modules       Skip module bay / module sync
--no-sfp           Skip SFP inventory item sync
--no-poe           Skip PoE mode sync
--housekeeping     Remove stale device bays and empty dummy interfaces
--debug            Enable debug logging
```

---

## Features

- **Device fields** — updates serial number, OS version, OS name, OS release (parsed from description)
- **Interfaces** — creates and updates all physical and logical interfaces; maps interface type by name prefix; links subinterfaces (e.g. `Gi0/0/1.100`) to their parent interface
- **PoE** — sets `poe_mode = pse` on ports reported as PoE-capable by Netdisco
- **MAC addresses** — creates `dcim.mac-addresses` objects and links them as primary MAC per interface (Netbox 4.x model)
- **IP addresses** — assigns interface IPs; fixes prefix mismatches (e.g. `/32` → `/26`); moves IPs from dummy placeholder interfaces to the correct one
- **Module bays & modules** — models physical chassis members as module bays with installed modules; assigns interfaces to their parent module
- **Blades** — models linecards, supervisors, and fabric modules as module bays on the device they belong to (routed per VSS member for split chassis)
- **Device type auto-creation** — creates manufacturer, device type (with part number and slug) if not present in Netbox
- **SFP / transceiver inventory** — creates inventory items for transceivers with serial numbers, linked to their interface
- **PSU inventory** — creates inventory items for power supplies
- **HA / VIP detection** — detects cluster VIPs by hostname mismatch; redirects sync to the real active node; creates a Virtual Chassis linking both HA members; optionally deletes the VIP device (housekeeping)
- **Housekeeping** — removes stale device bays auto-created from DeviceType templates (e.g. `PS-A`, `Fan 1`, `Slot 1`) and deletes empty dummy interfaces

---

## Supported Topologies

| Topology | Detection | Netbox model |
|---|---|---|
| **Standalone** | Single chassis, no stack root | Updates `device_type` directly on the device |
| **Traditional stack** (e.g. Cisco 3850) | `class=stack` root present | Module bay + module per stack member; interfaces linked to their member's module |
| **Nexus FEX** (e.g. N9K-C93180LC-EX) | Stack root type `cevContainerNexusLogicalFabric` | Primary N9K updates `device_type`; each FEX unit becomes a module bay + module; FEX interfaces linked to their FEX module |
| **Cat9500 / Cat9600 StackWise Virtual** | Stack root type contains `VirtualStack` or name contains `Virtual Stack` | Two separate Netbox devices linked via Virtual Chassis; each gets its own blades and device type; partner found by serial or hostname (`-2` suffix) |
| **HA pair / VIP** (e.g. Fortinet) | SNMP hostname differs from the Netbox device found by IP | Sync redirected to active physical node; Virtual Chassis created for both nodes; VIP device deleted on housekeeping |
| **Modular chassis** (e.g. N9K-C9508, C9606R) | Standalone with `class=module` blade entries | Blades (linecards, supervisors, fabric modules) modelled as module bays |

---

## Netbox Mapping

### Device

| Netdisco field | Netbox field |
|---|---|
| `serial` | `serial` |
| `os_ver` | `custom_fields.os_version` |
| `os` | `custom_fields.os_name` |
| OS release (parsed from `description`) | `custom_fields.os_release` |
| Chassis `model` | `device_type.model` |
| Chassis `type` (vendor prefix) | `device_type.manufacturer` |

### Interfaces

| Netdisco field | Netbox field |
|---|---|
| `port` / `descr` | `name` |
| Interface name prefix | `type` (e.g. `10gbase-x-sfpp`, `lag`, `virtual`) |
| `speed` | `speed` (kbps) |
| `duplex` | `duplex` |
| `description` | `description` |
| `up` | `enabled` |
| `mac` | `dcim.mac-addresses` → `primary_mac_address` |

### IP Addresses

| Netdisco field | Netbox field |
|---|---|
| `alias` / `ip` + `subnet` | `ipam.ip-addresses.address` (CIDR) |
| Interface port name | `assigned_object` (dcim.interface) |

### Module Bays & Modules

| Netdisco field | Netbox field |
|---|---|
| Chassis `name` | Module bay `name` |
| Chassis `pos` | Module bay `position` |
| Chassis `model` | Module type `model` / `part_number` |
| Chassis `serial` | Module `serial` |
| Chassis `type` vendor prefix | Module type manufacturer |

### Blades (Linecards / Supervisors)

| Netdisco field | Netbox field |
|---|---|
| Module `name` | Module bay `name` |
| Slot number (from `name`) | Module bay `position` |
| Module `model` | Module type `model` / `part_number` |
| Module `serial` | Module `serial` |

### SFP / Transceivers

| Netdisco field | Netbox field |
|---|---|
| Module `name` (expanded) | Inventory item `name` + interface link |
| Module `model` | Inventory item `part_id` |
| Module `serial` | Inventory item `serial` |

---

## Configuration

Copy `.env.example` to `.env` and fill in the values:

```env
NETDISCO_URL=https://netdisco.example.com
NETDISCO_USERNAME=admin
NETDISCO_PASSWORD=secret
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your-token-here

# Server mode
DISCOBOX_PORT=8080          # default: 8080
DISCOBOX_WORKERS=4          # uvicorn worker count, default: 4
DISCOBOX_AUTH_TOKEN=secret  # bearer token for /sync; leave unset to disable auth

# Sync feature defaults (server mode) — can be overridden per-request
DISCOBOX_NO_MAC=true        # disable MAC sync globally
DISCOBOX_NO_IP=true         # disable IP sync globally (e.g. if Netdisco is not VRF-aware)
DISCOBOX_NO_MODULES=true    # disable module bay sync globally
DISCOBOX_NO_SFP=true        # disable SFP inventory sync globally
DISCOBOX_NO_POE=true        # disable PoE sync globally
DISCOBOX_HOUSEKEEPING=true  # enable housekeeping globally
```

**venv setup:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a && source .env && set +a
```

---

## Server Endpoints

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/sync?host=<ip>` | GET, POST | yes | Trigger device sync |
| `/metrics` | GET | no | Prometheus metrics |
| `/health` | GET | no | Liveness check + in-flight hosts |
| `/docs` | GET | no | Swagger UI |

All sync flags are available as query parameters or JSON body fields, defaulting to the env var values:

```
POST /sync?host=10.0.0.1&housekeeping=true&mac=false
POST /sync  {"host": "10.0.0.1", "housekeeping": true, "mac": false}
```

Syncs run in a background thread pool. Duplicate requests for the same host are dropped while a sync is in progress. Concurrent syncs log under `discobox.<ip>` so they are distinguishable in the log stream.

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
