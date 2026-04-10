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
--housekeeping     Remove stale device bays and empty dummy interfaces
--debug            Enable debug logging
```

---

## Features

- **Device fields** — updates serial number, OS version, OS name, OS release (parsed from description)
- **Interfaces** — creates and updates all physical and logical interfaces; maps interface type by name prefix
- **MAC addresses** — creates `dcim.mac-addresses` objects and links them as primary MAC per interface (Netbox 4.x model)
- **IP addresses** — assigns interface IPs; fixes prefix mismatches (e.g. `/32` → `/26`); moves IPs from dummy placeholder interfaces to the correct one
- **Module bays & modules** — models physical chassis members as module bays with installed modules; assigns interfaces to their parent module
- **Device type auto-creation** — creates manufacturer, device type (with part number and slug) if not present in Netbox
- **SFP / transceiver inventory** — creates inventory items for transceivers with serial numbers, linked to their interface
- **PSU inventory** — creates inventory items for power supplies
- **Housekeeping** — removes stale device bays auto-created from DeviceType templates (e.g. `PS-A`, `Fan 1`) and deletes empty dummy interfaces

---

## Supported Topologies

| Topology | Detection | Netbox model |
|---|---|---|
| **Standalone** | Single chassis, no stack root | Updates `device_type` directly on the device |
| **Traditional stack** (e.g. Cisco 3850) | `class=stack` root present | Module bay + module per stack member; interfaces linked to their member's module |
| **Nexus FEX** (e.g. N9K-C93180LC-EX) | Stack root type `cevContainerNexusLogicalFabric` | Primary N9K updates `device_type`; each FEX unit becomes a module bay + module; FEX interfaces linked to their FEX module |

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
NETDISCO_USER=admin
NETDISCO_PASS=secret
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your-token-here

# Server mode
DISCOBOX_PORT=8080          # default: 8080
DISCOBOX_WORKERS=4          # uvicorn worker count, default: 4
DISCOBOX_AUTH_TOKEN=secret  # bearer token for /sync; leave unset to disable auth
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
| `/sync?host=<ip>` | POST | yes | Trigger device sync (also accepts JSON body `{"host": "<ip>"}`) |
| `/metrics` | GET | no | Prometheus metrics |
| `/health` | GET | no | Liveness check + in-flight hosts |
| `/docs` | GET | no | Swagger UI |

Syncs run in a background thread pool. Duplicate requests for the same host are dropped while a sync is in progress.

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
