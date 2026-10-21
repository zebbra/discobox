# Netbox Mapping

How Netdisco data maps to Netbox objects and custom fields, per topology.

## Device

| Netdisco field | Netbox field |
|---|---|
| `serial` | `serial` |
| `os_ver` | `custom_fields.os_version` |
| `os` | `custom_fields.os_name` |
| OS release (parsed from `description`) | `custom_fields.os_release` |
| Chassis `model` | `device_type.model` |
| Chassis `type` (vendor prefix) | `device_type.manufacturer` |
| Chassis count (traditional stack only) | `custom_fields.stack_members` |

## Interfaces

| Netdisco field | Netbox field |
|---|---|
| `port` / `descr` | `name` |
| Interface name prefix | `type` (e.g. `10gbase-x-sfpp`, `lag`, `virtual`) |
| `speed` | `speed` (kbps) |
| `duplex` | `duplex` |
| `description` | `description` |
| `up` | `enabled` |
| `mac` | `dcim.mac-addresses` → `primary_mac_address` |

## IP Addresses

| Netdisco field | Netbox field |
|---|---|
| `alias` / `ip` + `subnet` | `ipam.ip-addresses.address` (CIDR) |
| Interface port name | `assigned_object` (dcim.interface) |

## Module Bays & Modules

| Netdisco field | Netbox field |
|---|---|
| Chassis `name` | Module bay `name` |
| Chassis `pos` | Module bay `position` |
| Chassis `model` | Module type `model` / `part_number` |
| Chassis `serial` | Module `serial` |
| Chassis `type` vendor prefix | Module type manufacturer |

## Blades (Linecards / Supervisors)

| Netdisco field | Netbox field |
|---|---|
| Module `name` | Module bay `name` |
| Slot number (from `name`) | Module bay `position` |
| Module `model` | Module type `model` / `part_number` |
| Module `serial` | Module `serial` |

## SFP / Transceivers

| Netdisco field | Netbox field |
|---|---|
| Module `name` (expanded) | Inventory item `name` + interface link |
| Module `model` | Inventory item `part_id` |
| Module `serial` | Inventory item `serial` |

---

## Supported Topologies

| Topology | Detection | Netbox model |
|---|---|---|
| **Standalone** | Single chassis, no stack root | Updates `device_type` directly on the device |
| **Traditional stack** (e.g. Cisco 3850) | `class=stack` root present | Module bay + module per stack member; interfaces linked to their member's module |
| **Nexus FEX** (e.g. N9K-C93180LC-EX) | Stack root type `cevContainerNexusLogicalFabric` | Primary N9K updates `device_type`; each FEX unit becomes a module bay + module; FEX interfaces linked to their FEX module |
| **Cat9500 / Cat9600 StackWise Virtual** | Stack root type contains `VirtualStack` or name contains `Virtual Stack` | Two separate Netbox devices linked via Virtual Chassis; each gets its own blades and device type; partner found by serial or hostname (`-2` suffix) |
| **HA pair / VIP** (e.g. Fortinet) | SNMP hostname differs from Netbox device found by IP, or hostname contains `p1h`/`p2h`, `node1`/`node2`, or `-1`/`-2` | Virtual Chassis created linking both physical nodes and the VIP device (p0h). Management VIP IP stays on the VIP device record; interface VIPs are assigned to whichever physical node syncs first. All shared IPs get `role=vip`. Behaviour of the VIP device is controlled by `sync.vip_mode` / `sync.vip_mode_by_vendor`: `off` (default) does nothing, `threenode` adds it as VC member at position 0 and keeps its IPs, `soft` unassigns its IPs, `hard` deletes it (see [Configuration](configuration.md)). |
| **Modular chassis** (e.g. N9K-C9508, C9606R) | Standalone with `class=module` blade entries | Blades (linecards, supervisors, fabric modules) modelled as module bays |

---

## Netbox Custom Fields

### Written by discobox (Netdisco → Netbox)

These fields are updated on every sync. Create them on the **Device** object in Netbox.

| Slug | Type | Description |
|---|---|---|
| `os_version` | Text | OS version string (e.g. `17.3.4`) |
| `os_name` | Text | OS platform name (e.g. `ios-xe`, `fortios`, `nx-os`) |
| `os_release` | Text | IOS release name parsed from device description (e.g. `Gibraltar`) |
| `controller` | Object → Device | Set on an AP to the WLC reporting it (`custom_fields.controller`; skipped unless the field exists, `null` disables). Generic name so other controller-managed devices can use it later |
| `inventory_last_rebuild` | Date & time | Stamped (UTC) after every `/rebuild` with `dry_run=false`, whether or not it changed anything; never by a normal sync or a dry-run (`custom_fields.inventory_rebuild`; skipped unless the field exists, `null` disables) |
| `stack_members` | Integer | Total physical units in a traditional stack with more than one member. Unset for standalone, a degraded 1-member stack (a count of `1` adds no signal), VSS (split across two Netbox devices), and FEX (satellites aren't stack members). By default (`custom_fields.stack_members_only_increase: true`, yaml-only) the recorded value never decreases — a dead member won't silently reduce it |

### Read by discobox (Netbox → Netdisco, discover jobs)

These fields are optional. If set on a Device they are passed to the Netdisco discovery queue. The names are configurable (`custom_fields.auth_profile`, `custom_fields.snmp_timeout`; null = not used).

| Slug | Type | Description |
|---|---|---|
| `snmp_auth_profile` | Select | SNMP credential tag in Netdisco (the discover job's `device_auth` tag hint). With `reconcile.require_auth_tag: true`, reconcile only enqueues devices that have it |
| `snmp_polling_timeout` | Select | SNMP polling budget, e.g. `2m`; the discover job's per-request timeout is derived from it (`netdisco.discover_snmptimeout`, floor when unset) |
| `snmp_models` | Text | Set on **DeviceType** / **ModuleType**: raw SNMP model strings (comma- or newline-separated) that map to this type, e.g. `enterprises.2440`. See [Type aliases](#type-aliases) |

### Type aliases

SNMP often reports a model that isn't what the type should be called in Netbox, e.g. `enterprises.2440`
(a sysObjectID left over when Netdisco has no matching MIB). Before its usual matching (part number,
model, slug), discobox translates the raw model string through:

1. `types.device_aliases` / `types.module_aliases` in `discobox.yaml`: exact (case-insensitive) or `/regex/` keys → Netbox part number (or model/slug) of an existing type; vendor-scoped entries win over unscoped ones
2. the `snmp_models` custom field (`types.alias_cf`) on DeviceType/ModuleType, if it exists in Netbox

```yaml
types:
  create_missing: true
  alias_cf: snmp_models
  device_aliases:
    "enterprises.2440": efficientip-generic
    cisco:                        # vendor-scoped: Netdisco vendor or Netbox manufacturer name
      "N9KC93180YCFX3": N9K-C93180YC-FX3   # Netdisco placeholder chassis (no ENTITY-MIB data)
    fortinet:
      ".107.1.50001": FWB_VM
```

Models that are just OID fragments (`enterprises.2440`, `.1570`, `.107.1.50001`) are never trusted.
They resolve only through an alias, and are never matched or created as a type themselves. Such a
fragment is only unique within its vendor's enterprise tree, so scope it under the vendor (nested
mapping, vendor compared ignoring case and punctuation). `enterprises.<n>` already names the vendor and
can stay unscoped. Quote the keys: YAML reads an unquoted `.1570` as the number `0.157`.

An alias whose target type doesn't exist in Netbox is logged and skipped. discobox never falls back to
creating a type from the raw string. For devices without ENTITY-MIB chassis modules, the alias is
checked against Netdisco's device model and the device's current Netbox type. So a device already on a
wrongly auto-created `enterprises.2440` type moves to the aliased type on its next sync, and the old type
can then be deleted. With
`types.create_missing: false`, discobox never creates types. An unmatched model is logged and the device
keeps its current type (auto-create skips the device instead).

### Written by discobox on Interfaces (Netdisco → Netbox)

CDP/LLDP neighbor data from Netdisco, written on every interface sync. Text fields — no dependency on the neighbor existing in Netbox.

| Slug | Type | Object | Description |
|---|---|---|---|
| `lldp_neighbor` | Text | `dcim.interface` | Neighbor device hostname as reported by CDP/LLDP |
| `lldp_neighbor_port` | Text | `dcim.interface` | Neighbor port name as reported by CDP/LLDP |
