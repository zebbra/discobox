# Device rules

`interface_defaults` and `reconcile.exclude` select Netbox devices with the same `match` rules.

## Match syntax

The rules work like the ones in [snmp-modulator](https://github.com/zebbra/snmp-modulator): `match` maps a dotted path on the NetBox device (`status.value`, `role.slug`, `site.slug`, `device_type.manufacturer.slug`, `custom_fields.<name>`, `tags`, ...) to a regex. Every key must match (`re.fullmatch`, case-insensitive). A list field (tags by slug, multi-select) matches when any element does. An object custom field needs its attribute, e.g. `custom_fields.team.slug`.

## Interface defaults

`interface_defaults` fills interface custom fields where they're empty, on every sync of a device that matches a rule.

`set` maps interface custom fields to their value. A value already set is never overwritten, a deliberate `none` included; a value cleared by hand is filled again on the next sync, so set `none` to opt a port out. Per field, the first rule that matches and covers the interface wins. `physical_only` (default `true`) skips virtual (SVI, tunnel, loopback), LAG, bridge and wireless interfaces. A field that doesn't exist on Interface is skipped with a warning, and a value must be a valid choice of its field.

One severity for two teams:

```yaml
interface_defaults:
  - name: team severity
    match:
      custom_fields.service_team: "netops|campus"
    set:
      interface_severity: warning
```

Tiered, the first matching rule wins (active core switches get `critical` and a monitoring profile, the rest of netops `warning`):

```yaml
interface_defaults:
  - name: core
    match:
      role.slug: "core-.*"
      status.value: active
    set:
      interface_severity: critical
      monitoring_profile: full
  - name: netops
    match:
      custom_fields.service_team: netops
    set:
      interface_severity: warning
```

By tag, including virtual interfaces:

```yaml
interface_defaults:
  - name: branch
    match:
      tags: branch
      site.slug: "br-.*"
    set:
      monitoring_profile: standard
    physical_only: false
```

Existing devices get their values on their next sync; to backfill right away: `curl -X POST "http://discobox:8080/sync/all?cf_service_team=netops&force=true"` (extra parameters are NetBox device filters). Metric `discobox_interface_defaults_total{field}`. A debug sync (`/sync?host=...&force=true&debug=true`) logs every rule key as `MATCH`/`MISS` with its pattern and value.

## Reconcile exclude

`reconcile.exclude` keeps Netbox devices out of the gaps lists, so neither `discover-missing` nor `fix-tags` ever pushes them to Netdisco. A device matching any rule is counted in `discobox_gaps_excluded` and the gaps job summary. Syncs of devices Netdisco already knows are not affected, and neither is a manual `/discover`.

```yaml
reconcile:
  exclude:
    - name: lab gear
      match:
        name: "lab-.*"
    - name: appliances
      match:
        device_type.manufacturer.slug: "vendor-a|vendor-b"
    - name: one model
      match:
        device_type.slug: "model-x.*"
```

Vendor and model come from the Netbox DeviceType: a device with a placeholder type may not match a model rule, so a name rule is the safer choice there. A debug log line names the rule that excluded a device.
