#!/usr/bin/env python3
"""
typesync: enrich existing Netbox DeviceTypes from the netbox-community
devicetype-library (fill-blank only, never overwrites; only types discobox
minted itself are renamed to the library's name).

Used through discobox's HTTP API (GET/POST /types/library, see server.py).
Dry-run unless apply.

What it adds, only where Netbox has nothing yet:
  - DeviceType fields: weight (+unit), airflow, description, comments
    (datasheet link); u_height only when no device of the type is racked
  - component templates: interfaces, console ports, console server ports,
    power ports (missing names only; existing templates are never touched)

A type discobox created bare (see plan_rename) also takes the library's
model, part_number and slug; its raw names move to the alias custom field
(types.alias_cf) so the next sync still finds it. Types from any other source
keep their name.

Never imported: module/device bays (discobox owns those and its housekeeping
deletes template bays as stale), front/rear ports, power outlets, inventory
items, images. Templates only apply to devices created afterwards, so existing
devices never change.

Library entries are matched to a Netbox type by `library.device_types`
(Netbox model/part_number/slug → library part_number/model/slug) first, then by
part_number, then by exact model name, both within the same manufacturer, and
for Fortinet last by the SKU decoded from its MIB product name (fac3kf →
FAC-3000F, see fortinet_sku). Netbox part_numbers are often just
the model Netdisco reported, so review the "match" column before --apply.
"""
from __future__ import annotations

import logging
import re
import threading
from pathlib import Path
from typing import Iterable, Optional

import yaml

from discobox import NetboxClient
from discobox.util import _split_aliases

log = logging.getLogger("discobox.typesync")

CREATED_BY_DISCOBOX = "Created by discobox"

# library key → (pynetbox dcim endpoint, fields passed through)
COMPONENTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "interfaces": ("interface_templates", (
        "name", "label", "type", "enabled", "mgmt_only", "description", "poe_mode", "poe_type", "rf_role",
    )),
    "console-ports": ("console_port_templates", ("name", "label", "type", "description")),
    "console-server-ports": ("console_server_port_templates", ("name", "label", "type", "description")),
    "power-ports": ("power_port_templates", (
        "name", "label", "type", "maximum_draw", "allocated_draw", "description",
    )),
}
# Present in the library, deliberately not imported (see module docstring).
SKIPPED_COMPONENTS = (
    "module-bays", "device-bays", "front-ports", "rear-ports", "port-mappings",
    "power-outlets", "inventory-items",
)


# ── library ────────────────────────────────────────────────────────────────────

def _norm(value) -> str:
    return str(value or "").strip().lower()


_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)   # libyaml: ~10x faster


def _vendor_key(name) -> str:
    return "".join(ch for ch in _norm(name) if ch.isalnum())


def load_library(roots: Iterable[Path], vendors: Optional[Iterable[str]] = None) -> dict[str, dict]:
    """
    Index device-types/<Vendor>/*.yaml of each root by part_number, model and
    slug (lowercased). Later roots win, so an overlay listed after the library
    overrides or fills its gaps (e.g. models the library doesn't have yet).
    vendors limits parsing to those vendor directories (compared ignoring case
    and punctuation): the full library is ~10k files.
    """
    wanted = {_vendor_key(v) for v in vendors} if vendors is not None else None
    index: dict[str, dict] = {}
    for root in roots:
        base = Path(root) / "device-types"
        if not base.is_dir():
            raise FileNotFoundError(f"{base} not found (expected a devicetype-library checkout)")
        vendor_dirs = [d for d in sorted(base.iterdir()) if d.is_dir()
                       and (wanted is None or _vendor_key(d.name) in wanted)]
        for path in (p for d in vendor_dirs for p in sorted(d.rglob("*.y*ml"))):
            try:
                entry = yaml.load(path.read_text(), Loader=_YAML_LOADER) or {}
            except yaml.YAMLError as exc:
                log.warning("Skipping %s: %s", path, exc)
                continue
            if not isinstance(entry, dict) or not entry.get("model"):
                continue
            entry["_file"] = str(path.relative_to(root))
            for key in (entry.get("part_number"), entry.get("model"), entry.get("slug")):
                if key:
                    index[_norm(key)] = entry
    return index


class Library:
    """Per-vendor lazily loaded, process-cached library index (thread-safe)."""

    def __init__(self, roots: list[Path]):
        self.roots = roots
        self._by_vendor: dict[str, dict[str, dict]] = {}
        self._lock = threading.Lock()

    def index_for(self, vendors: Iterable[str]) -> dict[str, dict]:
        index: dict[str, dict] = {}
        for vendor in sorted({_vendor_key(v) for v in vendors if v}):
            with self._lock:
                if vendor not in self._by_vendor:
                    self._by_vendor[vendor] = load_library(self.roots, [vendor])
            index.update(self._by_vendor[vendor])
        return index


def match_library(
    nb_type: dict, index: dict[str, dict], mapping: Optional[dict] = None,
) -> tuple[Optional[dict], str]:
    """
    Library entry for a Netbox type as (entry, how). nb_type needs model,
    part_number, slug and manufacturer (name). how is "map", "part_number",
    "model" (exact library model name, e.g. a type imported from the library
    earlier) or "" when nothing matched. part_number/model need the same
    manufacturer.
    """
    keys = [_norm(nb_type.get(k)) for k in ("model", "part_number", "slug")]
    mapping = {_norm(k): v for k, v in (mapping or {}).items()}
    for key in keys:
        if key and key in mapping:
            target = index.get(_norm(mapping[key]))
            if target is None:
                log.warning("  %s: mapped to %r, not in library", nb_type.get("model"), mapping[key])
            return target, "map" if target else ""
    same_vendor = lambda e: _vendor_key(e.get("manufacturer")) == _vendor_key(nb_type.get("manufacturer"))  # noqa: E731
    for field in ("part_number", "model"):
        value = _norm(nb_type.get(field))
        entry = index.get(value) if value else None
        if entry and _norm(entry.get(field)) == value and same_vendor(entry):
            return entry, field
    if _vendor_key(nb_type.get("manufacturer")) == "fortinet":
        for field in ("part_number", "model"):
            sku = fortinet_sku(nb_type.get(field))
            entry = index.get(_norm(sku)) if sku else None
            if entry and _norm(entry.get("part_number")) == _norm(sku) and same_vendor(entry):
                _log_decoded(nb_type.get(field), sku)
                return entry, "fortinet"
    return None, ""


# Fortinet devices report FORTINET-*-MIB product names, not SKUs: fac3kf =
# FAC-3000F (digit, h = x100 / k = x1000, generation letter). Only a guess:
# match_library accepts it only when the library has exactly that part_number;
# odd names (VMs, FG-60E-POE, ...) go through library.device_types.
_FORTINET_PREFIXES = {
    "fac": "FAC", "fgt": "FG", "fwb": "FWB", "fad": "FAD", "fadc": "FAD",
    "fpx": "FPX", "faz": "FAZ", "fmg": "FMG",
}
_FORTINET_MODEL_RE = re.compile(r"^(fadc|fac|fgt|fwb|fad|fpx|faz|fmg)(\d+)([hk]?)([a-z]\w*)$", re.IGNORECASE)
_decoded_logged: set[str] = set()


def fortinet_sku(model) -> Optional[str]:
    """Fortinet MIB product name → SKU guess ("fac3kf" → "FAC-3000F"), None if it doesn't parse."""
    m = _FORTINET_MODEL_RE.match(str(model or "").strip())
    if not m:
        return None
    prefix, digits, scale, suffix = m.groups()
    number = int(digits) * {"h": 100, "k": 1000}.get(scale.lower(), 1)
    return f"{_FORTINET_PREFIXES[prefix.lower()]}-{number}{suffix.upper()}"


def _log_decoded(model, sku: str) -> None:
    # once per process: the metrics refresh matches every bare type hourly
    if model not in _decoded_logged:
        _decoded_logged.add(model)
        log.info("Fortinet model %r decoded as library part_number %s", model, sku)


# ── diff (pure) ────────────────────────────────────────────────────────────────

def plan_type_fields(nb_type: dict, entry: dict, racked: int) -> dict:
    """
    Fill-blank patch for the DeviceType itself. nb_type holds plain values:
    weight, weight_unit, airflow, description, comments, u_height.
    """
    patch: dict = {}
    if nb_type.get("weight") is None and entry.get("weight") is not None and entry.get("weight_unit"):
        patch["weight"] = entry["weight"]
        patch["weight_unit"] = entry["weight_unit"]
    if not nb_type.get("airflow") and entry.get("airflow"):
        patch["airflow"] = entry["airflow"]
    if not (nb_type.get("description") or "").strip() and entry.get("description"):
        patch["description"] = entry["description"]
    comments = (nb_type.get("comments") or "").strip()
    lib_comments = (entry.get("comments") or "").strip()
    if lib_comments and lib_comments not in comments:
        if not comments:
            patch["comments"] = lib_comments
        elif comments == CREATED_BY_DISCOBOX:
            patch["comments"] = f"{lib_comments}\n\n{CREATED_BY_DISCOBOX}"
    # u_height has a default (1), so "blank" can't be told apart from a
    # deliberate value: only follow the library while nothing is racked,
    # where a change can't collide with rack space.
    if "u_height" in entry and not racked:
        try:
            if float(nb_type.get("u_height")) != float(entry["u_height"]):
                patch["u_height"] = entry["u_height"]
        except (TypeError, ValueError):
            pass
    return patch


def plan_components(existing: dict[str, set[str]], entry: dict) -> dict[str, list[dict]]:
    """
    Component templates to create per library key: only names the type
    doesn't have yet (case-insensitive). existing maps library key → names.
    """
    out: dict[str, list[dict]] = {}
    for key, (_, fields) in COMPONENTS.items():
        have = {_norm(n) for n in existing.get(key, set())}
        new = [
            {f: c[f] for f in fields if c.get(f) is not None}
            for c in entry.get(key) or []
            if c.get("name") and _norm(c["name"]) not in have
        ]
        if new:
            out[key] = new
    return out


# ── Netbox ─────────────────────────────────────────────────────────────────────

def _plain(value):
    """ChoiceItem / Record → plain value for comparisons."""
    if hasattr(value, "value"):
        return value.value
    return value


def _nb_type_dict(dt) -> dict:
    return {
        "id": dt.id,
        "model": dt.model,
        "slug": dt.slug,
        "part_number": getattr(dt, "part_number", "") or "",
        "manufacturer": getattr(getattr(dt, "manufacturer", None), "name", "") or "",
        "weight": getattr(dt, "weight", None),
        "weight_unit": _plain(getattr(dt, "weight_unit", None)),
        "airflow": _plain(getattr(dt, "airflow", None)),
        "description": getattr(dt, "description", "") or "",
        "comments": getattr(dt, "comments", "") or "",
        "u_height": getattr(dt, "u_height", None),
        "device_count": getattr(dt, "device_count", None),
    }


def _source_of(dt, source_cf: Optional[str]) -> Optional[str]:
    """The type's source value ("" when unset); None when the CF isn't on DeviceType (or not configured)."""
    cf = dict(getattr(dt, "custom_fields", None) or {})
    if not source_cf or source_cf not in cf:
        return None
    value = cf.get(source_cf)
    return str(getattr(value, "value", None) or (value.get("value") if isinstance(value, dict) else value) or "")


def is_bare(dt, source_cf: Optional[str], bare_value: str) -> bool:
    """A type discobox minted without templates: source=bare_value, or unset with discobox's creation comment."""
    source = _source_of(dt, source_cf)
    if source:
        return source == bare_value
    return CREATED_BY_DISCOBOX in (getattr(dt, "comments", "") or "")


def plan_source(dt, matched: bool, source_cf: Optional[str], bare_value: str, library_value: str) -> Optional[str]:
    """
    New source for a reported type, or None to leave it: library-matched →
    library_value, unless another source (manual, …) owns it; unmatched and
    minted by discobox with no source yet → bare_value (the backfill).
    """
    source = _source_of(dt, source_cf)
    if source is None:
        return None
    if matched:
        return library_value if source in ("", bare_value) else None
    if not source and CREATED_BY_DISCOBOX in (getattr(dt, "comments", "") or ""):
        return bare_value
    return None


RENAME_FIELDS = ("model", "part_number", "slug")


def plan_rename(
    nb: NetboxClient, dt, t: dict, entry: dict,
    source_cf: Optional[str], bare_value: str, library_value: str,
) -> dict:
    """
    Rename of a discobox-minted type to its library entry, as report fields:
    {} (nothing to do, or not ours), {"rename": {field: new}, "aliases": [raw
    names for the alias CF]} or {"rename_skipped": why}.

    Ours: source bare_value, or the creation comment with no source or one an
    earlier apply set (library_value). The raw names must stay resolvable for
    get_or_create_device_type, else the next sync mints the bare type again.
    """
    source = _source_of(dt, source_cf)
    if source not in (None, "", bare_value, library_value):
        return {}
    if source != bare_value and CREATED_BY_DISCOBOX not in (getattr(dt, "comments", "") or ""):
        return {}
    rename = {f: entry[f] for f in RENAME_FIELDS if entry.get(f) and entry[f] != t.get(f)}
    if not rename:
        return {}
    new_refs = {_norm(entry.get(f)) for f in ("model", "part_number")}
    aliases = [
        raw for raw in dict.fromkeys(t.get(f) for f in ("model", "part_number") if f in rename)
        if raw and _norm(raw) not in new_refs
    ]
    if any(re.search(r"[,\n]", raw) for raw in aliases):
        return {"rename_skipped": "raw name can't be stored as an alias (comma)"}
    if aliases and not nb.type_alias_field("dcim.devicetype"):
        return {"rename_skipped": "no alias custom field (types.alias_cf) on DeviceType to keep the raw name"}
    old_refs = {t.get(f) for f in rename}
    for _, _, target in getattr(nb, "device_type_aliases", None) or ():
        if target in old_refs:
            return {"rename_skipped": f"types.device_aliases points at {target!r}: update the alias first"}
    mfr_id = getattr(getattr(dt, "manufacturer", None), "id", None)
    for field, value in rename.items():
        # model and slug are unique per manufacturer; a part_number anywhere
        # else is what get_or_create_device_type would pick up instead
        scope = {} if field == "part_number" else {"manufacturer_id": mfr_id}
        other = next((o for o in nb.nb.dcim.device_types.filter(**scope, **{field: value}) if o.id != dt.id), None)
        if other:
            return {"rename_skipped": f"{field} {value!r} already exists (DeviceType id {other.id})"}
    return {"rename": rename, "aliases": aliases}


def _merged_aliases(dt, cf: str, aliases: list[str]) -> str:
    """The type's alias CF value with aliases appended (case-insensitive, existing order kept)."""
    have = _split_aliases((getattr(dt, "custom_fields", None) or {}).get(cf))
    seen = {a.lower() for a in have}
    return ", ".join(have + [a for a in aliases if a.lower() not in seen])


def resolve_roles(nb: NetboxClient, roles: list[str]) -> list[int]:
    """Device role slugs, names or ids → ids. ValueError naming the unknown ones (and the valid slugs)."""
    known = list(nb.nb.dcim.device_roles.all())
    by_key: dict[str, int] = {}
    for r in known:
        by_key[str(r.id)] = r.id
        by_key[_norm(r.slug)] = r.id
        by_key[_norm(r.name)] = r.id
    ids, unknown = [], []
    for role in roles:
        rid = by_key.get(str(role).strip()) or by_key.get(_norm(str(role)))
        (ids.append(rid) if rid else unknown.append(str(role)))
    if unknown:
        raise ValueError(
            f"Unknown device role(s): {', '.join(unknown)} (valid slugs: "
            f"{', '.join(sorted(r.slug for r in known))})"
        )
    return ids


def select_types(
    nb: NetboxClient, roles: list[str], types: list[str],
    bare: bool = False, source_cf: Optional[str] = None, bare_value: str = "netdisco",
) -> list:
    """
    DeviceTypes used by devices of roles (slug, name or id), plus those named
    in types (model/slug/part_number), plus with bare every type discobox
    minted without templates (see is_bare).
    """
    selected: dict[int, object] = {}
    if bare:
        for dt in nb.nb.dcim.device_types.all():
            if is_bare(dt, source_cf, bare_value):
                selected[dt.id] = dt
    if roles:
        role_ids = resolve_roles(nb, roles)
        ids = {d.device_type.id for d in nb.nb.dcim.devices.filter(role_id=role_ids) if d.device_type}
        for dt_id in ids:
            dt = nb.nb.dcim.device_types.get(dt_id)
            if dt:
                selected[dt.id] = dt
    for name in types:
        for field in ("model", "slug", "part_number"):
            hits = [dt for dt in nb.nb.dcim.device_types.filter(**{field: name})
                    if _norm(getattr(dt, field, "")) == _norm(name)]
            for dt in hits:
                selected[dt.id] = dt
            if hits:
                break
        else:
            log.warning("DeviceType %r not found in Netbox", name)
    return sorted(selected.values(), key=lambda dt: dt.model.lower())


def build_report(
    nb: NetboxClient, library: Library, mapping: dict, dts: list,
    source_cf: Optional[str] = None, bare_value: str = "netdisco", library_value: str = "devicetype-library",
) -> list[dict]:
    """One item per DeviceType: its library match and what enrichment would add."""
    # Only the vendor directories of the selected types' manufacturers are
    # parsed, so a library.device_types target must live under the same vendor.
    index = library.index_for(getattr(getattr(dt, "manufacturer", None), "name", "") for dt in dts)
    items = []
    for dt in dts:
        t = _nb_type_dict(dt)
        entry, how = match_library(t, index, mapping)
        item: dict = {
            "id": t["id"], "manufacturer": t["manufacturer"], "model": t["model"],
            "part_number": t["part_number"], "devices": t["device_count"],
            "library": entry["_file"] if entry else None, "match": how or None,
        }
        if _source_of(dt, source_cf) is not None:
            item["source"] = _source_of(dt, source_cf) or None
            new_source = plan_source(dt, bool(entry), source_cf, bare_value, library_value)
            if new_source:
                item["set_source"] = new_source
        if entry:
            racked = nb.nb.dcim.devices.count(device_type_id=dt.id) - \
                nb.nb.dcim.devices.count(device_type_id=dt.id, rack_id="null")
            existing = {
                key: {tpl.name for tpl in getattr(nb.nb.dcim, ep).filter(device_type_id=dt.id)}
                for key, (ep, _) in COMPONENTS.items()
            }
            item["set"] = plan_type_fields(t, entry, racked)
            item.update(plan_rename(nb, dt, t, entry, source_cf, bare_value, library_value))
            item["add"] = plan_components(existing, entry)
            item["not_imported"] = [k for k in SKIPPED_COMPONENTS if entry.get(k)]
            item["images_in_library"] = bool(entry.get("front_image") or entry.get("rear_image"))
        items.append(item)
    return items


def apply_report(nb: NetboxClient, items: list[dict], dts: list, source_cf: Optional[str] = None) -> int:
    """Write each item's set/add, its rename, then its set_source; returns the number of failed types."""
    by_id = {dt.id: dt for dt in dts}
    errors = 0
    for item in items:
        if not any(item.get(k) for k in ("set", "add", "rename", "set_source")):
            continue
        dt = by_id[item["id"]]
        try:
            if item.get("set"):
                dt.update(item["set"])
            for key, comps in (item.get("add") or {}).items():
                ep = getattr(nb.nb.dcim, COMPONENTS[key][0])
                ep.create([{**c, "device_type": dt.id} for c in comps])
            if item.get("rename"):
                patch: dict = dict(item["rename"])
                if item.get("aliases"):
                    # same PATCH as the rename: never renamed without its alias
                    cf = nb.type_alias_field("dcim.devicetype")
                    patch["custom_fields"] = {cf: _merged_aliases(dt, cf, item["aliases"])}
                dt.update(patch)
                log.info("%s / %s: renamed to %s", item["manufacturer"], item["model"],
                         item["rename"].get("model") or item["rename"])
            # last: a type only counts as library-filled once the fill went through
            if item.get("set_source") and source_cf:
                dt.update({"custom_fields": {source_cf: item["set_source"]}})
            item["applied"] = True
        except Exception as exc:
            errors += 1
            item["error"] = str(exc)
            log.error("%s / %s: apply failed: %s", item["manufacturer"], item["model"], exc)
    return errors


def summarize(items: list[dict]) -> dict:
    return {
        "types": len(items),
        "matched": sum(1 for i in items if i["library"]),
        "unmatched": sum(1 for i in items if not i["library"]),
        "fields": sum(len(i.get("set") or {}) for i in items),
        "templates": sum(len(v) for i in items for v in (i.get("add") or {}).values()),
        "errors": sum(1 for i in items if i.get("error")),
        "source_changes": sum(1 for i in items if i.get("set_source")),
        "renames": sum(1 for i in items if i.get("rename")),
    }


def sync_types(
    nb: NetboxClient, library: Library, mapping: dict,
    roles: list[str], types: list[str], apply: bool = False,
    bare: bool = False, source_cf: Optional[str] = None,
    bare_value: str = "netdisco", library_value: str = "devicetype-library",
) -> dict:
    """Select, report and (with apply) write. Shared by the HTTP API and the CLI."""
    dts = select_types(nb, roles, types, bare=bare, source_cf=source_cf, bare_value=bare_value)
    items = build_report(nb, library, mapping, dts, source_cf, bare_value, library_value)
    if apply:
        apply_report(nb, items, dts, source_cf)
    return {"apply": apply, "summary": summarize(items), "types": items}
