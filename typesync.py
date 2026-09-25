#!/usr/bin/env python3
"""
typesync: enrich existing Netbox DeviceTypes from the netbox-community
devicetype-library (fill-blank only, never overwrites or renames).

Primary interface is discobox's HTTP API (GET/POST /types/library, see
server.py); this file also runs standalone. Dry-run by default.

    python typesync.py --library ../devicetype-library --role lwapp-ap
    python typesync.py --library ../devicetype-library --type 9120AX --apply

What it adds, only where Netbox has nothing yet:
  - DeviceType fields: weight (+unit), airflow, description, comments
    (datasheet link); u_height only when no device of the type is racked
  - component templates: interfaces, console ports, console server ports,
    power ports (missing names only; existing templates are never touched)

Never imported: module/device bays (discobox owns those and its housekeeping
deletes template bays as stale), front/rear ports, power outlets, inventory
items, images. Templates only apply to devices created afterwards, so existing
devices never change.

Library entries are matched to a Netbox type by `library.device_types`
(Netbox model/part_number/slug → library part_number/model/slug) first, then by
part_number, then by exact model name, both within the same manufacturer. Netbox part_numbers are often just
the model Netdisco reported, so review the "match" column before --apply.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Iterable, Optional

import yaml

from discobox import NetboxClient

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
    return None, ""


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


def select_types(nb: NetboxClient, roles: list[str], types: list[str]) -> list:
    """DeviceTypes used by devices of roles, plus those named in types (model/slug/part_number)."""
    selected: dict[int, object] = {}
    if roles:
        ids = {d.device_type.id for d in nb.nb.dcim.devices.filter(role=roles) if d.device_type}
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


def build_report(nb: NetboxClient, library: Library, mapping: dict, dts: list) -> list[dict]:
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
        if entry:
            racked = nb.nb.dcim.devices.count(device_type_id=dt.id) - \
                nb.nb.dcim.devices.count(device_type_id=dt.id, rack_id="null")
            existing = {
                key: {tpl.name for tpl in getattr(nb.nb.dcim, ep).filter(device_type_id=dt.id)}
                for key, (ep, _) in COMPONENTS.items()
            }
            item["set"] = plan_type_fields(t, entry, racked)
            item["add"] = plan_components(existing, entry)
            item["not_imported"] = [k for k in SKIPPED_COMPONENTS if entry.get(k)]
            item["images_in_library"] = bool(entry.get("front_image") or entry.get("rear_image"))
        items.append(item)
    return items


def apply_report(nb: NetboxClient, items: list[dict], dts: list) -> int:
    """Write each item's set/add; returns the number of failed types."""
    by_id = {dt.id: dt for dt in dts}
    errors = 0
    for item in items:
        if not item.get("set") and not item.get("add"):
            continue
        dt = by_id[item["id"]]
        try:
            if item.get("set"):
                dt.update(item["set"])
            for key, comps in (item.get("add") or {}).items():
                ep = getattr(nb.nb.dcim, COMPONENTS[key][0])
                ep.create([{**c, "device_type": dt.id} for c in comps])
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
    }


def sync_types(
    nb: NetboxClient, library: Library, mapping: dict,
    roles: list[str], types: list[str], apply: bool = False,
) -> dict:
    """Select, report and (with apply) write. Shared by the HTTP API and the CLI."""
    dts = select_types(nb, roles, types)
    items = build_report(nb, library, mapping, dts)
    if apply:
        apply_report(nb, items, dts)
    return {"apply": apply, "summary": summarize(items), "types": items}


def log_report(result: dict) -> None:
    for item in result["types"]:
        label = f"{item['manufacturer']} / {item['model']} (pn={item['part_number'] or '-'}, devices={item['devices']})"
        if not item["library"]:
            log.info("%-60s no library match: add it to library.device_types or the overlay", label)
            continue
        log.info("%-60s → %s (%s)", label, item["library"], item["match"])
        for k, v in item["set"].items():
            log.info("    set %-12s %s", k, v if k != "comments" else (str(v).splitlines() or [""])[0])
        for key, comps in item["add"].items():
            log.info("    add %-20s %s", key, ", ".join(c["name"] for c in comps))
        if item["not_imported"]:
            log.info("    not imported: %s", ", ".join(item["not_imported"]))
        if item["images_in_library"]:
            log.info("    images available in library (not imported)")
        if not item["set"] and not item["add"]:
            log.info("    nothing to add")
        if item.get("error"):
            log.info("    ERROR: %s", item["error"])
    log.info("%s: %s", "Applied" if result["apply"] else "Dry-run (use --apply to write)",
             "  ".join(f"{k}={v}" for k, v in result["summary"].items()))


def _load_config(path: str) -> dict:
    try:
        with open(path) as f:
            return (yaml.safe_load(f) or {}).get("library") or {}
    except FileNotFoundError:
        return {}


def main(argv: Optional[list[str]] = None) -> int:
    cfg = _load_config(os.getenv("DISCOBOX_CONFIG") or "discobox.yaml")
    p = argparse.ArgumentParser(description="Enrich Netbox DeviceTypes from the devicetype-library (fill-blank).")
    p.add_argument("--library", default=cfg.get("path"), help="devicetype-library checkout (library.path)")
    p.add_argument("--overlay", default=cfg.get("overlay"),
                   help="extra dir with the same layout, wins over the library (library.overlay)")
    p.add_argument("--role", action="append", default=None,
                   help="only DeviceTypes used by devices of this role slug (repeatable; library.roles)")
    p.add_argument("--type", action="append", default=[], dest="types",
                   help="DeviceType model/slug/part_number to include (repeatable)")
    p.add_argument("--apply", action="store_true", help="write changes (default: dry-run report)")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    if not args.library:
        log.error("No library: pass --library or set library.path")
        return 2
    roles = args.role if args.role is not None else list(cfg.get("roles") or [])
    if not roles and not args.types:
        log.error("Nothing selected: pass --role and/or --type (a full-catalog run is deliberately not supported)")
        return 2

    library = Library([Path(args.library)] + ([Path(args.overlay)] if args.overlay else []))
    nb = NetboxClient(
        url=os.environ["NETBOX_URL"],
        token=os.environ["NETBOX_TOKEN"],
        verify_tls=os.getenv("NETBOX_TLS_VERIFY", "true").lower() != "false",
    )
    result = sync_types(nb, library, cfg.get("device_types") or {}, roles, args.types, apply=args.apply)
    log_report(result)
    return 1 if result["summary"]["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
