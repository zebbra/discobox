"""Anonymize raw Netdisco JSON dumps for use as test fixtures.

Reads from samples/ (gitignored), writes deterministic-fake versions to
tests/samples/ (committed). Each real value (IP, MAC, serial, hostname,
location, chassis-id, engine-id) is mapped to a stable fake via a
hash-derived placeholder, so the same real value yields the same fake
across all files. Stdlib only.

Run from the repo root:

    python tests/anonymize.py
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "samples"
DST = ROOT / "tests" / "samples"

# Search-shape dumps are not consumed by the sync code — drop.
SKIP = {"fortigate-device.json", "nexus93180lcex-devices.json"}

# Filename rewrites: raw → fixture name.
RENAME = {
    "fortiproxy-devices.json":      "fortiproxy-device.json",
    "fortiproxy-interface.json":    "fortiproxy-ports.json",
    "9500vss-module.json":          "9500vss-modules.json",
    "cat9300-module.json":          "cat9300-modules.json",
    "nexus93180lcex-module.json":   "nexus93180lcex-modules.json",
    "nexusaci-module.json":         "nexusaci-modules.json",
    "3850-poe.json":                "3850-powered_ports.json",
}

NULL_MAC = "00:00:00:00:00:00"
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


# ── deterministic fake generators ──────────────────────────────────────────────

def _h(s: str, n: int = 8) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:n]


def _gen_ip(real: str) -> str:
    if real in ("0.0.0.0", "255.255.255.255"):
        return real
    # RFC 5737 TEST-NET-1
    return f"192.0.2.{int(_h(real, 4), 16) % 254 + 1}"


def _gen_mac(real: str) -> str:
    if real.lower() == NULL_MAC:
        return real     # keep null MAC so clean_mac filtering still triggers
    h = _h(real, 6)
    return f"02:00:00:{h[0:2]}:{h[2:4]}:{h[4:6]}".lower()


def _gen_serial(real: str) -> str:
    """Preserve length and char-class shape (digit/upper letter)."""
    h = _h(real, 64).upper()
    out = []
    for i, c in enumerate(real):
        if c.isdigit():
            pick = h[(i * 2) % len(h)]
            out.append(pick if pick.isdigit() else str((i * 7) % 10))
        elif c.isalpha():
            pick = h[(i * 2) % len(h)]
            out.append(pick if pick.isalpha() else chr(ord("A") + (i * 5) % 26))
        else:
            out.append(c)
    return "".join(out)


def _gen_host(real: str) -> str:
    n = int(_h(real, 4), 16) % 1000
    return f"device-{n:03d}.example.com" if "." in real else f"device-{n:03d}"


def _gen_location(real: str) -> str:
    n = int(_h(real, 3), 16) % 100
    return f"Site-{n:02d}"


def _gen_engineid(real: str) -> str:
    return _h(real, 40)


def _gen_chassis_id(real: str) -> str:
    return _h(real, 32)


def _make_mapper(gen):
    cache: dict[str, str] = {}

    def m(real):
        if not real:
            return real
        if real not in cache:
            cache[real] = gen(real)
        return cache[real]
    m.cache = cache  # type: ignore[attr-defined]
    return m


map_ip          = _make_mapper(_gen_ip)
map_mac         = _make_mapper(_gen_mac)
map_serial      = _make_mapper(_gen_serial)
map_host        = _make_mapper(_gen_host)
map_location    = _make_mapper(_gen_location)
map_engineid    = _make_mapper(_gen_engineid)
map_chassis_id  = _make_mapper(_gen_chassis_id)


# ── description / free-text scrubbing ──────────────────────────────────────────

# Substrings of free-text that pass through. Anything else is rewritten by
# substituting any registered real value with its fake, plus a regex sweep
# for IP-shaped tokens (catch-all for anything not encountered as a structured
# field).
DESCR_WHITELIST = (
    "Ethernet Port", "Virtual Domain", "Vitual Domain",   # last is Netdisco's typo
    "Fortinet", "FortiGate", "FortiProxy", "Cisco", "Catalyst", "Nexus",
    "HW Serial#:", "Power Supply", "Supervisor", "Fabric", "Module",
)


def scrub_text(text: str) -> str:
    if not text:
        return text
    # Substitute any registered real value with its fake. Sort by length desc
    # so longer matches win over substring overlap.
    for cache in (map_serial.cache, map_host.cache, map_location.cache):  # type: ignore[attr-defined]
        for real, fake in sorted(cache.items(), key=lambda kv: -len(kv[0])):
            if real:
                text = text.replace(real, fake)
    # Catch-all: rewrite any remaining IP-shaped token.
    text = IP_RE.sub(lambda m: map_ip(m.group(0)), text)
    return text


# ── shape transformers ─────────────────────────────────────────────────────────

def transform_device(d: dict) -> dict:
    out = dict(d)
    if out.get("ip"):           out["ip"] = map_ip(out["ip"])
    if out.get("mac"):          out["mac"] = map_mac(out["mac"])
    if out.get("serial"):       out["serial"] = map_serial(out["serial"])
    if out.get("dns"):          out["dns"] = map_host(out["dns"])
    if out.get("name"):         out["name"] = map_host(out["name"]).split(".")[0]
    if out.get("location"):     out["location"] = map_location(out["location"])
    if out.get("chassis_id"):   out["chassis_id"] = map_chassis_id(out["chassis_id"])
    if out.get("snmp_engineid"): out["snmp_engineid"] = map_engineid(out["snmp_engineid"])
    if "snmp_comm" in out:      out["snmp_comm"] = None
    if out.get("contact"):      out["contact"] = "noc@example.com"
    if out.get("description"):  out["description"] = scrub_text(out["description"])
    return out


def transform_module(m: dict) -> dict:
    out = dict(m)
    if out.get("ip"):           out["ip"] = map_ip(out["ip"])
    if out.get("serial"):       out["serial"] = map_serial(out["serial"])
    if out.get("description"):  out["description"] = scrub_text(out["description"])
    return out


def transform_port(p: dict) -> dict:
    out = dict(p)
    if out.get("ip"):  out["ip"] = map_ip(out["ip"])
    if out.get("mac"): out["mac"] = map_mac(out["mac"])
    # ifAlias (`name`): if it differs from the port id, replace with iface-N.
    # (Equality with port id is just the trivial label, safe to keep.)
    port_id = (out.get("port") or "").lower()
    name = out.get("name")
    if name and isinstance(name, str) and name.lower() != port_id:
        n = int(_h(name, 4), 16) % 1000
        out["name"] = f"iface-{n:03d}"
    if out.get("descr"): out["descr"] = scrub_text(out["descr"])
    return out


def transform_powered_port(p: dict) -> dict:
    out = dict(p)
    if out.get("ip"): out["ip"] = map_ip(out["ip"])
    return out


# ── driver ─────────────────────────────────────────────────────────────────────

def detect_shape(out_name: str) -> str:
    if out_name.endswith("-device.json"):         return "device"
    if out_name.endswith("-modules.json"):        return "modules"
    if out_name.endswith("-ports.json"):          return "ports"
    if out_name.endswith("-powered_ports.json"):  return "powered_ports"
    return "unknown"


TRANSFORMERS = {
    "device":         transform_device,
    "modules":        transform_module,
    "ports":          transform_port,
    "powered_ports":  transform_powered_port,
}


def main() -> int:
    if not SRC.exists():
        print(f"ERROR: {SRC} not found", file=sys.stderr)
        return 1
    DST.mkdir(parents=True, exist_ok=True)
    for src in sorted(SRC.glob("*.json")):
        if src.name in SKIP:
            print(f"  SKIP   {src.name}  (search-shape, dropped)")
            continue
        out_name = RENAME.get(src.name, src.name)
        shape = detect_shape(out_name)
        if shape == "unknown":
            print(f"  WARN   {src.name} → {out_name}  (unknown shape, skipping)")
            continue
        with src.open() as f:
            data = json.load(f)
        transformer = TRANSFORMERS[shape]
        anon = [transformer(e) for e in data] if isinstance(data, list) else transformer(data)
        with (DST / out_name).open("w") as f:
            json.dump(anon, f, indent=2)
        n = len(anon) if isinstance(anon, list) else 1
        print(f"  WROTE  tests/samples/{out_name}  ({shape}, n={n})")
    print(f"  done — {len(map_ip.cache)} IPs, {len(map_mac.cache)} MACs, "  # type: ignore[attr-defined]
          f"{len(map_serial.cache)} serials, {len(map_host.cache)} hostnames, "  # type: ignore[attr-defined]
          f"{len(map_location.cache)} locations")  # type: ignore[attr-defined]
    return 0


if __name__ == "__main__":
    sys.exit(main())
