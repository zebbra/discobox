"""Small helpers and shared constants: parsing, names, MACs, Netbox write retries, device locks."""

import ipaddress
import random
import re
import threading
import time
from typing import Callable, Optional

import pynetbox
import requests
import urllib3

from .log import logger

# ── Interface type mapping ─────────────────────────────────────────────────────

def map_iftype(nd_type: Optional[str], iface_name: Optional[str], default: Optional[str] = "other") -> Optional[str]:
    """
    Map a Netdisco interface to a Netbox type slug.

    Name-prefix matching is tried first because Netdisco reports nearly every
    physical port as ifType=ethernetCsmacd regardless of speed: the interface
    name (e.g. "TenGigabitEthernet0/1") carries the real type information.
    SNMP ifType is used as a fallback for LAG and virtual interfaces.

    When Netdisco reports no ifType at all (e.g. FortiGate devices with null
    types on every port) and no name rule matches, `default` is returned —
    pass default=None to express "no opinion" so the caller can preserve
    whatever type Netbox already has.

    Adapted from github.com/joeladria/netdisco-netbox-diode.
    """
    lname = (iface_name or "").lower()
    nd_type_l = (nd_type or "").lower()

    # Dot-notation subinterfaces (e.g. "TwentyFiveGigE1/1/8.2802", "Po1.100") are
    # always virtual: Netbox only allows a `parent` on interfaces of type virtual,
    # and the name-prefix rules below would otherwise assign the parent's physical type.
    if re.search(r"\.\d+$", lname):                                 return "virtual"

    # LAG / virtual via ifType: these are reliable regardless of name
    if "lag" in nd_type_l:                                          return "lag"
    if nd_type_l in ("softwareloopback", "propvirtual", "l2vlan",
                     "l3ipvlan"):                                   return "virtual"
    if nd_type_l == "tunnel":                                       return "virtual"

    # Name-prefix matches
    if lname.startswith(("loopback", "lo")):                        return "virtual"
    if lname.startswith("vlan"):                                     return "virtual"
    if lname.startswith(("tunnel", "tun", "gr", "ipip", "null")):   return "virtual"
    if lname.startswith(("port-channel", "bundle-ether", "be",
                          "etherchannel", "eth-trunk", "ae")):      return "lag"
    if re.match(r"po\d", lname):                                    return "lag"
    if lname.startswith(("management", "mgmt", "fxp", "em")):       return "1000base-t"
    if lname.startswith(("hundredgig", "hu", "ce", "et")):          return "100gbase-x-qsfp28"
    if lname.startswith("fiftygig"):                                 return "50gbase-x-sfp28"
    if lname.startswith(("fortygig", "fo")):                        return "40gbase-x-qsfpp"
    if lname.startswith(("twentyfive", "tf")):                      return "25gbase-x-sfp28"
    if lname.startswith(("tengig", "te", "xe")):                    return "10gbase-x-sfpp"
    if lname.startswith(("fivegig", "fg")):                         return "5gbase-t"
    if lname.startswith(("2.5gig", "twoandahalf", "tg")):           return "2.5gbase-t"
    if lname.startswith(("gigabit", "gi")):                         return "1000base-t"
    if lname.startswith(("fastethernet", "fa")):                    return "100base-tx"
    if lname.startswith(("ethernet", "eth")):                       return "1000base-t"
    if lname.startswith(("vmbr", "br")):                            return "bridge"
    if lname.startswith(("serial", "se")):                          return "other"

    # ifType fallback
    if "virtual" in nd_type_l:                                      return "virtual"
    if "ethernet" in nd_type_l:                                     return "1000base-t"

    if not nd_type_l:
        return default
    return "other"


_COPPER_IFACE_TYPES = {"100base-tx", "1000base-t", "2.5gbase-t", "5gbase-t", "10gbase-t"}

def cable_type_from_iface_type(iface_type: str) -> str:
    return "cat5e" if iface_type in _COPPER_IFACE_TYPES else "smf"


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_os_release(description: Optional[str]) -> Optional[str]:
    """
    Extract the IOS release name from the Netdisco device description.

    Example: "Cisco IOS Software [Gibraltar], Catalyst ..."  →  "Gibraltar"
    """
    if not description:
        return None
    m = re.search(r"\[([A-Za-z0-9]+)\]", description)
    return m.group(1) if m else None


def parse_sw_ver(sw_ver: Optional[str]) -> Optional[str]:
    """
    Extract a plain version number from a chassis sw_ver string.

    Examples:
      "FortiGate-600F v7.4.8,build2795,250523 (GA.M)"  →  "7.4.8"
      "Version 17.3.4"                                  →  "17.3.4"
    """
    if not sw_ver:
        return None
    m = re.search(r"v?(\d+\.\d+[\.\d]*)", sw_ver)
    return m.group(1) if m else None


def parse_sw_model(sw_ver: Optional[str]) -> Optional[str]:
    """
    Extract the product model name from a sw_ver string (the word before the version).

    Example: "FortiGate-600F v7.4.8,build2795,250523 (GA.M)"  →  "FortiGate-600F"
    """
    if not sw_ver:
        return None
    m = re.match(r"(\S+)\s+v\d+", sw_ver)
    return m.group(1) if m else None


def parse_speed_kbps(speed_str: Optional[str]) -> Optional[int]:
    """
    Parse Netdisco speed string to kbps for Netbox.

    Examples: "10 Mbps" → 10_000, "1 Gbps" → 1_000_000, "auto" → None
    """
    if not speed_str:
        return None
    m = re.match(r"([\d.]+)\s*(kbps|mbps|gbps)", speed_str.strip(), re.IGNORECASE)
    if not m:
        return None
    value, unit = float(m.group(1)), m.group(2).lower()
    multipliers = {"kbps": 1, "mbps": 1_000, "gbps": 1_000_000}
    return int(value * multipliers[unit])



# Raw OID fragments Netdisco reports as a model when it has no matching MIB:
# "enterprises.2440", ".1570", ".107.1.50001", "1.3.6.1.4.1.9"
_OID_MODEL_RE = re.compile(r"^(?:enterprises)?(?:\.\d+)+$|^\d+(?:\.\d+)+$", re.IGNORECASE)


def is_oid_model(value: Optional[str]) -> bool:
    """True when value is an OID fragment rather than a real model name."""
    return bool(value) and bool(_OID_MODEL_RE.match(value.strip()))


def _norm_vendor(value: Optional[str]) -> str:
    """Vendor name → comparison key: "Efficient IP", "efficientip" and "EfficientIP" are equal."""
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


TypeAlias = tuple[Optional[str], Callable[[str], bool], str]   # (vendor key or None, matcher, target)


def _compile_type_aliases(aliases: Optional[dict]) -> list[TypeAlias]:
    """
    Config alias map → [(vendor, matcher, target)], vendor-scoped entries first.

    Keys match a raw SNMP model exactly (case-insensitive), or as a regex when
    written /like this/. Targets name a Netbox part_number, model or slug.
    A mapping as value scopes its entries to that vendor (Netdisco vendor or
    Netbox manufacturer name): OID fragments like ".1570" are only unique
    within one vendor's enterprise tree.
    """
    def _matcher(key: str) -> Callable[[str], bool]:
        if len(key) > 2 and key.startswith("/") and key.endswith("/"):
            rx = re.compile(key[1:-1], re.IGNORECASE)
            return lambda s: bool(rx.search(s))
        k = key.lower()
        return lambda s: s.strip().lower() == k

    scoped: list[TypeAlias] = []
    unscoped: list[TypeAlias] = []
    def _add(vendor: Optional[str], key: object, target: object) -> None:
        if not isinstance(key, str):
            # unquoted YAML like `.1570: X` loads as the float 0.157
            logger.warning("Type alias key %r is not a string (quote it in YAML): ignored", key)
            return
        key, target = key.strip(), str(target or "").strip()
        if key and target:
            (scoped if vendor else unscoped).append((vendor, _matcher(key), target))

    for key, value in (aliases or {}).items():
        if isinstance(value, dict):
            for sub_key, target in value.items():
                _add(_norm_vendor(str(key)), sub_key, target)
        else:
            _add(None, key, value)
    return scoped + unscoped


def _split_aliases(value: object) -> list[str]:
    """Alias custom field value → list: accepts a list, or text separated by commas/newlines."""
    if not value:
        return []
    items = value if isinstance(value, (list, tuple)) else re.split(r"[,\n]", str(value))
    return [str(i).strip() for i in items if str(i).strip()]


def slugify(value: str) -> str:
    """Convert a string to a Netbox-compatible slug (lowercase, alphanum + hyphens)."""
    s = value.lower()
    s = re.sub(r"[^a-z0-9_-]", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")


def validate_ip(value: str) -> str:
    """Validate that value is a valid IP address. Raises ValueError if invalid."""
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        raise ValueError(f"not a valid IP address: {value!r}")


# ── Netbox client ──────────────────────────────────────────────────────────────

READ_RETRIES = 3
READ_RETRY_BACKOFF_S = 2.0      # pauses before read retries: 2, 4, 8 s (+ up to 1 s jitter)
READ_RETRY_BACKOFF_MAX_S = 30.0


class _ReadRetry(urllib3.util.Retry):
    """
    Retry whose first retry already waits: urllib3 retries the first failure
    immediately (0, 1, 2 s with factor 0.5), which gives an overloaded server
    no time to recover. Here: READ_RETRY_BACKOFF_S * 2^(n-1), capped, plus
    jitter so parallel syncs don't come back in lockstep.
    """
    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        new = super().increment(method, url, response, error, _pool, _stacktrace)
        # urllib3 logs a retry after a status code (500/502/503) only at DEBUG,
        # and one after a connection error without the pause: one WARNING for both
        if response is not None and error is None and not new.is_exhausted():
            logger.warning(
                "Retrying %s %s after HTTP %s (retry %d of %d, in ~%.0fs)",
                method, url, response.status, len(new.history), READ_RETRIES, new.get_backoff_time(),
            )
        return new

    def get_backoff_time(self) -> float:
        errors = len([h for h in self.history if h is not None])
        if errors == 0:
            return 0.0
        base = min(READ_RETRY_BACKOFF_MAX_S, self.backoff_factor * (2 ** (errors - 1)))
        return base + random.uniform(0, self.backoff_jitter)


def _mount_read_retries(session: requests.Session) -> None:
    """
    Retry idempotent reads (GET/HEAD) on a dropped connection, e.g. a server or
    proxy closing an idle keep-alive connection mid-run ("RemoteDisconnected"),
    and on a transient server error (500, 502, 503). Not on 504: a gateway
    timeout usually repeats (the request is too slow for the proxy), so a retry
    only multiplies the load. Writes are never retried: a POST/PATCH/DELETE
    may have been applied already. After the last attempt the response is
    returned as-is, so callers see the same error as before.
    """
    retry = _ReadRetry(
        total=READ_RETRIES, connect=READ_RETRIES, read=READ_RETRIES, status=READ_RETRIES, other=0,
        allowed_methods=frozenset({"GET", "HEAD"}),
        status_forcelist=(500, 502, 503),
        backoff_factor=READ_RETRY_BACKOFF_S, backoff_jitter=1.0, raise_on_status=False,
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)


# ── Field mapping ──────────────────────────────────────────────────────────────

# Device bay name patterns (regex) auto-created from DeviceType templates that
# should be removed when the device is managed via module bays / inventory items.
STALE_DEVICE_BAY_PATTERNS: list[re.Pattern] = [re.compile(p, re.IGNORECASE) for p in [
    r"^Network Module$",
    r"^PS-[AB]$",
    r"^PS\s*\d+$",
    r"^PSU\s*\d+$",
    r"^Fan\s*\d+$",
    r"^Slot\s*\d+$",
]]

# Dummy/placeholder interface names used as IP anchors before a proper sync.
# IPs found on these interfaces will be moved to the correct interface.
# In housekeeping, empty dummy interfaces are deleted.
DUMMY_INTERFACES: set[str] = {
    "main", "mgmt", "mgmt0",
}

# Interface name prefixes startwith (case-insensitive) to skip during port sync.
PORT_BLACKLIST_PREFIXES: tuple[str, ...] = (
    "null",
    "modem",
    "bluetooth",
    "ssl.",
    "appgigabitethernet",
    "eobc",
    "sdwan",
)

def vendor_from_chassis(chassis: dict) -> Optional[str]:
    """
    Extract a vendor/manufacturer name from a Netdisco chassis entry.

    Returns None for Cisco devices (caller falls back to device's existing
    manufacturer). For other vendors the name is encoded in the type string:
      "fortinet.6007.6007.0"  →  "Fortinet"

    Cisco ENTITY-MIB OID names all start with "cev" (e.g. cevChassisN9KC93600CDGX)
    and never contain a leading vendor prefix.
    """
    type_str = chassis.get("type") or ""
    # Cisco types: let caller use the device's existing Netbox manufacturer
    if type_str.lower().startswith("cev"):
        return None
    # Other vendors encode their name before the first dot
    if "." in type_str:
        vendor = type_str.split(".")[0]
        if vendor:
            return vendor.capitalize()
    return None


def _cf_record_id(value) -> Optional[int]:
    """Id of an object-type custom field value (dict, Record, or bare id)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("id")
    return getattr(value, "id", value)


NULL_MAC = "00:00:00:00:00:00"
NO_POE_IFACE_TYPES = frozenset({"virtual", "bridge", "lag"})   # Netbox: no poe_mode allowed
CREATED_BY_COMMENT = "Created by discobox"   # comments of the bare types discobox mints


DEADLOCK_RETRIES = (0.5, 1.0, 2.0)   # back-off seconds between attempts


def _is_deadlock(exc: Exception) -> bool:
    return "deadlock detected" in str(exc).lower()


def _delete_with_retry(obj, what: str, log=None) -> bool:
    """
    obj.delete(), retried when Netbox's Postgres aborts it as a deadlock (two
    writers on the same rows, e.g. the device's interface counter). Any other
    or a persistent failure is logged and returns False instead of raising, so
    one failed delete never aborts a whole sync or rebuild.
    """
    log = log or logger
    for attempt, pause in enumerate((0.0,) + DEADLOCK_RETRIES):
        if pause:
            time.sleep(pause)
        try:
            obj.delete()
            return True
        except Exception as exc:
            if _is_deadlock(exc) and attempt < len(DEADLOCK_RETRIES):
                log.warning("  %s: deadlock, retrying (%d/%d)", what, attempt + 1, len(DEADLOCK_RETRIES))
                continue
            log.error("  %s: delete failed: %s", what, exc)
            return False
    return False


# One sync/rebuild at a time per Netbox device: the same device can arrive via
# several IPs (an HA pair's WMI and RMIs, a cluster VIP), so the server's
# per-IP in-flight dedup doesn't serialise them, and concurrent writers on one
# device deadlock in Netbox's Postgres.
_DEVICE_LOCKS: dict[int, threading.Lock] = {}
_DEVICE_LOCKS_GUARD = threading.Lock()
DEVICE_LOCK_WAIT_S = 900


def _device_lock(device_id: int) -> threading.Lock:
    with _DEVICE_LOCKS_GUARD:
        return _DEVICE_LOCKS.setdefault(device_id, threading.Lock())


_CISCO_ABBREV = [
    ("HundredGigabitEthernet", "HundredGigE"),
    ("HundredGigabitEthernet", "Hu"),
    ("FortyGigabitEthernet",  "Fo"),
    ("TwentyFiveGigE",        "Twe"),
    ("TenGigabitEthernet",    "Te"),
    ("GigabitEthernet",       "Gi"),
    ("FastEthernet",          "Fa"),
    ("Ethernet",              "Et"),
    ("Management",            "Mg"),
    ("Loopback",              "Lo"),
    ("Vlan",                  "Vl"),
    ("Port-channel",          "Po"),
    ("Tunnel",                "Tu"),
    ("Serial",                "Se"),
    ("AppGigabitEthernet",    "Ap"),
]

def expand_iface_name(name: str) -> str:
    """Expand a Cisco abbreviated interface name to its full form.

    E.g. 'Gi3/0/1' → 'GigabitEthernet3/0/1', 'Te1/1' → 'TenGigabitEthernet1/1'.
    Returns the original string if no prefix matches.
    """
    for full, abbrev in _CISCO_ABBREV:
        if name.startswith(abbrev) and not name.startswith(full):
            return full + name[len(abbrev):]
    return name


def _slave_link_field(iface_name: str, nd_type: Optional[str], nb_type: Optional[str]) -> str:
    """Pick the Netbox field for a slave_of link.

    Virtual children (VLAN subinterfaces stacked on a LAG) attach via `parent`;
    physical members via `lag`. When Netdisco reports no ifType (e.g. FortiGate
    with null types on every port), trust the existing Netbox type — otherwise a
    virtual child would be mis-wired as a LAG member and rejected with 400.
    """
    if "." in iface_name:
        return "parent"
    if map_iftype(nd_type, iface_name, default=None) == "virtual":
        return "parent"
    if nb_type == "virtual":
        return "parent"
    return "lag"


def _reraise_if_gateway_error(exc: requests.HTTPError) -> None:
    """Re-raise 502/503/504 so server.py's retry/circuit-breaker logic can fire."""
    resp = getattr(exc, "response", None)
    if resp is not None and resp.status_code in (502, 503, 504):
        raise exc


def _slot_from_iface(topo: str, name: str) -> Optional[int]:
    """
    Extract module slot key from interface name for stack/fex topologies.

    Stack:  GigabitEthernet2/0/1  → 2   (first number = stack member, 1-indexed)
    FEX:    Ethernet101/1/1        → 101 (first number ≥ 100 = FEX ID)
    """
    m = re.match(r"[A-Za-z]+(\d+)/\d+/\d+", name)
    if not m:
        return None
    slot = int(m.group(1))
    if topo == "fex":
        return slot if slot >= 100 else None
    if topo in ("stack", "vss"):
        return slot
    return None


def clean_mac(raw: Optional[str]) -> Optional[str]:
    """Return uppercased MAC or None if missing, zero, or otherwise invalid."""
    if not raw:
        return None
    mac = raw.upper().replace("-", ":")
    if mac == NULL_MAC.upper():
        return None
    return mac

_MAC_RE = re.compile(r"^(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}$", re.IGNORECASE)


# Netbox custom fields on Device that discobox reads for the discover jobs it
# enqueues: the Netdisco device_auth tag hint and the SNMP polling budget
# (custom_fields.auth_profile / snmp_timeout; None = not used).
READ_CF: dict[str, Optional[str]] = {"auth_profile": "snmp_auth_profile", "snmp_timeout": "snmp_polling_timeout"}


def configure_read_fields(**names: Optional[str]) -> None:
    READ_CF.update(names)


def read_cf(cf: dict, key: str) -> Optional[str]:
    """Value of the READ_CF field key in a device's custom_fields, None if unset or not configured."""
    name = READ_CF.get(key)
    return (cf.get(name) or None) if name else None


_TIMEOUT_UNITS = {"us": 1, "ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000}

# Per-request SNMP timeout for the discover jobs discobox enqueues ("snmptimeout",
# µs; Netdisco hands it to net-snmp unmodified as the timeout of EVERY request).
# The Netbox CF snmp_polling_timeout is NOT a per-request value: modulator writes
# the whole-device Prometheus scrape budget there (sum of module scrape durations
# × 1.5, rounded to a choice step, floor 2m). Using it as-is would give 2-5 minutes
# per SNMP PDU. So the per-request value is derived from it and bounded:
#   clamp(budget / divisor, floor, cap)   e.g. "2m" → 10s, "5m" → 20s, "20s" → 10s
# and the floor applies when the CF is empty or unparsable. Configurable via
# netdisco.discover_snmptimeout {divisor, floor, cap} (see configure_discover_snmptimeout).
DISCOVER_SNMPTIMEOUT = {"divisor": 12, "floor_us": 10_000_000, "cap_us": 20_000_000}


def configure_discover_snmptimeout(divisor=None, floor=None, cap=None) -> None:
    """Override the per-request policy (floor/cap as durations like "10s", or µs ints)."""
    def _us(v):
        return v if isinstance(v, int) else _parse_snmp_timeout_us(str(v))
    if divisor:
        DISCOVER_SNMPTIMEOUT["divisor"] = int(divisor)
    if floor is not None and _us(floor):
        DISCOVER_SNMPTIMEOUT["floor_us"] = _us(floor)
    if cap is not None and _us(cap):
        DISCOVER_SNMPTIMEOUT["cap_us"] = _us(cap)


def discover_snmptimeout_us(budget: Optional[str]) -> int:
    """Per-request snmptimeout (µs) derived from a snmp_polling_timeout budget (see DISCOVER_SNMPTIMEOUT)."""
    p = DISCOVER_SNMPTIMEOUT
    parsed = _parse_snmp_timeout_us(budget) if budget else None
    if parsed is None:
        return p["floor_us"]
    return max(p["floor_us"], min(p["cap_us"], parsed // p["divisor"]))


def _parse_snmp_timeout_us(value: Optional[str]) -> Optional[int]:
    """Parse a Netbox snmp_polling_timeout string (e.g. '3m', '30s') to microseconds."""
    if not value:
        return None
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(us|ms|s|m|h)?", value.strip(), re.IGNORECASE)
    if not m:
        return None
    amount, unit = float(m.group(1)), (m.group(2) or "s").lower()
    return int(amount * _TIMEOUT_UNITS[unit])


def _merged_custom_fields(device: pynetbox.core.response.Record, updates: dict) -> dict:
    """
    Merge `updates` into `device`'s current custom_fields and return the full dict.

    pynetbox's Record.update() does a plain setattr for custom_fields — no
    merge. Netbox's API merges custom_fields correctly server-side regardless
    of what's sent, but passing only the changed keys permanently replaces
    the LOCAL object's .custom_fields attribute with just those keys for the
    rest of this record's lifetime: any later read of a different custom
    field on the same object (e.g. reading stack_members after an earlier
    os_version write) would see it as missing/None even though Netbox still
    has it. Always pass the full merged dict to device.update() instead of a
    bare {key: value} to keep the local object's custom_fields accurate.
    """
    merged = dict(getattr(device, "custom_fields", {}) or {})
    merged.update(updates)
    return merged
