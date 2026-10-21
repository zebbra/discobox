"""Wireless: access points reported by a WLC (pass, interfaces, bays, notes, uplinks)."""

import hashlib
import json
import os
import re
from typing import Optional
from urllib.parse import quote

from ._version import __version__
from .log import logger
from .util import _delete_with_retry

_AP_RADIO_PORT_RE = re.compile(r"^(?:[0-9a-f]{2}:){5}[0-9a-f]{2}\.\d+$", re.IGNORECASE)


def _is_ap_port(port: dict) -> bool:
    """
    True for a WLC's per-AP virtual radio "port" (type dot11a/b/g/n/ac/ax/...).

    These aren't real interfaces on the controller — Netdisco surfaces one per
    associated access point — so they must never be synced as the WLC's own
    interfaces (name collides with the AP's hostname, port key is a bare MAC).
    Some WLCs don't report a dot11 type, so the "<radio MAC>.<slot>" port name
    (e.g. "02:00:00:aa:bb:cc.0") counts too.
    """
    if (port.get("type") or "").lower().startswith("dot11"):
        return True
    return bool(_AP_RADIO_PORT_RE.match(port.get("port") or ""))


_AP_DESC_HEAD_RE = re.compile(r"^[^:]+:\s*(\S+)\s*\(([^)]*)\)\s*$")
_AP_DESC_FIELD_RES = {
    "ip": re.compile(r"^IP\s+(\S+)$", re.IGNORECASE),
    "dot3_mac": re.compile(r"^Dot3 MAC\s+([0-9a-fA-F:]+)$", re.IGNORECASE),
    "ethernet_mac": re.compile(r"^Ethernet MAC\s+([0-9a-fA-F:]+)$", re.IGNORECASE),
}
# "Connected via <switch>[ <port>][ (<ip>)]" (the port since the C9800 tag-aware SNMP::Info)
_AP_DESC_UPLINK_RE = re.compile(r"^Connected via\s+(\S+)(?:\s+([^\s(]+))?(?:\s*\(([^)]*)\))?$", re.IGNORECASE)
# C9800 AP tags, one of each per AP: "Policy tag <x>; Site tag <y>; RF tag <z>" (each optional,
# "default-*-tag" = no explicit assignment, kept as reported)
_AP_DESC_TAG_RE = re.compile(r"^(Policy|Site|RF) tag\s+(.+?)\s*$", re.IGNORECASE)
AP_TAG_KINDS = (("policy", "Policy"), ("site", "Site"), ("rf", "RF"))


def _parse_ap_description(description: str) -> dict:
    """
    Best-effort parse of a Netdisco ap-class module description, e.g.:
      "CW9166I-E: SITE1-W216 (C2/SITE1); IP 192.0.2.10; Dot3 MAC ...;
       Ethernet MAC ...; Connected via SWITCH.example.com[ Gi1/0/12][ (192.0.2.1)];
       Policy tag PT-X; Site tag ST-Y; RF tag RF-Z"

    The "(<site tag>/<tag>)" part is split into site_tag/tag (site_tag alone
    when there's no "/"); the uplink port and IP are optional. The C9800
    Policy/Site/RF tags (each optional, any order) go to tag_policy, tag_site,
    tag_rf.

    Returns {} if even the hostname can't be recovered — any other field
    missing from the text is simply absent from the result.
    """
    parts = [p.strip() for p in (description or "").split(";") if p.strip()]
    if not parts:
        return {}
    head = _AP_DESC_HEAD_RE.match(parts[0])
    if not head:
        return {}
    site_tag, _, tag = head.group(2).partition("/")
    result: dict = {"hostname": head.group(1), "site_tag": site_tag.strip()}
    if tag.strip():
        result["tag"] = tag.strip()
    for part in parts[1:]:
        uplink = _AP_DESC_UPLINK_RE.match(part)
        if uplink:
            result["uplink_name"] = uplink.group(1)
            if uplink.group(2):
                result["uplink_port"] = uplink.group(2)
            if uplink.group(3):
                result["uplink_ip"] = uplink.group(3)
            continue
        tag = _AP_DESC_TAG_RE.match(part)
        if tag:
            result[f"tag_{tag.group(1).lower()}"] = tag.group(2)
            continue
        for key, rx in _AP_DESC_FIELD_RES.items():
            m = rx.match(part)
            if m:
                result[key] = m.group(1)
                break
    return result


# Wired uplink as Cisco APs report it via CDP/LLDP, so the switch-side
# neighbor pass can cable to it.
AP_WIRED_IFACE = "GigabitEthernet0"
AP_NOTE_BEGIN = "<!-- discobox:ap -->"
AP_NOTE_END = "<!-- /discobox:ap -->"
_AP_NOTE_RE = re.compile(re.escape(AP_NOTE_BEGIN) + r".*?" + re.escape(AP_NOTE_END), re.DOTALL)
_AP_RADIO_BANDS = {"dot11b": "2.4 GHz", "dot11g": "2.4 GHz", "dot11a": "5 GHz"}


_AP_IFACE_RE = re.compile(r"^(?:gigabitethernet0|dot11radio\d+)$", re.IGNORECASE)


def _is_ap_managed_iface(name: str) -> bool:
    """The only interfaces an AP has in Netbox: its wired uplink and its radios."""
    return bool(_AP_IFACE_RE.match(name or ""))


def _prune_ap_interfaces(nb, ap_dev, ifaces: dict, source_cf: Optional[str], source_value: str, log) -> int:
    """
    Delete an AP's interfaces other than GigabitEthernet0 / Dot11Radio<n>:
    import placeholders (main, Vlan2) and ports from a wrong DeviceType
    template (e.g. GigabitEthernet1/0/x). Never one that carries a cable or
    an IP, or that another source owns. Every Dot11Radio<n> is kept, even
    when the WLC currently reports fewer radios. Returns the number deleted.
    """
    removed: list[str] = []
    for name, iface in ifaces.items():
        if _is_ap_managed_iface(name) or getattr(iface, "cable", None):
            continue
        if source_cf:
            owner = dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) or ""
            if owner and owner != source_value:
                continue
        if list(nb.nb.ipam.ip_addresses.filter(assigned_object_type="dcim.interface", assigned_object_id=iface.id)):
            log.debug("  AP %s: %s has IP(s), kept", ap_dev.name, name)
            continue
        if _delete_with_retry(iface, f"AP {ap_dev.name}: stale interface {name}", log):
            removed.append(name)
    if removed:
        log.info("  AP %s: removed %d stale interface(s)", ap_dev.name, len(removed))
        log.debug("  AP %s: removed %s", ap_dev.name, ", ".join(sorted(removed)))
    return len(removed)


def _tags_without(tags, slugs: set[str]) -> Optional[list[int]]:
    """Tag ids of tags minus those whose slug is in slugs, or None when none of them is present."""
    tags = list(tags or [])
    keep = [t for t in tags if (getattr(t, "slug", None) or "").lower() not in slugs]
    return None if len(keep) == len(tags) else [t.id for t in keep]


def _remove_wlc_radio_ports(nb, device, source_cf: Optional[str], source_value: str, log) -> int:
    """
    Delete a WLC's "<radio MAC>.<slot>" interfaces: per-AP radio pseudo-ports
    that older discobox versions synced as the controller's own interfaces
    (they're filtered out now, so only housekeeping would ever orphan-delete
    them). Never one with a cable, an IP, or another source's ownership.
    """
    removed = 0
    for iface in nb.nb.dcim.interfaces.filter(device_id=device.id):
        if not _AP_RADIO_PORT_RE.match(iface.name or "") or getattr(iface, "cable", None):
            continue
        owner = dict(getattr(iface, "custom_fields", {}) or {}).get(source_cf) if source_cf else ""
        if owner and owner != source_value:
            continue
        if list(nb.nb.ipam.ip_addresses.filter(assigned_object_type="dcim.interface", assigned_object_id=iface.id)):
            continue
        if _delete_with_retry(iface, f"AP radio pseudo-port {iface.name}", log):
            removed += 1
    if removed:
        log.info("  Removed %d AP radio pseudo-port(s) from the controller (legacy)", removed)
    return removed


# WLC AP pass skip: the AP pass costs a few Netbox reads per AP (hundreds per
# WLC) on every sync. When everything it reads from Netdisco is unchanged
# since the last pass, the APs are skipped, except those still unresolved
# (not found / failed) last time. A full pass still runs every
# AP_FULL_PASS_DAYS (repairs hand edits in Netbox) and on every rebuild.
# AP_STATE_DIR None = off (always a full pass).
AP_STATE_DIR: Optional[str] = None
AP_FULL_PASS_DAYS: float = 7
_AP_MODULE_KEYS = ("name", "description", "model", "serial", "sw_ver", "hw_ver", "fw_ver", "vendor")
_AP_PORT_KEYS = ("port", "mac", "type")


def configure_ap_fingerprint(state_dir: Optional[str], full_pass_days: Optional[float] = None) -> None:
    global AP_STATE_DIR, AP_FULL_PASS_DAYS
    AP_STATE_DIR = state_dir or None
    if full_pass_days is not None:
        AP_FULL_PASS_DAYS = float(full_pass_days)


def _ap_fingerprint(ap_modules: list[dict], ap_radio_ports: list[dict], context: list) -> str:
    """Hash of everything the AP pass reads from Netdisco, plus the settings/WLC fields it writes from."""
    data = [
        __version__, context,
        sorted([str(m.get(k) or "") for k in _AP_MODULE_KEYS] for m in ap_modules),
        sorted([str(p.get(k) or "") for k in _AP_PORT_KEYS] for p in ap_radio_ports),
    ]
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()[:32]


def _ap_state_path(device_id: int) -> Optional[str]:
    return os.path.join(AP_STATE_DIR, f"discobox.ap_pass.{device_id}.json") if AP_STATE_DIR else None


def _load_ap_state(device_id: int) -> dict:
    path = _ap_state_path(device_id)
    if not path:
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_ap_state(device_id: int, state: dict) -> None:
    path = _ap_state_path(device_id)
    if not path:
        return
    try:
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning("Could not save the AP pass state: %s", exc)


def _plan_ap_pass(state: dict, fingerprint: str, full: bool, now: float) -> tuple[bool, set]:
    """(full pass?, hostnames to retry on a skipped pass)."""
    if full or not AP_STATE_DIR or state.get("fingerprint") != fingerprint:
        return True, set()
    if now - float(state.get("full_at") or 0) >= AP_FULL_PASS_DAYS * 86400:
        return True, set()
    return False, set(state.get("pending") or [])


def _prune_ap_bays(nb, ap_dev, log) -> int:
    """
    Delete an AP's empty device bays and module bays: an AP has neither, so
    they can only come from a wrong DeviceType template. A bay with something
    installed stays. Uses the device's own bay counters to skip the queries
    for the (usual) AP without any bays. Returns the number deleted.
    """
    removed = 0
    for counter, endpoint, installed_attr in (
        ("device_bay_count", "device_bays", "installed_device"),
        ("module_bay_count", "module_bays", "installed_module"),
    ):
        if not getattr(ap_dev, counter, None):
            continue
        for bay in getattr(nb.nb.dcim, endpoint).filter(device_id=ap_dev.id):
            if getattr(bay, installed_attr, None):
                continue
            try:
                bay.delete()
                removed += 1
            except Exception as exc:
                log.error("  AP %s: could not delete %s %s: %s", ap_dev.name, endpoint[:-1].replace("_", " "), bay.name, exc)
    if removed:
        log.info("  AP %s: removed %d empty bay(s) (wrong template)", ap_dev.name, removed)
    return removed


def _is_wireless_type(iface_type: Optional[str]) -> bool:
    return (iface_type or "").startswith("ieee802.11")


def _ap_iface_fixups(existing, final_type: Optional[str]) -> dict:
    """
    Extra fields an AP interface patch needs to pass Netbox validation: rf_role
    is only valid on wireless types, but template-created interfaces can carry
    it on any type (templates skip validation). Clear it when the interface
    is (or becomes) non-wireless.
    """
    if existing is None:
        return {}
    rf_role = getattr(existing, "rf_role", None)
    rf_role = getattr(rf_role, "value", rf_role)
    if rf_role and not _is_wireless_type(final_type):
        return {"rf_role": ""}
    return {}


def _ap_wired_iface_type(model: str) -> str:
    """Speed type for a new GigabitEthernet0 (datasheet uplinks; a template's own type is kept)."""
    m = (model or "").upper()
    if m.startswith("CW9166"):
        return "5gbase-t"
    if m.startswith(("C9120", "C9124", "CW9162")):
        return "2.5gbase-t"
    return "1000base-t"


def _ap_radio_iface_type(model: str) -> str:
    """Netbox wireless type for a new Dot11Radio<slot> (a template's own type is kept)."""
    m = (model or "").upper()
    if m.startswith("CW917"):
        return "ieee802.11be"
    if m.startswith(("C9120", "C9124", "C9130", "C9136", "CW916", "ISR-AP1101AX")):
        return "ieee802.11ax"
    if m.startswith("AIR-"):        # Aironet wave 1/2
        return "ieee802.11ac"
    return "other"


def _ap_radio_slots(radio_ports: list[dict], radio_mac: str) -> list[tuple[str, str]]:
    """(slot, Netdisco port type) of the WLC radio ports carrying the AP's radio base MAC, by slot."""
    out = []
    for p in radio_ports:
        if not radio_mac or (p.get("mac") or "").lower() != radio_mac.lower():
            continue
        slot = (p.get("port") or "").rsplit(".", 1)[-1]
        if slot.isdigit():
            out.append((slot, p.get("type") or "?"))
    return sorted(out, key=lambda st: int(st[0]))


def _ap_radios(radio_ports: list[dict], radio_mac: str) -> list[str]:
    """ "slot (band)" labels of the WLC radio ports whose MAC is the AP's radio base MAC."""
    return [
        f"{slot} ({_AP_RADIO_BANDS.get(rtype.lower(), rtype)})"
        for slot, rtype in _ap_radio_slots(radio_ports, radio_mac)
    ]


def _ap_note_block(
    parsed: dict, controller_name: str, radios: list[str], controller_id: Optional[int] = None,
    uplink_id: Optional[int] = None,
) -> str:
    """
    The discobox-owned part of an AP's comments (Markdown, marker-delimited).
    Only slow-changing facts: no DHCP IP and no timestamp, so the block (and
    the AP's changelog) only changes when something real does. Links are
    Netbox-relative: the controller by id, the uplink switch by id when it
    resolved (see _resolve_ap_uplinks), else as a device search on its short name.
    """
    # as the WLC shows it ("show ap summary" Location column): "<site tag>/<tag>"
    tag = "/".join(x for x in (parsed.get("site_tag"), parsed.get("tag")) if x)
    uplink = parsed.get("uplink_name")
    if uplink:
        short = uplink.split(".", 1)[0]
        href = f"/dcim/devices/{uplink_id}/" if uplink_id else f"/dcim/devices/?q={quote(short)}"
        uplink = f"[{uplink}]({href})"
        if parsed.get("uplink_port"):
            uplink += f" {parsed['uplink_port']}"
        if parsed.get("uplink_ip"):
            uplink += f" ({parsed['uplink_ip']})"
    controller = f"[{controller_name}](/dcim/devices/{controller_id}/)" if controller_id else controller_name
    rows = [
        ("Controller", controller),
        ("Location", tag),
        ("Uplink", uplink),
    ]
    lines = [AP_NOTE_BEGIN, "## Wireless"]
    lines += [f" - {k}: {v}" for k, v in rows if v]
    # nested lists: Python-Markdown (Netbox) needs a 4-space indent
    tags = [(label, parsed[f"tag_{key}"]) for key, label in AP_TAG_KINDS if parsed.get(f"tag_{key}")]
    if tags:
        lines.append(" - Tags:")
        lines += [f"     - {k}: {v}" for k, v in tags]
    macs = [(k, v) for k, v in (("Ethernet", parsed.get("ethernet_mac")), ("Radio", parsed.get("dot3_mac"))) if v]
    if macs:
        lines.append(" - MAC:")
        lines += [f"     - {k}: {v}" for k, v in macs]
    if radios:
        lines.append(" - Radios:")
        lines += [f"     - {r}" for r in radios]
    lines.append(AP_NOTE_END)
    return "\n".join(lines)


def _ap_uplink_key(parsed: dict) -> tuple[str, str]:
    return (parsed.get("uplink_name") or "", parsed.get("uplink_ip") or "")


def _chunks(items: list, size: int = 50):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _resolve_ap_uplinks(nb, parsed_aps: list[dict], log=None) -> dict[tuple[str, str], int]:
    """
    Netbox device id per AP uplink (uplink_name, uplink_ip) from the WLC's AP
    descriptions, in bulk: a few chunked queries per pass for all distinct
    switches, never one per AP. Per uplink, the first unambiguous hit of:
      1. uplink_ip = a device's primary IPv4
      2. exact name, the reported one or its short name (case-insensitive)
      3. a unique device whose name starts with "<short>." (FQDN vs short name)
    Unresolved uplinks are left out (the note keeps its search link).
    """
    log = log or logger
    keys = {_ap_uplink_key(p) for p in parsed_aps if p.get("uplink_name") or p.get("uplink_ip")}
    if not keys:
        return {}
    dcim, ipam = nb.nb.dcim, nb.nb.ipam

    by_ip: dict[str, set] = {}
    ips = sorted({ip for _, ip in keys if ip})
    for chunk in _chunks(ips):
        addr_ids = [a.id for a in ipam.ip_addresses.filter(address=chunk)]
        for ids in _chunks(addr_ids):
            for d in dcim.devices.filter(primary_ip4_id=ids):
                host = str(getattr(d.primary_ip4, "address", "") or "").split("/")[0]
                by_ip.setdefault(host, set()).add(d.id)

    by_name: dict[str, set] = {}
    names = sorted({n.lower() for n, _ in keys if n} | {n.split(".", 1)[0].lower() for n, _ in keys if n})
    for chunk in _chunks(names):
        for d in dcim.devices.filter(name__ie=chunk):
            by_name.setdefault((d.name or "").lower(), set()).add(d.id)
    by_prefix: dict[str, set] = {}
    shorts = sorted({n.split(".", 1)[0].lower() for n, _ in keys if n})
    unresolved = [s for s in shorts if s not in by_name]
    for chunk in _chunks(unresolved):
        for d in dcim.devices.filter(name__isw=chunk):
            lname = (d.name or "").lower()
            short = lname.split(".", 1)[0]
            if short in unresolved and lname.startswith(short + "."):
                by_prefix.setdefault(short, set()).add(d.id)

    out: dict[tuple[str, str], int] = {}
    for name, ip in keys:
        short = name.split(".", 1)[0].lower()
        for hits in (by_ip.get(ip) if ip else None, by_name.get(name.lower()) if name else None,
                     by_name.get(short) if name else None, by_prefix.get(short) if name else None):
            if hits and len(hits) == 1:
                out[(name, ip)] = next(iter(hits))
                break
    log.debug("AP uplinks: %d of %d resolved to a Netbox device", len(out), len(keys))
    return out


def _merge_note_block(comments: str, block: str, block_re: re.Pattern) -> Optional[str]:
    """
    comments with the marker-delimited block (matched by block_re) replaced or
    appended, or None when it is unchanged, so an unchanged device gets no
    changelog entry. Text outside the markers (other tools' blocks, manual
    notes) is never touched.
    """
    comments = comments or ""
    current = block_re.search(comments)
    if current:
        if current.group(0) == block:
            return None
        return comments[:current.start()] + block + comments[current.end():]
    return f"{comments.rstrip()}\n\n{block}" if comments.strip() else block


def _merge_ap_note(comments: str, block: str) -> Optional[str]:
    return _merge_note_block(comments, block, _AP_NOTE_RE)


WLC_NOTE_BEGIN = "<!-- discobox:wlc -->"
WLC_NOTE_END = "<!-- /discobox:wlc -->"
_WLC_NOTE_RE = re.compile(re.escape(WLC_NOTE_BEGIN) + r".*?" + re.escape(WLC_NOTE_END), re.DOTALL)


def _wlc_note_block(cf_controller: str, device_id: int) -> str:
    """Controller comments block: a Netbox-relative link listing every device whose controller CF points here."""
    return "\n".join([
        WLC_NOTE_BEGIN,
        "## Wireless controller",
        f" - [List all APs of this controller](/dcim/devices/?cf_{cf_controller}={device_id})",
        WLC_NOTE_END,
    ])
