#!/usr/bin/env python3
"""
discobox — Netdisco → Netbox sync library.

Imported by cli.py (one-shot CLI) and server.py (FastAPI webhook receiver).
"""

import ipaddress
import logging
import os
import re
from typing import Optional

import pynetbox
import requests
import urllib3

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("discobox")


# ── Interface type mapping ─────────────────────────────────────────────────────

def map_iftype(nd_type: Optional[str], iface_name: Optional[str]) -> str:
    """
    Map a Netdisco interface to a Netbox type slug.

    Name-prefix matching is tried first because Netdisco reports nearly every
    physical port as ifType=ethernetCsmacd regardless of speed — the interface
    name (e.g. "TenGigabitEthernet0/1") carries the real type information.
    SNMP ifType is used as a fallback for LAG and virtual interfaces.

    Adapted from github.com/joeladria/netdisco-netbox-diode.
    """
    lname = (iface_name or "").lower()
    nd_type_l = (nd_type or "").lower()

    # LAG / virtual via ifType — these are reliable regardless of name
    if "lag" in nd_type_l:                                          return "lag"
    if nd_type_l in ("softwareloopback", "propvirtual", "l2vlan",
                     "l3ipvlan"):                                   return "virtual"
    if nd_type_l == "tunnel":                                       return "virtual"

    # Name-prefix matches
    if lname.startswith(("loopback", "lo")):                        return "virtual"
    if lname.startswith("vlan"):                                     return "virtual"
    if lname.startswith(("tunnel", "tun", "gr", "ipip", "null")):   return "virtual"
    if lname.startswith(("port-channel", "po", "bundle-ether", "be",
                          "etherchannel", "eth-trunk", "ae")):      return "lag"
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

    return "other"


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


# ── Netdisco client ────────────────────────────────────────────────────────────

class NetdiscoClient:
    def __init__(self, base_url: str, username: str, password: str, verify_tls: bool = True):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = verify_tls
        self.session.headers.update({"Accept": "application/json"})
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        url = f"{self.base_url}/login"
        logger.debug("POST %s", url)
        resp = self.session.post(url, auth=(username, password), timeout=15)
        resp.raise_for_status()
        token = resp.json().get("api_key") or resp.json().get("token")
        if not token:
            logger.error("Netdisco login response did not contain a token: %s", resp.text)
            sys.exit(1)
        self.session.headers["Authorization"] = token
        logger.debug("Netdisco login OK")

    def _get(self, path: str) -> dict | list:
        url = f"{self.base_url}{path}"
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_device(self, ip: str) -> dict:
        result = self._get(f"/api/v1/object/device/{ip}")
        # Some Netdisco versions return a list, others a dict
        if isinstance(result, list):
            return result[0] if result else {}
        return result

    def get_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/ports")

    def get_device_ips(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/device_ips")

    def get_modules(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/modules")


# ── Netbox client ──────────────────────────────────────────────────────────────

class NetboxClient:
    def __init__(self, url: str, token: str, verify_tls: bool = True, changelog_message: str = "DiscoBox"):
        self.nb = pynetbox.api(url, token=token)
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.nb.http_session.verify = False
        self.nb.http_session.headers.update({"X-Netbox-Change-Reason": changelog_message})

    def find_device_by_ip(self, ip: str) -> Optional[pynetbox.core.response.Record]:
        """
        Find a Netbox device by management IP.

        Strategy:
          1. Search IPAM for the address; walk the assignment back to a device.
          2. Fall back to scanning devices whose primary_ip4 matches (slow on
             large installs, but avoids needing CIDR notation).
        """
        for addr in self.nb.ipam.ip_addresses.filter(address=ip):
            if addr.assigned_object_type == "dcim.interface" and addr.assigned_object:
                iface = self.nb.dcim.interfaces.get(addr.assigned_object.id)
                if iface and iface.device:
                    device = self.nb.dcim.devices.get(iface.device.id)
                    if device:
                        return device

        # Fallback: check primary_ip4 for devices matching a free-text search
        for dev in self.nb.dcim.devices.filter(q=ip):
            if dev.primary_ip4 and dev.primary_ip4.address.split("/")[0] == ip:
                return dev

        return None

    def update_device_fields(self, device: pynetbox.core.response.Record, nd_device: dict) -> None:
        """Update serial, and custom fields os_ver / os_name / os_release."""
        patch: dict = {}

        if nd_device.get("serial"):
            patch["serial"] = nd_device["serial"]

        custom: dict = {}
        if nd_device.get("os_ver"):
            custom["os_version"] = nd_device["os_ver"]
        if nd_device.get("os"):
            custom["os_name"] = nd_device["os"]
        os_release = parse_os_release(nd_device.get("description"))
        if os_release:
            custom["os_release"] = os_release
        if custom:
            patch["custom_fields"] = custom

        if not patch:
            logger.debug("Device fields — nothing to update")
            return

        device.update(patch)
        logger.info(
            "Device fields updated — serial=%r os_name=%r os_ver=%r os_release=%r",
            patch.get("serial"), custom.get("os_name"),
            custom.get("os_ver"), custom.get("os_release"),
        )

    def fetch_interfaces(self, device_id: int) -> dict[str, pynetbox.core.response.Record]:
        """Return all existing interfaces for a device, keyed by name."""
        return {
            iface.name: iface
            for iface in self.nb.dcim.interfaces.filter(device_id=device_id)
        }

    @staticmethod
    def _nb_value(val) -> object:
        """Unwrap pynetbox ChoiceValue / Record to a plain comparable value."""
        if hasattr(val, "value"):   # ChoiceValue (type, duplex, ...)
            return val.value
        if hasattr(val, "id"):      # Related Record (device, ...)
            return val.id
        return val

    def upsert_interface(
        self,
        device_id: int,
        data: dict,
        existing: Optional[pynetbox.core.response.Record],
    ) -> str:
        """
        Create or update a Netbox interface.

        MAC address is handled separately via upsert_mac() because in Netbox 4.x
        it is its own model (dcim.mac-addresses) rather than a plain string field.

        Returns one of: "created", "updated", "unchanged".
        """
        mac = data.pop("mac_address", None)

        if existing is None:
            iface = self.nb.dcim.interfaces.create(**data, device=device_id)
            action = "created"
        else:
            patch = {}
            for k, v in data.items():
                if v is None:
                    continue
                nb_val = self._nb_value(getattr(existing, k, None))
                if nb_val != v:
                    logger.debug("  diff %-20s  nb=%r  nd=%r", k, nb_val, v)
                    patch[k] = v
            if patch:
                existing.update(patch)
                action = "updated"
            else:
                action = "unchanged"
            iface = existing

        if mac:
            self._upsert_mac(iface, mac)

        return action

    def _upsert_mac(self, iface: pynetbox.core.response.Record, mac: str) -> None:
        """
        Create or update the primary MAC address for an interface (Netbox 4.x).

        In Netbox 4.x MAC addresses are standalone objects assigned to an interface.
        After creating the MAC object we explicitly set primary_mac_address on the
        interface so it shows up as the primary.
        """
        primary = getattr(iface, "primary_mac_address", None)

        if primary:
            existing_mac = str(primary.mac_address).upper() if hasattr(primary, "mac_address") else ""
            if existing_mac == mac.upper():
                return  # already correct
            mac_obj = self.nb.dcim.mac_addresses.get(primary.id)
            mac_obj.update({"mac_address": mac})
            logger.debug("  MAC updated → %s", mac)
        else:
            mac_obj = self.nb.dcim.mac_addresses.create(
                mac_address=mac,
                assigned_object_type="dcim.interface",
                assigned_object_id=iface.id,
            )
            iface.update({"primary_mac_address": mac_obj.id})
            logger.debug("  MAC created → %s", mac)

    def upsert_ip(self, address: str, iface: pynetbox.core.response.Record) -> str:
        """
        Create or update an IP address assigned to an interface.

        Returns one of: "created", "unchanged", "skipped".
        """
        host = str(ipaddress.ip_interface(address).ip)

        # Check if this host IP already exists anywhere in Netbox
        existing = list(self.nb.ipam.ip_addresses.filter(address=host))

        if existing:
            nb_ip = existing[0]
            same_iface = (
                nb_ip.assigned_object_type == "dcim.interface"
                and nb_ip.assigned_object_id == iface.id
            )
            if same_iface:
                nb_host = str(ipaddress.ip_interface(nb_ip.address).ip)
                nd_host = str(ipaddress.ip_interface(address).ip)
                if nb_ip.address == address or nb_host != nd_host:
                    return "unchanged"
                # Same host, different prefix — fix it
                nb_ip.update({"address": address})
                return "fixed"

            # Check if the IP is on a dummy placeholder interface
            assigned_name = (
                nb_ip.assigned_object.name
                if nb_ip.assigned_object_type == "dcim.interface" and nb_ip.assigned_object
                else None
            )
            if assigned_name and assigned_name.lower() in {d.lower() for d in DUMMY_INTERFACES}:
                nb_ip.update({
                    "address": address,
                    "assigned_object_type": "dcim.interface",
                    "assigned_object_id": iface.id,
                })
                logger.info(
                    "  IP %-20s moved from dummy interface %r → %s",
                    address, assigned_name, iface.name,
                )
                return "moved"

            logger.warning(
                "  IP %-20s already in Netbox (id=%s, assigned to %s) — skipping",
                address, nb_ip.id, nb_ip.assigned_object or "unassigned",
            )
            return "skipped"

        self.nb.ipam.ip_addresses.create(
            address=address,
            status="active",
            assigned_object_type="dcim.interface",
            assigned_object_id=iface.id,
        )
        return "created"

    def get_or_create_manufacturer(self, name: str) -> pynetbox.core.response.Record:
        """Return an existing Manufacturer or create one."""
        existing = self.nb.dcim.manufacturers.get(name=name)
        if existing:
            return existing
        mfr = self.nb.dcim.manufacturers.create(name=name, slug=slugify(name))
        logger.debug("  Manufacturer created: %s", name)
        return mfr

    def get_or_create_device_type(
        self,
        manufacturer: pynetbox.core.response.Record,
        model: str,
    ) -> pynetbox.core.response.Record:
        """Return an existing DeviceType or create one under manufacturer."""
        results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if r.model == model), None)
        if not existing:
            slug = slugify(model)
            results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if r.slug == slug), None)
        if existing:
            return existing
        dt = self.nb.dcim.device_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=model,
            comments="Created by discobox",
        )
        logger.debug("  DeviceType created: %s / %s", manufacturer.name, model)
        return dt

    def get_or_create_module_type(
        self,
        manufacturer: pynetbox.core.response.Record,
        model: str,
    ) -> pynetbox.core.response.Record:
        """Return an existing ModuleType or create one under manufacturer."""
        results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if r.model == model), None)
        if not existing:
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, part_number=model))
            existing = next((r for r in results if r.part_number == model), None)
        if not existing:
            slug = slugify(model)
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if r.slug == slug), None)
        if existing:
            if not existing.part_number:
                existing.update({"part_number": model})
            return existing
        mt = self.nb.dcim.module_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=model,
            comments="Created by discobox",
        )
        logger.debug("  ModuleType created: %s / %s", manufacturer.name, model)
        return mt

    def upsert_module_bay(
        self,
        device: pynetbox.core.response.Record,
        name: str,
        position: str,
    ) -> pynetbox.core.response.Record:
        """Return an existing ModuleBay or create one on the device."""
        results = list(self.nb.dcim.module_bays.filter(device_id=device.id, name=name))
        if len(results) > 1:
            logger.warning("  ModuleBay %r: %d duplicates in Netbox, using first", name, len(results))
            return results[0]
        if results:
            return results[0]
        bay = self.nb.dcim.module_bays.create(
            device=device.id,
            name=name,
            position=position,
        )
        logger.debug("  ModuleBay created: %s", name)
        return bay

    def upsert_module(
        self,
        device: pynetbox.core.response.Record,
        bay: pynetbox.core.response.Record,
        module_type: pynetbox.core.response.Record,
        serial: str,
    ) -> tuple[str, pynetbox.core.response.Record]:
        """Install or update a Module in a ModuleBay. Returns (action, module_record)."""
        results = list(self.nb.dcim.modules.filter(module_bay_id=bay.id))
        existing = results[0] if results else None
        if existing:
            patch = {}
            if self._nb_value(existing.module_type) != module_type.id:
                patch["module_type"] = module_type.id
            if (existing.serial or "") != serial:
                patch["serial"] = serial
            if not patch:
                return "unchanged", existing
            existing.update(patch)
            return "updated", existing

        module = self.nb.dcim.modules.create(
            device=device.id,
            module_bay=bay.id,
            module_type=module_type.id,
            serial=serial,
            status="active",
        )
        return "created", module

    def remove_stale_device_bays(
        self,
        device: pynetbox.core.response.Record,
        patterns: list,
    ) -> int:
        """
        Delete device bays whose names match any of the given compiled regex patterns.

        These are typically auto-created from the DeviceType template
        (e.g. 'PS-A', 'PSU1', 'Fan 1') and should be removed when managing
        power supplies and modules via inventory items / module bays instead.

        Returns the number of bays deleted.
        """
        deleted = 0
        for bay in self.nb.dcim.device_bays.filter(device_id=device.id):
            if any(p.search(bay.name) for p in patterns):
                if bay.installed_device:
                    logger.warning(
                        "  Device bay %r has installed device %r — skipping deletion",
                        bay.name, bay.installed_device,
                    )
                    continue
                bay.delete()
                logger.info("  Deleted device bay %r", bay.name)
                deleted += 1
        return deleted

    def remove_empty_dummy_interfaces(
        self,
        device: pynetbox.core.response.Record,
        dummy_names: set[str],
    ) -> int:
        """
        Delete dummy placeholder interfaces that have no IPs assigned.

        Safe to run after IP sync — if the IP was moved to the real interface
        the dummy is now empty and can be removed.

        Returns the number of interfaces deleted.
        """
        deleted = 0
        lower_names = {n.lower() for n in dummy_names}
        for iface in self.nb.dcim.interfaces.filter(device_id=device.id):
            if iface.name.lower() not in lower_names:
                continue
            ips = list(self.nb.ipam.ip_addresses.filter(
                assigned_object_type="dcim.interface",
                assigned_object_id=iface.id,
            ))
            if ips:
                logger.warning(
                    "  Dummy interface %r still has %d IP(s) — skipping deletion",
                    iface.name, len(ips),
                )
                continue
            iface.delete()
            logger.info("  Deleted empty dummy interface %r", iface.name)
            deleted += 1
        return deleted

    def upsert_sfp(
        self,
        device: pynetbox.core.response.Record,
        iface: pynetbox.core.response.Record,
        manufacturer: pynetbox.core.response.Record,
        name: str,
        model: str,
        serial: str,
    ) -> str:
        """
        Create or update an Inventory Item representing an SFP on an interface.

        Returns one of: "created", "updated", "unchanged".
        """
        existing = list(self.nb.dcim.inventory_items.filter(
            device_id=device.id,
            component_type="dcim.interface",
            component_id=iface.id,
        ))

        if existing:
            inv = existing[0]
            patch = {}
            if (inv.serial or "") != serial:
                patch["serial"] = serial
            if (inv.part_id or "") != model:
                patch["part_id"] = model
            if not patch:
                return "unchanged"
            inv.update(patch)
            return "updated"

        self.nb.dcim.inventory_items.create(
            device=device.id,
            name=name,
            manufacturer=manufacturer.id,
            part_id=model,
            serial=serial,
            component_type="dcim.interface",
            component_id=iface.id,
        )
        return "created"

    def upsert_inventory_item(
        self,
        device: pynetbox.core.response.Record,
        name: str,
        manufacturer: Optional[pynetbox.core.response.Record],
        model: str,
        serial: str,
    ) -> str:
        """
        Create or update a device-level Inventory Item (no component link).

        Returns one of: "created", "updated", "unchanged".
        """
        existing = list(self.nb.dcim.inventory_items.filter(
            device_id=device.id,
            name=name,
        ))

        if existing:
            inv = existing[0]
            patch = {}
            if (inv.serial or "") != serial:
                patch["serial"] = serial
            if (inv.part_id or "") != model:
                patch["part_id"] = model
            if not patch:
                return "unchanged"
            inv.update(patch)
            return "updated"

        payload: dict = {"device": device.id, "name": name, "serial": serial}
        if model:
            payload["part_id"] = model
        if manufacturer:
            payload["manufacturer"] = manufacturer.id
        self.nb.dcim.inventory_items.create(**payload)
        return "created"


# ── Field mapping ──────────────────────────────────────────────────────────────

# Device bay name patterns (regex) auto-created from DeviceType templates that
# should be removed when the device is managed via module bays / inventory items.
STALE_DEVICE_BAY_PATTERNS: list[re.Pattern] = [re.compile(p, re.IGNORECASE) for p in [
    r"^Network Module$",
    r"^PS-[AB]$",
    r"^PS\s*\d+$",
    r"^PSU\s*\d+$",
    r"^Fan\s*\d+$",
]]

# Dummy/placeholder interface names used as IP anchors before a proper sync.
# IPs found on these interfaces will be moved to the correct interface.
# In housekeeping, empty dummy interfaces are deleted.
DUMMY_INTERFACES: set[str] = {
    "main", "mgmt", "mgmt0",
}

# Interface name prefixes (case-insensitive) to skip during port sync.
PORT_BLACKLIST_PREFIXES: tuple[str, ...] = (
    "null",
    "modem",
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
    type_str = chassis.get("type", "")
    # Cisco types — let caller use the device's existing Netbox manufacturer
    if type_str.lower().startswith("cev"):
        return None
    # Other vendors encode their name before the first dot
    if "." in type_str:
        vendor = type_str.split(".")[0]
        if vendor:
            return vendor.capitalize()
    return None


NULL_MAC = "00:00:00:00:00:00"


_CISCO_ABBREV = [
    ("HundredGigE",          "Hu"),
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


def _slot_from_iface(topo: str, name: str) -> Optional[int]:
    """
    Extract module slot key from interface name for stack/fex topologies.

    Stack:  GigabitEthernet2/0/1  → 2   (first number = stack member)
    FEX:    Ethernet101/1/1        → 101 (first number ≥ 100 = FEX ID)
    """
    m = re.match(r"[A-Za-z]+(\d+)/\d+/\d+", name)
    if not m:
        return None
    slot = int(m.group(1))
    if topo == "fex":
        return slot if slot >= 100 else None
    if topo == "stack":
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

def port_to_netbox(port: dict) -> dict:
    """
    Map a Netdisco port dict to Netbox dcim.interfaces fields.

    Netdisco field notes:
      port  = ifDescr  (full interface name, e.g. "FastEthernet0/4")
      name  = ifAlias  (configured description, e.g. "Fa0/4" or a custom label)
      descr = ifDescr  (same as port in practice)
      up_admin          "up" / "down"  →  Netbox enabled (bool)
      speed             "10 Mbps"      →  Netbox speed in kbps
      duplex            operational duplex; may be NULL → fall back to duplex_admin
    """
    speed_kbps = parse_speed_kbps(port.get("speed"))

    raw_duplex = port.get("duplex") or port.get("duplex_admin")
    duplex = raw_duplex if raw_duplex in ("full", "half", "auto") else None

    # Use ifAlias as description only when it differs from the interface name
    full_name = port.get("port") or port.get("descr") or ""
    alias = port.get("name", "")
    description = alias if alias and alias.lower() != full_name.lower() else ""

    return {
        "name":        full_name,
        "type":        map_iftype(port.get("type"), full_name),
        "enabled":     port.get("up_admin", "").lower() == "up",
        "mtu":         port.get("mtu") or None,
        "mac_address": clean_mac(port.get("mac")),
        "speed":       speed_kbps,
        "duplex":      duplex,
        "description": description,
    }


# ── Sync logic ─────────────────────────────────────────────────────────────────

def sync_device(
    ip: str,
    nd: NetdiscoClient,
    nb: NetboxClient,
    sync_mac: bool = True,
    sync_ip: bool = True,
    sync_modules: bool = True,
    sync_sfp: bool = True,
    housekeeping: bool = False,
) -> dict:
    """
    Sync device fields, interfaces, MACs, IPs, modules, and SFPs.

    Returns a dict with:
      ok          bool   — True if no errors occurred
      interfaces  dict   — created/updated/unchanged/error counts
      ips         dict   — created/fixed/moved/unchanged/skipped/error counts
      modules     dict   — created/updated/unchanged/error counts
    """
    logger.info("── device %s", ip)

    try:
        nd_device = nd.get_device(ip)
        nd_ports = nd.get_ports(ip)
    except requests.HTTPError as exc:
        logger.error("Netdisco request failed for %s: %s", ip, exc)
        return False

    nd_hostname = nd_device.get("name") or nd_device.get("dns") or ""
    logger.info("Netdisco  hostname=%r  ports=%d", nd_hostname, len(nd_ports))

    nb_device = nb.find_device_by_ip(ip)
    if not nb_device:
        logger.error("No Netbox device found for IP %s — skipping", ip)
        return False

    logger.info("Netbox    device=%r  id=%s", nb_device.name, nb_device.id)

    if nd_hostname and nb_device.name:
        if nd_hostname.lower() != nb_device.name.lower():
            logger.warning(
                "Hostname mismatch for %s — Netdisco=%r  Netbox=%r",
                ip, nd_hostname, nb_device.name,
            )

    nb.update_device_fields(nb_device, nd_device)

    if housekeeping:
        deleted_bays = nb.remove_stale_device_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        deleted_ifaces = nb.remove_empty_dummy_interfaces(nb_device, DUMMY_INTERFACES)
        logger.info(
            "Housekeeping — deleted %d stale device bay(s), %d empty dummy interface(s)",
            deleted_bays, deleted_ifaces,
        )

    counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    ip_counts: dict[str, int] = {"created": 0, "fixed": 0, "moved": 0, "unchanged": 0, "skipped": 0, "error": 0}
    mod_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    sfp_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}

    # slot_to_module populated during module sync; consumed by interface→module pass.
    slot_to_module: dict[int, int] = {}  # slot key (stack pos / FEX ID) → nb module id
    topo = "standalone"                   # updated inside sync_modules block

    # ── Modules (before interfaces so bays exist when interfaces are assigned) ────

    if sync_modules:
        try:
            nd_mods = nd.get_modules(ip)
        except requests.HTTPError as exc:
            logger.error("Could not fetch modules from Netdisco: %s", exc)
            nd_mods = []

        chassis = [m for m in nd_mods if m.get("class") == "chassis" and m.get("model")]
        stack_root = next((m for m in nd_mods if m.get("class") == "stack"), None)
        has_stack = stack_root is not None
        # Nexus FEX topology: stack root is a logical fabric, not a real member stack.
        # The primary N9K chassis + satellite FEX units all appear as chassis entries.
        is_fex = has_stack and (stack_root.get("type", "").lower() == "cevcontainernexuslogicalfabric")
        is_standalone = not has_stack and len(chassis) == 1

        # Log tree
        root = next((m for m in nd_mods if not m.get("parent")), None)
        if root:
            logger.info("  %s (root)  %r  model=%s  serial=%s",
                        root.get("class", "?"), root.get("name", ""),
                        root.get("model", ""), root.get("serial", ""))
        for i, ch in enumerate(chassis):
            prefix = "└──" if i == len(chassis) - 1 else "├──"
            logger.info("  %s chassis  %r  model=%-20s  serial=%s",
                        prefix, ch.get("name", ""), ch.get("model", ""), ch.get("serial", ""))
        topo = "fex" if is_fex else ("standalone" if is_standalone else "stack")
        logger.info("Modules   chassis=%d  topology=%s", len(chassis), topo)

        manufacturer = nb_device.device_type.manufacturer

        def _update_device_type(ch: dict) -> None:
            """Update DeviceType (and serial) on nb_device from a chassis entry."""
            model = ch.get("model", "")
            serial = ch.get("serial") or ""
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            device_type = nb.get_or_create_device_type(mfr, model)
            patch = {}
            if nb_device.device_type.id != device_type.id:
                patch["device_type"] = device_type.id
            if serial and (nb_device.serial or "") != serial:
                patch["serial"] = serial
            if patch:
                nb_device.update(patch)
                # pynetbox replaces device_type with a plain int after update — restore the object
                nb_device.device_type = device_type
                logger.info("  DeviceType → %s / %s  serial=%s  updated", mfr.name, model, serial)
                mod_counts["updated"] += 1
            else:
                logger.debug("  DeviceType unchanged")
                mod_counts["unchanged"] += 1

        def _upsert_chassis_bay(ch: dict, slot_key: Optional[int] = None) -> None:
            """Create/update a module bay + module for a chassis member."""
            name = ch.get("name", "")
            model = ch.get("model", "")
            serial = ch.get("serial") or ""
            position = str(ch.get("pos", ""))
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            module_type = nb.get_or_create_module_type(mfr, model)
            bay = nb.upsert_module_bay(nb_device, name, position)
            action, module = nb.upsert_module(nb_device, bay, module_type, serial)
            mod_counts[action] += 1
            if slot_key is not None and module:
                slot_to_module[slot_key] = module.id
            if action != "unchanged":
                logger.info("  %-30s %-20s serial=%-15s %s", name, model, serial, action)
            else:
                logger.debug("  %-30s unchanged", name)

        if is_standalone:
            # Single device — update DeviceType on the device itself
            try:
                _update_device_type(chassis[0])
            except Exception as exc:
                mod_counts["error"] += 1
                logger.error("  DeviceType update error: %s", exc)

        elif is_fex:
            # Nexus FEX: primary N9K chassis → DeviceType update; FEX units → module bays
            device_serial = nd_device.get("serial", "")
            primary = next((c for c in chassis if c.get("serial") == device_serial), chassis[0])
            fex_units = [c for c in chassis if c is not primary]
            try:
                _update_device_type(primary)
            except Exception as exc:
                mod_counts["error"] += 1
                logger.error("  DeviceType update error: %s", exc)
            for ch in fex_units:
                # Extract FEX ID from name: "Fex-101 Nexus2332 Chassis" → 101
                fex_match = re.match(r"[Ff]ex-(\d+)", ch.get("name", ""))
                slot_key = int(fex_match.group(1)) if fex_match else None
                try:
                    _upsert_chassis_bay(ch, slot_key)
                except Exception as exc:
                    mod_counts["error"] += 1
                    logger.error("  %-30s error: %s", ch.get("name", ""), exc)

        else:
            # Traditional stack — create a module bay + module per chassis member
            for ch in chassis:
                try:
                    _upsert_chassis_bay(ch, slot_key=ch.get("pos"))
                except Exception as exc:
                    mod_counts["error"] += 1
                    logger.error("  %-30s error: %s", ch.get("name", ""), exc)

        logger.info(
            "Modules — updated=%d unchanged=%d errors=%d",
            mod_counts.get("updated", 0) + mod_counts.get("created", 0),
            mod_counts["unchanged"], mod_counts["error"],
        )

        # PSUs — inventory items on the device (skip Unknown type with no model)
        psus = [
            m for m in nd_mods
            if m.get("class") == "powerSupply" and m.get("type") != "cevPowerSupplyUnknown"
        ]
        logger.info("PSUs      entries: %d", len(psus))
        psu_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
        for psu in psus:
            psu_name = psu.get("name", "")
            psu_model = psu.get("model", "")
            psu_serial = psu.get("serial", "")
            try:
                action = nb.upsert_inventory_item(
                    nb_device, psu_name,
                    manufacturer if psu_model else None,
                    psu_model, psu_serial,
                )
                psu_counts[action] += 1
                if action != "unchanged":
                    logger.info("  PSU %-35s model=%-20s serial=%s → %s",
                                psu_name, psu_model or "-", psu_serial or "-", action)
                else:
                    logger.debug("  PSU %-35s unchanged", psu_name)
            except Exception as exc:
                psu_counts["error"] += 1
                logger.error("  PSU %-35s error: %s", psu_name, exc)

        logger.info(
            "PSUs — created=%d updated=%d unchanged=%d errors=%d",
            psu_counts["created"], psu_counts["updated"], psu_counts["unchanged"], psu_counts["error"],
        )

    # ── Interfaces ────────────────────────────────────────────────────────────────

    existing_ifaces = nb.fetch_interfaces(nb_device.id)
    logger.debug("Netbox    existing interfaces: %d", len(existing_ifaces))

    for port in nd_ports:
        iface_name = port.get("port") or port.get("descr") or "?"
        if iface_name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            logger.debug("  %-40s blacklisted — skipping", iface_name)
            continue
        try:
            nb_data = port_to_netbox(port)
            if not sync_mac:
                nb_data.pop("mac_address", None)
            action = nb.upsert_interface(nb_device.id, nb_data, existing_ifaces.get(iface_name))
            counts[action] += 1
            if action != "unchanged":
                logger.info("  %-40s %s", iface_name, action)
            else:
                logger.debug("  %-40s unchanged", iface_name)
        except Exception as exc:
            counts["error"] += 1
            logger.error("  %-40s error: %s", iface_name, exc)

    nd_names = {
        port.get("port") or port.get("descr") for port in nd_ports
        if not (port.get("port") or port.get("descr") or "").lower().startswith(PORT_BLACKLIST_PREFIXES)
    }
    for name in existing_ifaces:
        if name not in nd_names and not name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            logger.warning("  %-40s in Netbox but not in Netdisco", name)

    # ── Interface → Module assignment ─────────────────────────────────────────────

    if slot_to_module:
        all_ifaces = nb.fetch_interfaces(nb_device.id)
        mod_iface_counts = {"updated": 0, "unchanged": 0, "skipped": 0}
        for iface_name, iface in all_ifaces.items():
            slot = _slot_from_iface(topo, iface_name)
            mod_id = slot_to_module.get(slot) if slot is not None else None
            if mod_id is None:
                mod_iface_counts["skipped"] += 1
                continue
            current = nb._nb_value(getattr(iface, "module", None))
            if current != mod_id:
                iface.update({"module": mod_id})
                mod_iface_counts["updated"] += 1
                logger.debug("  %-40s → module slot %s", iface_name, slot)
            else:
                mod_iface_counts["unchanged"] += 1
        logger.info(
            "Interface→Module — updated=%d unchanged=%d skipped=%d",
            mod_iface_counts["updated"], mod_iface_counts["unchanged"], mod_iface_counts["skipped"],
        )

    # ── IPs ───────────────────────────────────────────────────────────────────────

    if sync_ip:
        # Re-fetch interfaces so newly created ones have IDs
        existing_ifaces = nb.fetch_interfaces(nb_device.id)
        try:
            nd_ips = nd.get_device_ips(ip)
        except requests.HTTPError as exc:
            logger.error("Could not fetch device IPs from Netdisco: %s", exc)
            nd_ips = []

        for entry in nd_ips:
            address = entry.get("alias") or entry.get("ip")
            subnet = entry.get("subnet")
            port_name = entry.get("port")
            if not address or not port_name:
                continue
            # Build CIDR if needed
            if "/" not in str(address) and subnet:
                try:
                    prefix = ipaddress.ip_network(subnet, strict=False).prefixlen
                    address = f"{address}/{prefix}"
                except ValueError:
                    pass
            iface = existing_ifaces.get(port_name)
            if not iface:
                logger.warning("  IP %-20s skipped — interface %r not found in Netbox", address, port_name)
                ip_counts["skipped"] += 1
                continue
            try:
                action = nb.upsert_ip(address, iface)
                ip_counts[action] += 1
                if action in ("created", "fixed", "moved"):
                    logger.info("  IP %-20s → %s on %s", address, action, port_name)
                elif action == "unchanged":
                    logger.debug("  IP %-20s → unchanged on %s", address, port_name)
            except Exception as exc:
                ip_counts["error"] += 1
                logger.error("  IP %-20s on %-30s error: %s", address, port_name, exc)

        logger.info(
            "IPs — created=%d fixed=%d moved=%d unchanged=%d skipped=%d errors=%d",
            ip_counts["created"], ip_counts["fixed"], ip_counts["moved"],
            ip_counts["unchanged"], ip_counts["skipped"], ip_counts["error"],
        )

    # ── SFPs ──────────────────────────────────────────────────────────────────────

    if sync_sfp:
        # nd_mods already fetched if sync_modules ran; fetch only if needed
        if not sync_modules:
            try:
                nd_mods = nd.get_modules(ip)
            except requests.HTTPError as exc:
                logger.error("Could not fetch modules from Netdisco: %s", exc)
                nd_mods = []

        sfps = [
            m for m in nd_mods
            if m.get("class") == "port" and m.get("model") and m.get("serial")
        ]
        logger.info("SFPs      entries: %d", len(sfps))

        # Re-fetch interfaces to include anything created this run
        existing_ifaces = nb.fetch_interfaces(nb_device.id)
        manufacturer = nb_device.device_type.manufacturer

        for sfp in sfps:
            name = sfp.get("name", "")
            model = sfp.get("model", "")
            serial = sfp.get("serial", "")
            iface = existing_ifaces.get(expand_iface_name(name))
            if not iface:
                logger.warning("  SFP %-20s skipped — interface not in Netbox", name)
                continue
            try:
                action = nb.upsert_sfp(nb_device, iface, manufacturer, name, model, serial)
                sfp_counts[action] += 1
                if action != "unchanged":
                    logger.info("  SFP %-20s model=%-20s serial=%s → %s", name, model, serial, action)
                else:
                    logger.debug("  SFP %-20s unchanged", name)
            except Exception as exc:
                sfp_counts["error"] += 1
                logger.error("  SFP %-20s error: %s", name, exc)

        logger.info(
            "SFPs — created=%d updated=%d unchanged=%d errors=%d",
            sfp_counts["created"], sfp_counts["updated"], sfp_counts["unchanged"], sfp_counts["error"],
        )

    logger.info(
        "── done %s  created=%d updated=%d unchanged=%d errors=%d",
        ip, counts["created"], counts["updated"], counts["unchanged"], counts["error"],
    )
    return {
        "ok": counts["error"] == 0,
        "interfaces": counts,
        "ips": ip_counts if sync_ip else {},
        "modules": mod_counts if sync_modules else {},
        "sfps": sfp_counts if sync_sfp else {},
    }


