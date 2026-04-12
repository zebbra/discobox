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

    def get_powered_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/powered_ports")


# ── Netbox client ──────────────────────────────────────────────────────────────

class _ChangelogSession(requests.Session):
    """requests.Session that injects changelog_message into every write body."""
    def __init__(self, changelog_message: str):
        super().__init__()
        self._changelog_message = changelog_message

    def request(self, method, url, **kwargs):
        if method.upper() in ("POST", "PATCH", "PUT") and self._changelog_message:
            json_data = kwargs.get("json")
            if isinstance(json_data, dict):
                kwargs["json"] = {**json_data, "changelog_message": self._changelog_message}
        return super().request(method, url, **kwargs)


class NetboxClient:
    def __init__(self, url: str, token: str, verify_tls: bool = True, changelog_message: str = "DiscoBox"):
        session = _ChangelogSession(changelog_message)
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session.verify = False
        self.nb = pynetbox.api(url, token=token)
        self.nb.http_session = session

    def find_device_by_ip(self, ip: str, hostname: str = "", serial: str = "") -> Optional[pynetbox.core.response.Record]:
        """
        Find a Netbox device by management IP, with hostname and serial fallbacks.

        Strategy:
          1. Search IPAM for the address; walk the assignment back to a device.
          2. Scan devices whose primary_ip4 matches.
          3. Exact hostname match (name__ie) — works when Netdisco has the full FQDN.
          4. Short-name contains match (name__ic) — fallback for when Netdisco returns
             a hostname from PTR that differs from the Netbox FQDN (e.g. wrong domain).
          5. Serial match — most reliable identifier when IP and hostname both fail.
        """
        for addr in self.nb.ipam.ip_addresses.filter(address=ip):
            if addr.assigned_object_type == "dcim.interface" and addr.assigned_object:
                iface = self.nb.dcim.interfaces.get(addr.assigned_object.id)
                if iface and iface.device:
                    device = self.nb.dcim.devices.get(iface.device.id)
                    if device:
                        return device

        for dev in self.nb.dcim.devices.filter(q=ip):
            if dev.primary_ip4 and dev.primary_ip4.address.split("/")[0] == ip:
                return dev

        if hostname:
            short = hostname.lower().split(".")[0]
            for dev in self.nb.dcim.devices.filter(name__ie=hostname):
                logger.info("Device found by hostname %r (no IP match for %s)", dev.name, ip)
                return dev
            for dev in self.nb.dcim.devices.filter(name__ic=short):
                if dev.name.lower().split(".")[0] == short:
                    logger.info("Device found by short hostname %r (no IP match for %s)", dev.name, ip)
                    return dev

        if serial:
            dev = self.find_device_by_serial(serial)
            if dev:
                logger.info("Device found by serial %r (no IP/hostname match for %s)", serial, ip)
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
        part_number: Optional[str] = None,
    ) -> pynetbox.core.response.Record:
        """Return an existing DeviceType or create one under manufacturer."""
        slug = slugify(model)
        results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if getattr(r, "model", None) == model), None)
        if not existing and part_number:
            results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, part_number=part_number))
            existing = next((r for r in results if getattr(r, "part_number", None) == part_number), None)
        if not existing:
            results = list(self.nb.dcim.device_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if getattr(r, "slug", None) == slug), None)
        if existing:
            if not getattr(existing, "part_number", None):
                existing.update({"part_number": part_number or model})
            return existing
        dt = self.nb.dcim.device_types.create(
            manufacturer=manufacturer.id,
            model=model,
            slug=slugify(model),
            part_number=part_number or model,
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
        slug = slugify(model)
        results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, model=model))
        existing = next((r for r in results if getattr(r, "model", None) == model), None)
        if not existing:
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, part_number=model))
            existing = next((r for r in results if getattr(r, "part_number", None) == model), None)
        if not existing:
            results = list(self.nb.dcim.module_types.filter(manufacturer_id=manufacturer.id, slug=slug))
            existing = next((r for r in results if getattr(r, "slug", None) == slug), None)
        if existing:
            if not getattr(existing, "part_number", None):
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

    def find_device_by_serial(self, serial: str) -> Optional[pynetbox.core.response.Record]:
        results = list(self.nb.dcim.devices.filter(serial=serial))
        return results[0] if results else None

    def find_ha_partner(self, device_name: str) -> Optional[pynetbox.core.response.Record]:
        """
        Find the HA partner device by swapping the node indicator in the hostname.

        Matches patterns like p1h↔p2h, node1↔node2, -1↔-2 near the end of the
        short hostname (before the first dot).
        """
        short = device_name.split(".")[0]
        suffix = device_name[len(short):]
        m = re.search(r"(p|node|-)([12])(h?)", short, re.IGNORECASE)
        if not m:
            return None
        other = "2" if m.group(2) == "1" else "1"
        partner_short = short[:m.start(2)] + other + short[m.end(2):]
        partner_name = partner_short + suffix
        results = list(self.nb.dcim.devices.filter(name__ie=partner_name))
        if not results:
            results = list(self.nb.dcim.devices.filter(name__ie=partner_short))
        return results[0] if results else None

    def upsert_virtual_chassis(
        self,
        name: str,
        members: list[tuple[pynetbox.core.response.Record, int]],
    ) -> tuple[str, pynetbox.core.response.Record]:
        """Create or update a Virtual Chassis and assign member devices."""
        results = list(self.nb.dcim.virtual_chassis.filter(name=name))
        vc = results[0] if results else None
        action = "unchanged"
        if not vc:
            vc = self.nb.dcim.virtual_chassis.create(name=name, master=members[0][0].id)
            action = "created"

        for device, position in members:
            patch = {}
            if self._nb_value(getattr(device, "virtual_chassis", None)) != vc.id:
                patch["virtual_chassis"] = vc.id
            if getattr(device, "vc_position", None) != position:
                patch["vc_position"] = position
            if patch:
                device.update(patch)
                action = action if action == "created" else "updated"
        return action, vc

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
    r"^Slot\s*\d+$",
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
    "bluetooth",
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
    sync_poe: bool = True,
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
    log = logging.getLogger(f"discobox.{ip}")
    log.info("── device %s", ip)

    try:
        nd_device = nd.get_device(ip)
        nd_ports = nd.get_ports(ip)
    except requests.HTTPError as exc:
        log.error("Netdisco request failed for %s: %s", ip, exc)
        return {"ok": False, "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    nd_hostname = nd_device.get("name") or nd_device.get("dns") or ""
    nd_serial = nd_device.get("serial") or ""
    logger.info("Netdisco  hostname=%r  ports=%d", nd_hostname, len(nd_ports))

    nb_device = nb.find_device_by_ip(ip, hostname=nd_hostname, serial=nd_serial)
    if not nb_device:
        log.error("No Netbox device found for IP %s or hostname %r — skipping", ip, nd_hostname)
        return {"ok": False, "interfaces": {}, "ips": {}, "modules": {}, "sfps": {}}

    logger.info("Netbox    device=%r  id=%s", nb_device.name, nb_device.id)

    # ── HA / VIP detection ────────────────────────────────────────────────────────
    # Signal: SNMP hostname belongs to a different Netbox device than the IP lookup found.
    # This happens when the management IP is a cluster VIP. The serial approach alone
    # is unreliable because previous syncs may have written the active node's serial
    # onto the VIP device. Hostname mismatch is the primary signal.
    vip_device = None
    if nd_hostname and nb_device.name:
        nd_short = nd_hostname.lower().split(".")[0]
        nb_short = nb_device.name.lower().split(".")[0]
        if nd_short != nb_short:
            # Try to find the real physical device by SNMP hostname
            real_device = None
            # 1. Exact match, then contains with short-name verification
            for dev in nb.nb.dcim.devices.filter(name__ie=nd_hostname):
                real_device = dev
                break
            if not real_device:
                for dev in nb.nb.dcim.devices.filter(name__ic=nd_short):
                    if dev.name.lower().split(".")[0] == nd_short:
                        real_device = dev
                        break
            # 2. Serial fallback
            if not real_device and nd_serial:
                serial_match = nb.find_device_by_serial(nd_serial)
                if serial_match and serial_match.id != nb_device.id:
                    real_device = serial_match

            if real_device and real_device.id != nb_device.id:
                log.info(
                    "HA VIP detected — %r → redirecting to %r",
                    nb_device.name, real_device.name,
                )
                vip_device = nb_device
                nb_device = real_device

                # Find partner node by swapping node indicator in hostname
                partner_dev = nb.find_ha_partner(nb_device.name)

                # Create / update Virtual Chassis using the VIP device's short name as identity
                vc_name = vip_device.name.split(".")[0]
                active_m = re.search(r"p(\d+)h", nb_device.name, re.IGNORECASE)
                active_pos = int(active_m.group(1)) if active_m else 1
                vc_members: list[tuple] = [(nb_device, active_pos)]
                if partner_dev:
                    partner_m = re.search(r"p(\d+)h", partner_dev.name, re.IGNORECASE)
                    partner_pos = int(partner_m.group(1)) if partner_m else (2 if active_pos == 1 else 1)
                    vc_members.append((partner_dev, partner_pos))
                    log.info("HA partner found: %r", partner_dev.name)
                else:
                    log.warning("HA partner not found for %r — VC will have one member", nb_device.name)
                try:
                    vc_action, _ = nb.upsert_virtual_chassis(vc_name, vc_members)
                    log.info("HA VirtualChassis %r — %s", vc_name, vc_action)
                except Exception as exc:
                    log.error("HA VirtualChassis error: %s", exc)

                # Delete VIP device (only during housekeeping — it's destructive)
                if housekeeping:
                    try:
                        nb.nb.dcim.devices.get(vip_device.id).delete()
                        log.info("Deleted VIP device %r (id=%s)", vip_device.name, vip_device.id)
                    except Exception as exc:
                        log.error("Could not delete VIP device %r: %s", vip_device.name, exc)
            else:
                log.warning(
                    "Hostname mismatch for %s — Netdisco=%r  Netbox=%r",
                    ip, nd_hostname, nb_device.name,
                )

    nb.update_device_fields(nb_device, nd_device)

    if housekeeping:
        deleted_bays = nb.remove_stale_device_bays(nb_device, STALE_DEVICE_BAY_PATTERNS)
        deleted_ifaces = nb.remove_empty_dummy_interfaces(nb_device, DUMMY_INTERFACES)
        log.info(
            "Housekeeping — deleted %d stale device bay(s), %d empty dummy interface(s)",
            deleted_bays, deleted_ifaces,
        )

    counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    ip_counts: dict[str, int] = {"created": 0, "fixed": 0, "moved": 0, "unchanged": 0, "skipped": 0, "error": 0}
    mod_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
    sfp_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}

    # slot_to_module populated during module sync; consumed by interface→module pass.
    slot_to_module: dict[int, int] = {}  # slot key (stack pos / FEX ID) → nb module id
    # slot_to_device populated for VSS; used to route blades to the correct member device.
    slot_to_device: dict[int, object] = {}  # VSS pos → nb device record
    topo = "standalone"                   # updated inside sync_modules block

    # ── Modules (before interfaces so bays exist when interfaces are assigned) ────

    if sync_modules:
        try:
            nd_mods = nd.get_modules(ip)
        except requests.HTTPError as exc:
            log.error("Could not fetch modules from Netdisco: %s", exc)
            nd_mods = []

        chassis = [m for m in nd_mods if m.get("class") == "chassis" and m.get("model")]
        stack_root = next((m for m in nd_mods if m.get("class") == "stack"), None)
        has_stack = stack_root is not None
        # Nexus FEX topology: stack root is a logical fabric, not a real member stack.
        # The primary N9K chassis + satellite FEX units all appear as chassis entries.
        is_fex = has_stack and (stack_root.get("type", "").lower() == "cevcontainernexuslogicalfabric")
        is_vss = has_stack and (
            "virtualstack" in stack_root.get("type", "").lower()
            or "virtual stack" in stack_root.get("name", "").lower()
        )
        is_standalone = not has_stack and len(chassis) == 1

        # Log tree
        root = next((m for m in nd_mods if not m.get("parent")), None)
        if root:
            log.info("  %s (root)  %r  type=%s  model=%s  serial=%s",
                        root.get("class", "?"), root.get("name", ""),
                        root.get("type", ""), root.get("model", ""), root.get("serial", ""))
        for i, ch in enumerate(chassis):
            prefix = "└──" if i == len(chassis) - 1 else "├──"
            log.info("  %s chassis  %r  model=%s  serial=%s",
                        prefix, ch.get("name", ""), ch.get("model", ""), ch.get("serial", ""))
        topo = "fex" if is_fex else ("vss" if is_vss else ("standalone" if is_standalone else "stack"))
        log.info("Modules   chassis=%d  topology=%s", len(chassis), topo)

        manufacturer = nb_device.device_type.manufacturer

        def _update_device_type(ch: dict) -> None:
            """Update DeviceType (and serial) on nb_device from a chassis entry."""
            part_number = ch.get("model", "") or ""
            serial = ch.get("serial") or ""
            # Skip device type update when model looks like a raw OID fragment
            # (e.g. ".112.100.1003") — keep whatever is already set in Netbox.
            model = parse_sw_model(ch.get("sw_ver", "")) or part_number
            if not model or model.startswith("."):
                if serial and (nb_device.serial or "") != serial:
                    nb_device.update({"serial": serial})
                    log.info("  serial=%s updated (no valid model from Netdisco)", serial)
                else:
                    log.debug("  DeviceType skipped — no valid model from Netdisco")
                mod_counts["unchanged"] += 1
                return
            vendor_name = vendor_from_chassis(ch)
            mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
            device_type = nb.get_or_create_device_type(mfr, model, part_number=part_number)
            patch = {}
            if nb_device.device_type.id != device_type.id:
                patch["device_type"] = device_type.id
            if serial and (nb_device.serial or "") != serial:
                patch["serial"] = serial
            if patch:
                nb_device.update(patch)
                # pynetbox replaces device_type with a plain int after update — restore the object
                nb_device.device_type = device_type
                log.info("  DeviceType → %s / %s  serial=%s  updated", mfr.name, model, serial)
                mod_counts["updated"] += 1
            else:
                log.debug("  DeviceType unchanged")
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
                log.info("  %s  %s  serial=%s  %s", name, model, serial, action)
            else:
                log.debug("  %-30s unchanged", name)

        if is_standalone:
            # Single device — update DeviceType on the device itself
            try:
                _update_device_type(chassis[0])
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error: %s", exc)

        elif is_fex:
            # Nexus FEX: primary N9K chassis → DeviceType update; FEX units → module bays
            device_serial = nd_device.get("serial", "")
            primary = next((c for c in chassis if c.get("serial") == device_serial), chassis[0])
            fex_units = [c for c in chassis if c is not primary]
            try:
                _update_device_type(primary)
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error: %s", exc)
            for ch in fex_units:
                # Extract FEX ID from name: "Fex-101 Nexus2332 Chassis" → 101
                fex_match = re.match(r"[Ff]ex-(\d+)", ch.get("name", ""))
                slot_key = int(fex_match.group(1)) if fex_match else None
                try:
                    _upsert_chassis_bay(ch, slot_key)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  %-30s error: %s", ch.get("name", ""), exc)

        elif is_vss:
            # Cat9500 StackWise Virtual — two physical devices in separate Netbox records.
            # Create/update a Virtual Chassis to link them; no module bays on either device.
            device_serial = nd_device.get("serial", "")
            primary_ch = next((c for c in chassis if c.get("serial") == device_serial), chassis[0])
            partner_chs = [c for c in chassis if c is not primary_ch]

            # Update DeviceType for the primary (the device we're syncing right now)
            try:
                _update_device_type(primary_ch)
            except Exception as exc:
                mod_counts["error"] += 1
                log.error("  DeviceType update error (primary): %s", exc)

            primary_pos = primary_ch.get("pos", 1)
            vc_members: list[tuple] = [(nb_device, primary_pos)]

            for partner_ch in partner_chs:
                partner_serial = partner_ch.get("serial", "")
                partner_pos = partner_ch.get("pos", 2)
                partner_model = partner_ch.get("model", "")

                # 1. Find partner by serial number
                partner_dev = nb.find_device_by_serial(partner_serial) if partner_serial else None

                # 2. Fallback: append "-<pos>" before the domain suffix
                #    e.g. "sw1.example.com" → "sw1-2.example.com"
                if not partner_dev and nd_hostname:
                    parts = nd_hostname.split(".", 1)
                    if len(parts) > 1:
                        partner_hostname = f"{parts[0]}-{partner_pos}.{parts[1]}"
                    else:
                        partner_hostname = f"{nd_hostname}-{partner_pos}"
                    results = list(nb.nb.dcim.devices.filter(name__ie=partner_hostname))
                    if results:
                        partner_dev = results[0]
                        log.info("  VSS partner found by hostname %r", partner_dev.name)

                if not partner_dev:
                    log.warning(
                        "  VSS partner not found (serial=%r) — Virtual Chassis will be incomplete",
                        partner_serial,
                    )
                    mod_counts["error"] += 1
                    continue

                # Update DeviceType on the partner device
                try:
                    vendor_name = vendor_from_chassis(partner_ch)
                    mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
                    partner_part_number = partner_model
                    partner_model = parse_sw_model(partner_ch.get("sw_ver", "")) or partner_part_number
                    partner_dt = nb.get_or_create_device_type(mfr, partner_model, part_number=partner_part_number)
                    patch: dict = {}
                    if partner_dev.device_type.id != partner_dt.id:
                        patch["device_type"] = partner_dt.id
                    if partner_serial and (partner_dev.serial or "") != partner_serial:
                        patch["serial"] = partner_serial
                    if patch:
                        partner_dev.update(patch)
                        log.info(
                            "  VSS partner DeviceType → %s  serial=%s  updated",
                            partner_model, partner_serial,
                        )
                        mod_counts["updated"] += 1
                    else:
                        mod_counts["unchanged"] += 1
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  VSS partner DeviceType update error: %s", exc)

                vc_members.append((partner_dev, partner_pos))

            # Create/update Virtual Chassis linking both physical devices
            vc_name = nd_hostname.split(".")[0] if nd_hostname else f"vc-{ip}"
            try:
                vc_action, _vc = nb.upsert_virtual_chassis(vc_name, vc_members)
                log.info("  VirtualChassis %r — %s", vc_name, vc_action)
            except Exception as exc:
                log.error("  VirtualChassis error: %s", exc)

            # Build pos → device map so blades can be routed to the right member
            for _dev, _pos in vc_members:
                slot_to_device[_pos] = _dev

        else:
            # Traditional stack — create a module bay + module per chassis member.
            # Netdisco pos is 0-indexed; Cisco interface names are 1-indexed (Gi1/0/1 = member 1).
            for ch in chassis:
                try:
                    pos = ch.get("pos")
                    _upsert_chassis_bay(ch, slot_key=pos + 1 if pos is not None else None)
                except Exception as exc:
                    mod_counts["error"] += 1
                    log.error("  %-30s error: %s", ch.get("name", ""), exc)

        log.info(
            "Modules — updated=%d unchanged=%d errors=%d",
            mod_counts.get("updated", 0) + mod_counts.get("created", 0),
            mod_counts["unchanged"], mod_counts["error"],
        )

        # Supplement os_version from chassis sw_ver when the device field was empty
        # (e.g. Fortinet: "FortiGate-600F v7.4.8,build2795,250523 (GA.M)" → "7.4.8")
        if not nd_device.get("os_ver"):
            for ch in chassis:
                ver = parse_sw_ver(ch.get("sw_ver", ""))
                if ver:
                    try:
                        nb_device.update({"custom_fields": {"os_version": ver}})
                        log.info("  OS version from chassis sw_ver: %s", ver)
                    except Exception as exc:
                        log.error("  OS version update error: %s", exc)
                    break

        # PSUs — inventory items on the device (skip Unknown type with no model)
        psus = [
            m for m in nd_mods
            if m.get("class") == "powerSupply" and m.get("type") != "cevPowerSupplyUnknown"
        ]
        log.info("PSUs      entries: %d", len(psus))
        psu_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
        for psu in psus:
            psu_name = psu.get("name", "")
            psu_model = psu.get("model", "")
            psu_serial = psu.get("serial", "")
            # For VSS route to the correct member device via "Switch N" prefix
            psu_target = nb_device
            if slot_to_device:
                sw_match = re.match(r"Switch\s+(\d+)", psu_name, re.IGNORECASE)
                if sw_match:
                    psu_target = slot_to_device.get(int(sw_match.group(1)), nb_device)
            try:
                action = nb.upsert_inventory_item(
                    psu_target, psu_name,
                    manufacturer if psu_model else None,
                    psu_model, psu_serial,
                )
                psu_counts[action] += 1
                if action != "unchanged":
                    log.info("  PSU %s  model=%s  serial=%s  %s",
                                psu_name, psu_model or "-", psu_serial or "-", action)
                else:
                    log.debug("  PSU %-35s unchanged", psu_name)
            except Exception as exc:
                psu_counts["error"] += 1
                log.error("  PSU %-35s error: %s", psu_name, exc)

        log.info(
            "PSUs — created=%d updated=%d unchanged=%d errors=%d",
            psu_counts["created"], psu_counts["updated"], psu_counts["unchanged"], psu_counts["error"],
        )

        # Blades (linecards / supervisors) — module bay + module per slot.
        # Only meaningful for VSS chassis; other topologies don't produce class=module entries.
        blades = [
            m for m in nd_mods
            if m.get("class") == "module"
            and m.get("model") and m.get("model") != "Unknown PID"
            and m.get("serial")
            and "transceiver" not in m.get("name", "").lower()
        ]
        log.info("Blades    entries: %d", len(blades))
        blade_counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0, "error": 0}
        for blade in blades:
            blade_name = blade.get("name", "")
            blade_model = blade.get("model", "")
            blade_serial = blade.get("serial", "")
            # Extract slot number from name: "Switch 1 Slot 3 Supervisor" → 3
            slot_match = re.search(r"Slot\s+(\d+)", blade_name, re.IGNORECASE)
            position = slot_match.group(1) if slot_match else ""
            # For VSS route to the correct member device via "Switch N" prefix
            target_device = nb_device
            if slot_to_device:
                sw_match = re.match(r"Switch\s+(\d+)", blade_name, re.IGNORECASE)
                if sw_match:
                    target_device = slot_to_device.get(int(sw_match.group(1)), nb_device)
            try:
                vendor_name = vendor_from_chassis(blade)
                mfr = nb.get_or_create_manufacturer(vendor_name) if vendor_name else manufacturer
                module_type = nb.get_or_create_module_type(mfr, blade_model)
                bay = nb.upsert_module_bay(target_device, blade_name, position)
                action, _ = nb.upsert_module(target_device, bay, module_type, blade_serial)
                blade_counts[action] += 1
                if action != "unchanged":
                    log.info("  Blade %s  model=%s  serial=%s  %s",
                                blade_name, blade_model, blade_serial, action)
                else:
                    log.debug("  Blade %-35s unchanged", blade_name)
            except Exception as exc:
                blade_counts["error"] += 1
                log.error("  Blade %-35s error: %s", blade_name, exc)

        log.info(
            "Blades — created=%d updated=%d unchanged=%d errors=%d",
            blade_counts["created"], blade_counts["updated"], blade_counts["unchanged"], blade_counts["error"],
        )

    # ── Interfaces ────────────────────────────────────────────────────────────────

    existing_ifaces = nb.fetch_interfaces(nb_device.id)
    logger.debug("Netbox    existing interfaces: %d", len(existing_ifaces))

    # Sort: parent interfaces before subinterfaces (dot-notation) so parents exist when children are created
    nd_ports_sorted = sorted(nd_ports, key=lambda p: (1 if "." in (p.get("port") or p.get("descr") or "") else 0))

    for port in nd_ports_sorted:
        iface_name = port.get("port") or port.get("descr") or "?"
        if iface_name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            log.debug("  %-40s blacklisted — skipping", iface_name)
            continue
        try:
            nb_data = port_to_netbox(port)
            if not sync_mac:
                nb_data.pop("mac_address", None)
            action = nb.upsert_interface(nb_device.id, nb_data, existing_ifaces.get(iface_name))
            counts[action] += 1
            if action != "unchanged":
                log.info("  %-40s %s", iface_name, action)
            else:
                log.debug("  %-40s unchanged", iface_name)
        except Exception as exc:
            counts["error"] += 1
            log.error("  %-40s error: %s", iface_name, exc)

    nd_names = {
        port.get("port") or port.get("descr") for port in nd_ports
        if not (port.get("port") or port.get("descr") or "").lower().startswith(PORT_BLACKLIST_PREFIXES)
    }
    for name in existing_ifaces:
        if name not in nd_names and not name.lower().startswith(PORT_BLACKLIST_PREFIXES):
            log.warning("  %-40s in Netbox but not in Netdisco", name)

    # Wire up parent links for subinterfaces (e.g. GigabitEthernet0/0/1.1132 → GigabitEthernet0/0/1)
    all_ifaces = nb.fetch_interfaces(nb_device.id)
    parent_updated = 0
    for iface_name, iface in all_ifaces.items():
        if "." not in iface_name:
            continue
        parent_name = iface_name.rsplit(".", 1)[0]
        parent = all_ifaces.get(parent_name)
        if not parent:
            continue
        current_parent = nb._nb_value(getattr(iface, "parent", None))
        if current_parent != parent.id:
            try:
                iface.update({"parent": parent.id})
                parent_updated += 1
                log.debug("  %s → parent %s", iface_name, parent_name)
            except Exception as exc:
                log.error("  %s parent link error: %s", iface_name, exc)
    if parent_updated:
        log.info("Subinterfaces — linked %d parent(s)", parent_updated)

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
                log.debug("  %-40s → module slot %s", iface_name, slot)
            else:
                mod_iface_counts["unchanged"] += 1
        log.info(
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
            log.error("Could not fetch device IPs from Netdisco: %s", exc)
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
                log.warning("  IP %-20s skipped — interface %r not found in Netbox", address, port_name)
                ip_counts["skipped"] += 1
                continue
            try:
                action = nb.upsert_ip(address, iface)
                ip_counts[action] += 1
                if action in ("created", "fixed", "moved"):
                    log.info("  IP %-20s → %s on %s", address, action, port_name)
                elif action == "unchanged":
                    log.debug("  IP %-20s → unchanged on %s", address, port_name)
            except Exception as exc:
                ip_counts["error"] += 1
                log.error("  IP %-20s on %-30s error: %s", address, port_name, exc)

        log.info(
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
                log.error("Could not fetch modules from Netdisco: %s", exc)
                nd_mods = []

        sfps = [
            m for m in nd_mods
            if m.get("class") == "port" and m.get("model") and m.get("serial")
        ]
        log.info("SFPs      entries: %d", len(sfps))

        # Re-fetch interfaces to include anything created this run.
        # For VSS, build a per-member interface cache so Switch 2 SFPs land on the right device.
        manufacturer = nb_device.device_type.manufacturer
        if slot_to_device:
            sfp_ifaces_by_device: dict[int, dict] = {
                pos: nb.fetch_interfaces(dev.id)
                for pos, dev in slot_to_device.items()
            }
        else:
            sfp_ifaces_by_device = {1: nb.fetch_interfaces(nb_device.id)}

        for sfp in sfps:
            name = sfp.get("name", "")
            model = sfp.get("model", "")
            serial = sfp.get("serial", "")
            expanded = expand_iface_name(name)
            # Determine which VSS member owns this interface (first digit run before first '/').
            # e.g. GigabitEthernet2/0/1 → switch 2.  Falls back to pos 1 for standalone/stack.
            sw_match = re.match(r"[A-Za-z-]+(\d+)/", expanded)
            sw_pos = int(sw_match.group(1)) if sw_match and sw_match.group(1) in {
                str(k) for k in sfp_ifaces_by_device
            } else 1
            target_ifaces = sfp_ifaces_by_device.get(sw_pos) or sfp_ifaces_by_device.get(1, {})
            target_device = slot_to_device.get(sw_pos, nb_device) if slot_to_device else nb_device
            iface = target_ifaces.get(expanded)
            if not iface:
                log.warning("  SFP %-20s skipped — interface not in Netbox", name)
                continue
            try:
                action = nb.upsert_sfp(target_device, iface, manufacturer, name, model, serial)
                sfp_counts[action] += 1
                if action != "unchanged":
                    log.info("  SFP %-20s model=%-20s serial=%s → %s", name, model, serial, action)
                else:
                    log.debug("  SFP %-20s unchanged", name)
            except Exception as exc:
                sfp_counts["error"] += 1
                log.error("  SFP %-20s error: %s", name, exc)

        log.info(
            "SFPs — created=%d updated=%d unchanged=%d errors=%d",
            sfp_counts["created"], sfp_counts["updated"], sfp_counts["unchanged"], sfp_counts["error"],
        )

    # ── PoE ───────────────────────────────────────────────────────────────────────

    poe_counts: dict[str, int] = {"updated": 0, "unchanged": 0, "skipped": 0, "error": 0}

    if sync_poe:
        try:
            powered_ports = nd.get_powered_ports(ip)
        except requests.HTTPError as exc:
            log.warning("Could not fetch powered ports (device may not support PoE): %s", exc)
            powered_ports = []

        if powered_ports:
            existing_ifaces = nb.fetch_interfaces(nb_device.id)
            poe_iface_names = {p["port"] for p in powered_ports if p.get("port")}
            for port_name in poe_iface_names:
                iface = existing_ifaces.get(port_name)
                if not iface:
                    poe_counts["skipped"] += 1
                    continue
                try:
                    current = nb._nb_value(getattr(iface, "poe_mode", None))
                    if current != "pse":
                        iface.update({"poe_mode": "pse"})
                        poe_counts["updated"] += 1
                        log.debug("  PoE %s → pse", port_name)
                    else:
                        poe_counts["unchanged"] += 1
                except Exception as exc:
                    poe_counts["error"] += 1
                    log.error("  PoE %s error: %s", port_name, exc)

            log.info(
                "PoE — updated=%d unchanged=%d skipped=%d errors=%d",
                poe_counts["updated"], poe_counts["unchanged"], poe_counts["skipped"], poe_counts["error"],
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
        "ha_vip": vip_device is not None,
    }


