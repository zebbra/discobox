"""Netdisco API client."""

import json
import sys
from typing import Callable, Optional

import requests
import urllib3

from .log import logger
from .util import DISCOVER_SNMPTIMEOUT, _mount_read_retries

# ── Netdisco client ────────────────────────────────────────────────────────────

class NetdiscoClient:
    def __init__(
        self,
        base_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        verify_tls: bool = True,
        on_request: Optional[Callable[[str], None]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        _mount_read_retries(self.session)
        self.session.verify = verify_tls
        self.session.headers.update({"Accept": "application/json", "User-Agent": "discobox"})
        if not verify_tls:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._username = username
        self._password = password
        self._on_request = on_request or (lambda method: None)
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
            logger.debug("Netdisco auth via static token")
        elif username and password:
            self._login(username, password)
        else:
            raise ValueError("NetdiscoClient requires either token or username+password")

    def _login(self, username: str, password: str) -> None:
        url = f"{self.base_url}/login"
        logger.debug("POST %s", url)
        self._on_request("POST")
        resp = self.session.post(url, auth=(username, password), timeout=15)
        resp.raise_for_status()
        token = resp.json().get("api_key") or resp.json().get("token")
        if not token:
            logger.error("Netdisco login response did not contain a token: %s", resp.text)
            sys.exit(1)
        self.session.headers["Authorization"] = token
        logger.debug("Netdisco login OK")

    def _reauth(self) -> bool:
        """Re-login with username/password if credentials are available. Returns True on success."""
        if self._username and self._password:
            logger.warning("Netdisco token expired: re-authenticating")
            self._login(self._username, self._password)
            return True
        return False

    def _get(self, path: str) -> dict | list:
        url = f"{self.base_url}{path}"
        logger.debug("GET %s", url)
        self._on_request("GET")
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 401 and self._reauth():
            self._on_request("GET")
            resp = self.session.get(url, timeout=30)
        if resp.status_code == 401:
            logger.error("Netdisco 401 GET %s: response: %s", url, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body) -> dict | list:
        url = f"{self.base_url}{path}"
        logger.debug("POST %s", url)
        self._on_request("POST")
        resp = self.session.post(url, json=body, timeout=30)
        if resp.status_code == 401 and self._reauth():
            self._on_request("POST")
            resp = self.session.post(url, json=body, timeout=30)
        if resp.status_code == 401:
            logger.error("Netdisco 401 POST %s: response: %s", url, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    def get_device(self, ip: str) -> dict:
        result = self._get(f"/api/v1/object/device/{ip}")
        # Some Netdisco versions return a list, others a dict
        if isinstance(result, list):
            return result[0] if result else {}
        return result

    def find_canonical_ip(self, ip: str) -> Optional[str]:
        """
        Resolve `ip` to a device's own canonical IP via Netdisco's device search,
        which (unlike get_device) matches against ANY IP Netdisco knows for a
        device, not just the one it's keyed on.

        For clustered devices, Netbox's primary_ip4 is sometimes the shared/
        floating address rather than the member's own management IP — Netdisco
        only discovers/polls the latter, so /object/device/{floating_ip} 404s
        even though Netdisco knows the device perfectly well under a different
        IP. Returns None on no match or an ambiguous (>1) match — never guesses.
        """
        result = self._get(f"/api/v1/search/device?ip={ip}&matchall=false&seeallcolumns=false")
        if isinstance(result, list) and len(result) == 1:
            return result[0].get("ip") or None
        return None

    def get_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/ports")

    def get_device_ips(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/device_ips")

    def get_modules(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/modules")

    def get_powered_ports(self, ip: str) -> list[dict]:
        return self._get(f"/api/v1/object/device/{ip}/powered_ports")

    def get_nodes(self, ip: str) -> list[dict]:
        """The device's forwarding table as Netdisco macsucked it: {mac, port, vlan, active, ...}."""
        return self._get(f"/api/v1/object/device/{ip}/nodes")

    def get_all_devices(self) -> list[dict]:
        """
        Full device inventory via /api/v1/search/device. That endpoint requires
        both "limit" and "offset" when no "q"/filter param is given, so page
        through it rather than requesting everything in one call.
        """
        devices: list[dict] = []
        limit = 1000
        offset = 0
        while True:
            page = self._get(f"/api/v1/search/device?fields=ip,dns,name,device_auth_tag,last_discover&limit={limit}&offset={offset}")
            if not page:
                break
            devices.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return devices

    def get_queue_status(self, since: str = "1h") -> dict:
        return self._get(f"/api/v1/queue/status?since={since}")

    def enqueue_discover(self, ip: str, device_auth_tag_hint: Optional[str] = None, snmp_timeout_us: Optional[int] = None) -> None:
        """snmp_timeout_us is the per-request value (see discover_snmptimeout_us); None = the policy floor."""
        extra: dict = {
            "snmptimeout": snmp_timeout_us if snmp_timeout_us is not None else DISCOVER_SNMPTIMEOUT["floor_us"],
            "skip_neighbor_queue": True,
        }
        if device_auth_tag_hint:
            extra["device_auth_tag_hint"] = device_auth_tag_hint
        self._post("/api/v1/queue/jobs", [{"action": "discover", "device": ip, "extra": json.dumps(extra)}])
