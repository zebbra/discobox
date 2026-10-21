"""
discobox: Netdisco → Netbox sync library, used by server.py.

    log         per-device logging, sync error counter
    util        parsing, names, MACs, write retries, device locks, discover timeout policy
    netdisco    Netdisco API client
    netbox      Netbox client and type resolution
    neighbors   LLDP/CDP/FDB neighbor resolution
    interfaces  ports → interfaces, matching, PoE, orphans
    aps         access points reported by a WLC
    ha          HA pairs and VIP devices
    reconcile   Netbox ↔ Netdisco gaps; discover missing devices, auto-create, tag fixes, liveness
    sync        one device's sync
"""
from ._version import __version__
from .aps import configure_ap_fingerprint
from .netbox import NetboxClient
from .netdisco import NetdiscoClient
from .reconcile import discover_missing, fetch_liveness, find_gaps, fix_tag_mismatches
from .sync import _recently_touched, sync_device
from .util import (
    _parse_snmp_timeout_us,
    configure_discover_snmptimeout,
    configure_read_fields,
    discover_snmptimeout_us,
    read_cf,
    validate_ip,
)

__all__ = [
    "NetboxClient", "NetdiscoClient", "__version__", "_parse_snmp_timeout_us", "_recently_touched",
    "configure_ap_fingerprint", "configure_discover_snmptimeout", "configure_read_fields", "discover_snmptimeout_us",
    "discover_missing", "fetch_liveness", "find_gaps", "fix_tag_mismatches", "read_cf", "sync_device", "validate_ip",
]
