"""
discobox settings: one schema for discobox.yaml (DISCOBOX_CONFIG), checked at
startup. An unknown key or a wrong type stops the server with the offending
path instead of being silently ignored.

Connections and secrets come from the environment, everything else from the
YAML file:

    NETBOX_URL, NETBOX_TOKEN, NETBOX_TLS_VERIFY
    NETDISCO_URL, NETDISCO_TOKEN (or NETDISCO_USERNAME + NETDISCO_PASSWORD), NETDISCO_TLS_VERIFY
    DISCOBOX_AUTH_TOKEN, DISCOBOX_CONFIG, DISCOBOX_STATE_DIR, DISCOBOX_LISTEN_PORT

Optional strings follow the YAML convention discobox always had: a missing key
takes the default, null or "" turns the feature off.
"""
from __future__ import annotations

import os
import re
from typing import Annotated, Any, Optional, Union

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, ValidationError


def _blank_none(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value.strip() if isinstance(value, str) else value


def _str_list(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(v).strip() for v in value if str(v).strip()]


def _vip_mode(value: Any) -> Any:
    return "off" if value is False else value     # YAML reads a bare off as false


OptStr = Annotated[Optional[str], BeforeValidator(_blank_none)]
VipMode = Annotated[str, BeforeValidator(_vip_mode)]
StrList = Annotated[list[str], BeforeValidator(_str_list)]
Duration = Union[int, str]            # µs int or "10s" / "2m"


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Auth(_Section):
    token: OptStr = None              # prefer DISCOBOX_AUTH_TOKEN
    metrics_path: str = "/metrics"


class Retry(_Section):
    delay: int = 480                  # seconds before the first retry of a timed-out sync, doubling
    max_delay: int = 1800
    max: int = 6


class CircuitBreaker(_Section):
    window: int = 120                 # seconds looked back
    threshold: int = 3                # timeouts in the window that pause syncing
    backoff: int = 120                # seconds paused


class HaResolve(_Section):
    url: OptStr = None                # Prometheus-compatible query API; default: reconcile.liveness.url
    metric: str = "fgHaStatsSyncStatus_info"
    hostname_label: str = "fgHaStatsHostname"
    target_label: str = "netbox_primary_ip"
    timeout: int = 15
    instance_label: str = "instance"
    mgmt_iface_name: str = "mgmt1"


class Sync(_Section):
    cooldown: int = 3600
    no_mac: bool = False
    no_ip: bool = False
    no_modules: bool = False
    no_sfp: bool = False
    no_poe: bool = False
    housekeeping: bool = False
    lldp_clear_stale: bool = False
    vip_mode: VipMode = "off"        # off | threenode | soft | hard
    vip_mode_by_vendor: dict[str, VipMode] = {}
    pause_on_error: bool = False
    remove_orphaned_interfaces: Optional[bool] = None   # unset: follows housekeeping
    orphan_delete_max_percent: int = 25
    touch_cooldown_days: int = 1
    max_concurrent: int = 3
    max_queue: int = 10000
    retry: Retry = Retry()
    circuit_breaker: CircuitBreaker = CircuitBreaker()
    ha_resolve: HaResolve = HaResolve()


class Cabling(_Section):
    scope: str = "site"
    source_cf: OptStr = "source"
    source_value: OptStr = "netdisco"


class CustomFields(_Section):
    source: OptStr = "source"
    source_value: str = "netdisco"
    os_version: OptStr = "os_version"
    os_name: OptStr = "os_name"
    os_release: OptStr = "os_release"
    stack_members: OptStr = "stack_members"
    stack_members_only_increase: bool = True
    touch: OptStr = "netdisco_last_update"
    inventory_rebuild: OptStr = "inventory_last_rebuild"
    controller: OptStr = "controller"
    auth_profile: OptStr = "snmp_auth_profile"       # read: Netdisco device_auth tag hint for discover jobs
    snmp_timeout: OptStr = "snmp_polling_timeout"    # read: SNMP polling budget → per-request discover timeout
    neighbor_text: OptStr = "neighbor"
    neighbor_port: OptStr = "neighbor_port"
    neighbor_device: OptStr = "neighbor_device"
    neighbor_iface: OptStr = "neighbor_iface"


class Aps(_Section):
    prune_interfaces: bool = True
    type_confirmed_untag: StrList = []
    skip_unchanged: bool = True
    full_pass_days: float = 7


class Types(_Section):
    create_missing: bool = True
    alias_cf: OptStr = "snmp_models"
    device_aliases: dict[str, Any] = {}
    module_aliases: dict[str, Any] = {}


class Library(_Section):
    path: OptStr = "/opt/devicetype-library"
    overlay: OptStr = None
    roles: StrList = []
    device_types: dict[str, str] = {}
    source_value: str = "devicetype-library"


class DiscoverSnmpTimeout(_Section):
    divisor: Optional[int] = None
    floor: Optional[Duration] = None
    cap: Optional[Duration] = None


class Netdisco(_Section):
    discover_snmptimeout: DiscoverSnmpTimeout = DiscoverSnmpTimeout()


class Liveness(_Section):
    url: OptStr = None
    query: str = 'max_over_time(up{job="snmp"}[6h])'
    label: str = "netbox_primary_ip"
    key: str = "ip"                   # ip | name
    timeout: int = 15
    tls_verify: bool = True


class Reconcile(_Section):
    max_queued: Optional[int] = None
    max_failed: Optional[int] = None
    max_enqueue: Optional[int] = None
    roles: StrList = ["router", "switch", "firewall", "appliance"]
    statuses: StrList = ["active"]
    require_auth_tag: bool = False
    defer_if_queued: bool = True
    defer_seconds: int = 3600
    not_in_netbox_stale_hours: Optional[float] = 24
    liveness: Liveness = Liveness()


class AutoCreate(_Section):
    role: OptStr = None
    site: OptStr = None
    status: str = "active"
    location: bool = False


class JobSchedule(_Section):
    interval: int                     # seconds; 0 = on demand only


class GapsSchedule(JobSchedule):
    max_age: int = 7200               # discover-missing / fix-tags refuse an older gaps result


class Jobs(_Section):
    gaps: GapsSchedule = GapsSchedule(interval=3600)
    discover_missing: JobSchedule = Field(default=JobSchedule(interval=86400), alias="discover-missing")
    types_bare: JobSchedule = Field(default=JobSchedule(interval=3600), alias="types-bare")


def _match_patterns(value: Any) -> Any:
    if not isinstance(value, dict) or not value:
        raise ValueError("match needs at least one path: pattern")
    out = {}
    for path, pattern in value.items():
        pattern = "" if pattern is None else str(pattern)
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"match {path}: invalid pattern {pattern!r}: {exc}") from None
        out[str(path)] = pattern
    return out


class InterfaceDefault(_Section):
    name: str = ""
    match: Annotated[dict[str, str], BeforeValidator(_match_patterns)]
                                      # dotted device path → regex (fullmatch, case-insensitive),
                                      # all must match: status.value, role.slug, custom_fields.x, tags
    set: dict[str, Any]               # interface custom field → value, filled where empty
    physical_only: bool = True        # skip virtual, LAG, bridge and wireless interfaces


class Settings(_Section):
    auth: Auth = Auth()
    sync: Sync = Sync()
    cabling: Cabling = Cabling()
    custom_fields: CustomFields = CustomFields()
    aps: Aps = Aps()
    types: Types = Types()
    library: Library = Library()
    netdisco: Netdisco = Netdisco()
    reconcile: Reconcile = Reconcile()
    auto_create: AutoCreate = AutoCreate()
    jobs: Jobs = Jobs()
    interface_defaults: list[InterfaceDefault] = []


class ConfigError(Exception):
    pass


def parse_settings(data: Optional[dict]) -> Settings:
    try:
        return Settings.model_validate(data or {})
    except ValidationError as exc:
        lines = [f"  {'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()]
        raise ConfigError("invalid configuration:\n" + "\n".join(lines)) from None


def load_settings(path: Optional[str] = None) -> tuple[Settings, str]:
    """(settings, source): DISCOBOX_CONFIG, else ./discobox.yaml if present, else the defaults."""
    explicit = path or os.getenv("DISCOBOX_CONFIG")
    path = explicit or "discobox.yaml"
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        if explicit:
            raise ConfigError(f"config file not found: {path}") from None
        return parse_settings({}), "defaults"
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: {exc}") from None
    if data is not None and not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    return parse_settings(data), path


def env_flag(name: str, default: bool = True) -> bool:
    return os.getenv(name, "true" if default else "false").lower() != "false"


_SECRET_ENV = ("NETBOX_TOKEN", "NETDISCO_TOKEN", "NETDISCO_PASSWORD", "DISCOBOX_AUTH_TOKEN")
_PLAIN_ENV = ("NETBOX_URL", "NETBOX_TLS_VERIFY", "NETDISCO_URL", "NETDISCO_USERNAME", "NETDISCO_TLS_VERIFY",
              "DISCOBOX_CONFIG", "DISCOBOX_STATE_DIR", "DISCOBOX_LISTEN_PORT")


def effective(settings: Settings, source: str) -> dict:
    """The settings as JSON for GET /config, secrets masked."""
    data = settings.model_dump(by_alias=True)
    if data["auth"].get("token"):
        data["auth"]["token"] = "***"
    env = {k: os.environ[k] for k in _PLAIN_ENV if k in os.environ}
    env.update({k: "***" for k in _SECRET_ENV if os.environ.get(k)})
    return {"source": source, "env": env, "settings": data}
