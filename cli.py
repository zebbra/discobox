#!/usr/bin/env python3
"""
discobox CLI — one-shot sync for a single device.

Usage:
    python cli.py --host 10.0.0.1
    python cli.py --host 10.0.0.1 --no-mac --debug
"""

import argparse
import logging
import os
import sys

from discobox import NetboxClient, NetdiscoClient, sync_device, validate_ip


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync a device from Netdisco into Netbox (device must already exist in Netbox)."
    )
    parser.add_argument(
        "--host", metavar="IP", required=True,
        help="Management IP of the device to sync.",
    )
    parser.add_argument("--no-mac",      action="store_true", help="Skip MAC address sync.")
    parser.add_argument("--no-ip",       action="store_true", help="Skip IP address sync.")
    parser.add_argument("--no-modules",  action="store_true", help="Skip module bay/module sync.")
    parser.add_argument("--no-sfp",      action="store_true", help="Skip SFP inventory item sync.")
    parser.add_argument("--housekeeping", action="store_true",
                        help="Remove stale device bays and empty dummy interfaces.")
    parser.add_argument("--debug",       action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    try:
        ip = validate_ip(args.host)
    except ValueError as exc:
        logging.getLogger("discobox").error("%s", exc)
        sys.exit(1)

    nd = NetdiscoClient(
        base_url=os.environ["NETDISCO_URL"],
        username=os.environ["NETDISCO_USERNAME"],
        password=os.environ["NETDISCO_PASSWORD"],
        verify_tls=os.getenv("NETDISCO_TLS_VERIFY", "true").lower() != "false",
    )
    nb = NetboxClient(
        url=os.environ["NETBOX_URL"],
        token=os.environ["NETBOX_TOKEN"],
        verify_tls=os.getenv("NETBOX_TLS_VERIFY", "true").lower() != "false",
        change_reason="DiscoBox CLI",
    )

    ok = sync_device(
        ip, nd, nb,
        sync_mac=not args.no_mac,
        sync_ip=not args.no_ip,
        sync_modules=not args.no_modules,
        sync_sfp=not args.no_sfp,
        housekeeping=args.housekeeping,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
