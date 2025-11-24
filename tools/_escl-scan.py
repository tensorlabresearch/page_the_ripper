#!/usr/bin/env python3
#
# License:
#   MIT
# Author:
#   diitoo
# Inspired by:
# - https://bugs.launchpad.net/hplip/+bug/1811504
# - https://github.com/kno10/python-scan-eSCL
# - https://github.com/ziman/scan-eSCL/
# - http://testcluster.blogspot.com/2014/03/scanning-from-escl-device-using-command.html

import argparse
import datetime
import logging
import os.path
import sys

import requests

from .escl_scan import (
    DEF_NAME,
    ESCLScanError,
    create_session,
    fetch_capabilities,
    fetch_status,
    scan_document,
)


def main(args):
    logging.basicConfig(level=(logging.INFO, logging.DEBUG)[args.verbose or args.very_verbose])
    logging.getLogger("requests").setLevel(logging.WARN)
    log = logging.getLogger("scan")

    output_path = args.out or f"{DEF_NAME}_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.{args.type}"
    if not args.info and os.path.isfile(output_path):
        return error(f"File exists already: {output_path}")

    verify = args.verify_tls if args.verify_tls is not None else False
    session = create_session(verify=verify)

    try:
        capabilities = fetch_capabilities(session, args.url)
        status = fetch_status(session, args.url)
    except (requests.RequestException, ESCLScanError) as exc:
        return error(str(exc))

    if args.info:
        print(f"Scanner model: {capabilities.make_and_model}")
        print(f"Serial number: {capabilities.serial_number}")
        print(f"Scanner URL:   {args.url}")
        print(f"Admin URL:     {capabilities.admin_uri}")
        print(f"Formats:       {', '.join(capabilities.formats)}")
        print(f"Color Modes:   {', '.join(capabilities.color_modes)}")
        print(f"X-Resolutions: {', '.join(capabilities.x_resolutions)}")
        print(f"Y-Resolutions: {', '.join(capabilities.y_resolutions)}")
        print(f"Max width:     {capabilities.max_width}")
        print(f"Max height:    {capabilities.max_height}")
        print(f"Status:        {status}")
        return 0

    try:
        scan_document(
            base_url=args.url,
            output_path=output_path,
            document_type=args.type,
            color_mode=args.color_mode,
            resolution=args.resolution,
            size_key=args.size,
            session=session,
            logger=log,
        )
    except (requests.RequestException, ESCLScanError) as exc:
        return error(str(exc))

    print(output_path)
    return 0


def error(msg):
    print(msg)
    return 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="A little Python3 script for scanning via the eSCL protocol")
    ap.add_argument("-i", "--info", action="store_true", help="show scanner information and exit")
    ap.add_argument(
        "-o",
        "--out",
        default="",
        help="output file name [default: " + DEF_NAME + "_<datetime>.<type>]",
    )
    ap.add_argument(
        "-t",
        "--type",
        default="jpg",
        help="desired resulting file type [default: %(default)s]",
        choices=["jpg", "pdf"],
    )
    ap.add_argument(
        "-r",
        "--resolution",
        default="",
        help="a single value for both X and Y resolution [default: max. available]",
    )
    ap.add_argument(
        "-c",
        "--color-mode",
        default="r24",
        help="RGB24 (r24) or Grayscale8 (g8) [default: %(default)s]",
        choices=["r24", "g8"],
    )
    ap.add_argument(
        "-s",
        "--size",
        default="max",
        help="size of scanned paper [default: %(default)s]",
        choices=["a4", "a5", "b5", "us", "max"],
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Show debug output")
    ap.add_argument("-V", "--very-verbose", action="store_true", help="Show debug output and all data")
    ap.add_argument(
        "--verify-tls",
        action="store_true",
        help="Verify TLS certificates when contacting the scanner",
    )
    ap.add_argument("url", help="URL of the scanner, incl. scheme and (if necessary) port")
    sys.exit(main(ap.parse_args()))
