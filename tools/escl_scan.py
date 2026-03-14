"""
Reusable helpers for talking to an eSCL-compatible scanner.

The original CLI entry point lives in ``_escl-scan.py`` – this module holds the
pieces that are easier to reuse from tests or other Python code.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
import urllib3
from lxml import etree
from urllib3.exceptions import InsecureRequestWarning

DEF_NAME = "scan"
SIZES: dict[str, Sequence[int]] = {
    "a4": (2480, 3508),
    "a5": (1748, 2480),
    "b5": (2079, 2953),
    "us": (2550, 3300),
}
MAX_POLL = 50
NS_SCAN = "http://schemas.hp.com/imaging/escl/2011/05/03"
NS_PWG = "http://www.pwg.org/schemas/2010/12/sm"
SCAN_REQUEST_TEMPLATE = """
<?xml version="1.0" encoding="UTF-8"?>
<scan:ScanSettings xmlns:pwg="{ns_pwg}" xmlns:scan="{ns_scan}">
  <pwg:Version>{version}</pwg:Version>
  <pwg:ScanRegions>
    <pwg:ScanRegion>
      <pwg:XOffset>0</pwg:XOffset>
      <pwg:YOffset>0</pwg:YOffset>
      <pwg:Width>{width}</pwg:Width>
      <pwg:Height>{height}</pwg:Height>
      <pwg:ContentRegionUnits>escl:ThreeHundredthsOfInches</pwg:ContentRegionUnits>
    </pwg:ScanRegion>
  </pwg:ScanRegions>
  <pwg:InputSource>Platen</pwg:InputSource>
  <pwg:DocumentFormat>{document_format}</pwg:DocumentFormat>
  <scan:ColorMode>{color_mode}</scan:ColorMode>
  <scan:XResolution>{resolution}</scan:XResolution>
  <scan:YResolution>{resolution}</scan:YResolution>
</scan:ScanSettings>
""".strip()


class ESCLScanError(RuntimeError):
    """Raised when the scanner interaction fails."""


@dataclass
class ScannerCapabilities:
    version: str
    make_and_model: str | None
    serial_number: str | None
    admin_uri: str | None
    formats: list[str]
    color_modes: list[str]
    x_resolutions: list[str]
    y_resolutions: list[str]
    max_width: int
    max_height: int


def first(seq: Sequence[str], default: str | None = None) -> str | None:
    return seq[0] if seq else default


def first_int(seq: Sequence[str], default: int | None = None) -> int | None:
    value = first(seq)
    return int(value) if value is not None else default


def ensure_http_url(url: str) -> str:
    if url.startswith(("http://", "https://")):
        return url
    raise ESCLScanError(f"Invalid scanner URL (expected http/https): {url}")


def fetch_capabilities(session: requests.Session, base_url: str) -> ScannerCapabilities:
    ensure_http_url(base_url)
    capabilities_url = urljoin(base_url, "eSCL/ScannerCapabilities")
    response = session.get(capabilities_url)
    response.raise_for_status()
    tree = etree.fromstring(response.content)

    namespaces = {"pwg": NS_PWG, "scan": NS_SCAN}
    return ScannerCapabilities(
        version=first(tree.xpath("//pwg:Version/text()", namespaces=namespaces)) or "",
        make_and_model=first(tree.xpath("//pwg:MakeAndModel/text()", namespaces=namespaces)),
        serial_number=first(tree.xpath("//pwg:SerialNumber/text()", namespaces=namespaces)),
        admin_uri=first(tree.xpath("//scan:AdminURI/text()", namespaces=namespaces)),
        formats=tree.xpath("//pwg:DocumentFormat/text()", namespaces=namespaces),
        color_modes=tree.xpath("//scan:ColorMode/text()", namespaces=namespaces),
        x_resolutions=tree.xpath("//scan:XResolution/text()", namespaces=namespaces),
        y_resolutions=tree.xpath("//scan:YResolution/text()", namespaces=namespaces),
        max_width=first_int(tree.xpath("//scan:MaxWidth/text()", namespaces=namespaces)) or 0,
        max_height=first_int(tree.xpath("//scan:MaxHeight/text()", namespaces=namespaces)) or 0,
    )


def fetch_status(session: requests.Session, base_url: str) -> str:
    ensure_http_url(base_url)
    status_url = urljoin(base_url, "eSCL/ScannerStatus")
    response = session.get(status_url)
    response.raise_for_status()
    tree = etree.fromstring(response.content)
    namespaces = {"pwg": NS_PWG}
    status = first(tree.xpath("//pwg:State/text()", namespaces=namespaces))
    if not status:
        raise ESCLScanError("Scanner did not report status")
    return status


def build_scan_request(
    *,
    version: str,
    document_format: str,
    color_mode: str,
    resolution: str,
    width: int,
    height: int,
) -> str:
    return SCAN_REQUEST_TEMPLATE.format(
        ns_scan=NS_SCAN,
        ns_pwg=NS_PWG,
        version=version,
        document_format=document_format,
        color_mode=color_mode,
        resolution=resolution,
        width=width,
        height=height,
    )


def start_scan(session: requests.Session, base_url: str, scan_request_xml: str) -> str:
    ensure_http_url(base_url)
    start_url = urljoin(base_url, "eSCL/ScanJobs")
    response = session.post(start_url, scan_request_xml, headers={"Content-Type": "text/xml"})
    response.raise_for_status()
    location = response.headers.get("Location")
    if not location:
        raise ESCLScanError("Scan job missing Location header")
    return urljoin(f"{location}/", "NextDocument")


def poll_for_document(
    session: requests.Session,
    result_url: str,
    *,
    max_poll: int = MAX_POLL,
    poll_interval: float = 2.0,
) -> bytes:
    for _attempt in range(1, max_poll + 1):
        response = session.get(result_url)
        if response.status_code == 200:
            return response.content
        time.sleep(poll_interval)
    raise ESCLScanError(f"Giving up after {max_poll} attempts to load result from {result_url}")


def size_dimensions(size_key: str, *, max_width: int, max_height: int) -> Sequence[int]:
    if size_key == "max":
        return max_width, max_height
    if size_key not in SIZES:
        raise ESCLScanError(f"Unknown paper size: {size_key}")
    width, height = SIZES[size_key]
    if width > max_width or height > max_height:
        raise ESCLScanError(f"Requested paper size {size_key} exceeds scanner limits")
    return width, height


def resolve_resolution(requested: str, *, options_x: Iterable[str], options_y: Iterable[str]) -> str:
    if requested:
        if requested not in options_x or requested not in options_y:
            raise ESCLScanError(f"Unsupported resolution '{requested}'. X options: {options_x}, Y options: {options_y}")
        return requested
    intersection = [value for value in options_x if value in options_y]
    if not intersection:
        raise ESCLScanError("Scanner reports no matching X/Y resolution")
    return max(intersection, key=int)


def resolve_color_mode(requested: str) -> str:
    lookup = {"r24": "RGB24", "g8": "Grayscale8"}
    if requested not in lookup:
        raise ESCLScanError(f"Invalid color mode: {requested}")
    return lookup[requested]


def resolve_format(requested: str) -> str:
    lookup = {"jpg": "image/jpeg", "pdf": "application/pdf"}
    if requested not in lookup:
        raise ESCLScanError(f"Invalid output format: {requested}")
    return lookup[requested]


def create_session(*, verify: bool = True) -> requests.Session:
    session = requests.Session()
    session.verify = verify
    if not verify:
        urllib3.disable_warnings(InsecureRequestWarning)
    return session


def scan_document(
    *,
    base_url: str,
    output_path: str,
    document_type: str,
    color_mode: str,
    resolution: str,
    size_key: str,
    session: requests.Session | None = None,
    logger: logging.Logger | None = None,
) -> str:
    session = session or create_session()
    logger = logger or logging.getLogger("scan")

    capabilities = fetch_capabilities(session, base_url)
    status = fetch_status(session, base_url)
    logger.debug("Scanner status: %s", status)
    if status != "Idle":
        raise ESCLScanError(f"Invalid scanner status: {status}")

    document_format = resolve_format(document_type)
    if document_format not in capabilities.formats:
        raise ESCLScanError(f"Unsupported format '{document_format}', supported: {capabilities.formats}")

    requested_color_mode = resolve_color_mode(color_mode)
    if requested_color_mode not in capabilities.color_modes:
        raise ESCLScanError(f"Unsupported color mode '{requested_color_mode}', supported: {capabilities.color_modes}")

    selected_resolution = resolve_resolution(
        resolution, options_x=capabilities.x_resolutions, options_y=capabilities.y_resolutions
    )

    width, height = size_dimensions(size_key, max_width=capabilities.max_width, max_height=capabilities.max_height)

    scan_request = build_scan_request(
        version=capabilities.version,
        document_format=document_format,
        color_mode=requested_color_mode,
        resolution=selected_resolution,
        width=width,
        height=height,
    )

    result_url = start_scan(session, base_url, scan_request)
    logger.debug("Scan result URL: %s", result_url)
    document_bytes = poll_for_document(session, result_url)
    with open(output_path, "wb") as handle:
        handle.write(document_bytes)
    logger.info("Wrote %s", output_path)
    return output_path


def parse_env_file(path: str) -> dict[str, str]:
    """
    Parse a simple ``.env`` style file into a dictionary.

    We keep it tiny to avoid an additional dependency.
    """
    data: dict[str, str] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            data[key.strip()] = value.strip()
    return data


def load_scanner_urls(env_path: str, *, key: str = "SCANNER_URLS") -> list[str]:
    env = parse_env_file(env_path)
    raw_value = env.get(key, "")
    urls = [value.strip() for value in raw_value.split(",") if value.strip()]
    return [value if value.startswith(("http://", "https://")) else f"http://{value}" for value in urls]


def list_jobs(session: requests.Session, base_url: str) -> list[tuple[str, str]]:
    ensure_http_url(base_url)
    status_url = urljoin(base_url, "eSCL/ScannerStatus")
    response = session.get(status_url)
    response.raise_for_status()
    tree = etree.fromstring(response.content)
    namespaces = {"scan": NS_SCAN, "pwg": NS_PWG}
    jobs = []
    for node in tree.xpath("//scan:Jobs/scan:JobInfo", namespaces=namespaces):
        uri = first(node.xpath("./pwg:JobUri/text()", namespaces=namespaces)) or ""
        state = first(node.xpath("./pwg:JobState/text()", namespaces=namespaces)) or ""
        jobs.append((uri, state))
    return jobs
