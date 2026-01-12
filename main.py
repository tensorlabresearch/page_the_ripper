import configparser
import io
import os
import json
import math
import time
import shutil
import base64
import tempfile
import subprocess
import sqlite3
import threading
import queue
import uuid
import shlex
import platform
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set, Callable, Union, Any, TypeVar
from urllib.parse import urljoin

import img2pdf
import requests
import numpy as np
import ocrmypdf
from ocrmypdf.exceptions import ExitCodeException, MissingDependencyError
from PIL import Image, ImageOps
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import pytesseract
from fastapi import FastAPI, HTTPException, Response, Query
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

# Cap Tesseract threading to avoid runaway resource usage.
os.environ.setdefault("OMP_THREAD_LIMIT", "2")

from tools import escl_scan

try:
    import psutil  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psutil = None

# ====================== CONFIG ======================
CONFIG_PATH = Path(os.getenv("SCANNER_CFG", "scanner.cfg"))
DEFAULT_CONFIG: Dict[str, Dict[str, str]] = {
    "defaults": {
        "dpi": "600",
        "color_mode": "Grayscale8",
        "verify_ssl": "false",
        "target_width": "0",
        "target_height": "0",
        "output_dir": "scans",
    },
    "scanner:et3850": {
        "label": "ET-3850",
        "backend": "sane",
        "sane_hint": "ET-3850",
        "command": "",
        "source": "Flatbed",
        "duplex": "false",
        "extra_args": "",
        "color_mode": "Grayscale8",
    },
    "scanner:es580w": {
        "label": "ES-580W",
        "backend": "sane",
        "sane_hint": "ES-580W",
        "command": "",
        "source": "ADF Duplex",
        "duplex": "true",
        "extra_args": "",
        "color_mode": "Grayscale8",
        "page_width_mm": "215.9",
        "page_height_mm": "279.4",
    },
}

CONFIG = configparser.ConfigParser()
CONFIG.read_dict(DEFAULT_CONFIG)
if CONFIG_PATH.exists():
    CONFIG.read(CONFIG_PATH)

# Environment helpers
TRUTHY = {"1", "true", "yes", "on"}
STAGE_STATES = {
    "queued",
    "scanning",
    "assembling",
    "ocr",
    "merging",
    "reducing",
    "finalizing",
    "completed",
    "cancelled",
    "failed",
    "deleted",
}


def env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        env_path = Path(".env")
        if env_path.exists():
            try:
                env_data = escl_scan.parse_env_file(str(env_path))
                value = env_data.get(name)
            except Exception:
                value = None
    if value is None:
        return default
    return value.strip().lower() in TRUTHY


def _cfg_get(section: str, option: str, fallback: str) -> str:
    return CONFIG.get(section, option, fallback=fallback)


def _cfg_get_int(section: str, option: str, fallback: int) -> int:
    try:
        return CONFIG.getint(section, option, fallback=fallback)
    except ValueError:
        return fallback


def _cfg_get_bool(section: str, option: str, fallback: bool) -> bool:
    try:
        return CONFIG.getboolean(section, option, fallback=fallback)
    except ValueError:
        return fallback


def _cfg_get_float(section: str, option: str, fallback: float) -> float:
    try:
        return CONFIG.getfloat(section, option, fallback=fallback)
    except ValueError:
        return fallback


DPI = _cfg_get_int("defaults", "dpi", 300)
COLOR_MODE = _cfg_get("defaults", "color_mode", "Grayscale8")
if COLOR_MODE not in {"Grayscale8", "RGB24"}:
    COLOR_MODE = "Grayscale8"

TARGET_WIDTH = _cfg_get_int("defaults", "target_width", 405)
TARGET_HEIGHT = _cfg_get_int("defaults", "target_height", 636)
OUTPUT_DIR = Path(os.getenv("SCAN_OUTPUT_DIR", _cfg_get("defaults", "output_dir", "scans"))).expanduser()

DEBUG_KEEP_SANE_RAW = env_flag("SCAN_DEBUG_KEEP_SANE_RAW", False)
DEBUG_RAW_DIR = Path(os.getenv("SCAN_DEBUG_RAW_DIR", str(OUTPUT_DIR / "debug_raw"))).expanduser()


@dataclass
class ScanResult:
    pages: List[Image.Image]
    raw_dir: Path
    raw_paths: List[Path]

# ---------------- OCR defaults ----------------
TESSERACT_LANG = os.environ.get("TESSERACT_LANG", "eng")

# eSCL endpoints and constants
ESCL_CAPS = "/eSCL/ScannerCapabilities"
ESCL_SCANJOBS = "/eSCL/ScanJobs"
ESCL_NEXTDOC_TMPL = "/eSCL/ScanJobs/{job}/NextDocument"

# PWG media keyword for US Letter
PWG_LETTER = "na_letter_8.5x11in"

# Timeouts / polling
HTTP_TIMEOUT = 30
DOC_POLL_SLEEP = 0.5
MAX_DOCS = 999
# TLS verification (legacy eSCL path)
VERIFY_SSL = env_flag("SCANNER_VERIFY_SSL", _cfg_get_bool("defaults", "verify_ssl", False))
SCAN_TIMEOUT = int(os.getenv("SCAN_TIMEOUT", "240"))
CANCEL_WAIT_SECONDS = int(os.getenv("SCAN_CANCEL_WAIT", "30"))
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "deleted"}


def create_escl_session(verify_ssl: Optional[bool] = None, auth: Optional[Tuple[str, str]] = None) -> requests.Session:
    if verify_ssl is None:
        verify_ssl = VERIFY_SSL
    session = requests.Session()
    session.verify = verify_ssl
    if not verify_ssl:
        urllib3.disable_warnings(InsecureRequestWarning)
    if auth and auth[0] and auth[1]:
        session.auth = requests.auth.HTTPBasicAuth(auth[0], auth[1])
    return session


# ---------------- eSCL helpers (no external tools) ----------------
def escl_post_scan_job(base_url: str,
                       input_source: str,
                       dpi: int,
                       color_mode: str,
                       media_name: Optional[str],
                       session: Optional[requests.Session] = None,
                       scan_region: Optional[Tuple[int, int]] = None) -> Tuple[str, str]:
    """
    Create an eSCL ScanJob and return (job_url, next_document_url).
    input_source: "Platen", "Adf", or "AdfDuplex"
    color_mode: "Color" or "Gray"
    media_name: e.g. "na_letter_8.5x11in" (None to let device auto-size)
    """
    # eSCL uses XML; keep settings minimal + broadly compatible
    # Namespaces commonly accepted by devices
    # - Version is often PWG 2.1, but many ignore it.
    media_block = f"""
        <pwg:MediaSize>
            <pwg:Name>{media_name}</pwg:Name>
        </pwg:MediaSize>
    """ if media_name else ""

    region_block = ""
    if scan_region:
        width, height = scan_region
        region_block = f"""
  <pwg:ScanRegions>
    <pwg:ScanRegion>
      <pwg:XOffset>0</pwg:XOffset>
      <pwg:YOffset>0</pwg:YOffset>
      <pwg:Width>{width}</pwg:Width>
      <pwg:Height>{height}</pwg:Height>
      <pwg:ContentRegionUnits>scan:ThreeHundredthsOfInches</pwg:ContentRegionUnits>
    </pwg:ScanRegion>
  </pwg:ScanRegions>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<scan:ScanSettings xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:scan="http://schemas.hp.com/imaging/escl/2011/05/03"
    xmlns:pwg="http://www.pwg.org/schemas/2010/12/sm">
  <pwg:Version>2.1</pwg:Version>
  <scan:Intent>Document</scan:Intent>
  <scan:InputSource>{input_source}</scan:InputSource>
  <scan:DocumentFormat>image/jpeg</scan:DocumentFormat>
  <scan:ColorMode>{color_mode}</scan:ColorMode>
  <scan:Resolution>
    <scan:XResolution>{dpi}</scan:XResolution>
    <scan:YResolution>{dpi}</scan:YResolution>
  </scan:Resolution>
  {region_block}
  {media_block}
</scan:ScanSettings>
""".strip()

    session = session or create_escl_session()
    url = base_url.rstrip("/") + ESCL_SCANJOBS
    r = session.post(url, data=xml.encode("utf-8"),
                     headers={"Content-Type": "text/xml"},
                     timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    # Many devices return Location header with job path
    loc = r.headers.get("Location")
    if not loc:
        # Fallback: some return a minimal body with job link — try to guess job id
        # Often it’s the last path segment appended to ScanJobs
        loc = r.headers.get("location")  # try lowercase
    if not loc:
        # Best-effort: ask capabilities for JobUri? Most support Location, though.
        raise RuntimeError("Scanner did not return a Location header for the new job.")

    if loc.startswith("/"):
        job_url = base_url.rstrip("/") + loc
    elif loc.startswith("http"):
        job_url = loc
    else:
        job_url = base_url.rstrip("/") + "/" + loc.lstrip("/")

    job_id = job_url.rstrip("/").split("/")[-1]
    nextdoc_url = base_url.rstrip("/") + ESCL_NEXTDOC_TMPL.format(job=job_id)
    return job_url, nextdoc_url


def escl_fetch_documents(nextdoc_url: str,
                         session: Optional[requests.Session] = None) -> List[bytes]:
    """
    Pull every document (page image) for the job until the device stops serving pages.
    Returns list of JPEG bytes.
    """
    out: List[bytes] = []
    session = session or create_escl_session()
    for _ in range(MAX_DOCS):
        resp = session.get(nextdoc_url, timeout=HTTP_TIMEOUT)
        if resp.status_code in (200, 201):
            out.append(resp.content)
            # some models need a short pause between pulls
            time.sleep(DOC_POLL_SLEEP)
            continue
        # 204 No Content, 404 Not Found, or 410 Gone usually mean "no more pages"
        if resp.status_code in (204, 404, 410):
            break
        # transient?
        if resp.status_code == 503:
            time.sleep(DOC_POLL_SLEEP)
            continue
        resp.raise_for_status()
    return out


# ---------------- SANE helpers ----------------
def list_sane_devices() -> List[Tuple[str, str]]:
    """Return all devices reported by SANE as (id, description)."""
    proc = subprocess.run(
        ["scanimage", "-L"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode not in {0}:
        stderr = proc.stderr.strip()
        raise RuntimeError(f"scanimage -L failed ({proc.returncode}): {stderr or proc.stdout.strip()}")

    devices: List[Tuple[str, str]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("device `"):
            continue
        try:
            device_part, rest = line.split("`", 1)[1].split("'", 1)
        except ValueError:
            continue
        description = rest.split(" is a ", 1)[-1].strip()
        devices.append((device_part.strip(), description))
    return devices


def resolve_sane_device(explicit: str, hint: str) -> str:
    explicit = explicit.strip()
    if explicit:
        return explicit
    hint_lower = hint.strip().lower()
    if not hint_lower:
        raise RuntimeError("A SANE device hint is required when no explicit device is configured")
    for device_id, description in list_sane_devices():
        if hint_lower in device_id.lower() or hint_lower in description.lower():
            return device_id
    raise RuntimeError(f"Unable to locate SANE device matching hint '{hint}'")


def sane_scan_to_directory(
    options: Dict[str, object],
    *,
    dpi: int,
    color_mode: str,
    output_dir: Path,
    progress_cb: Optional[Callable[[str], None]] = None,
    job_entry: Optional[Dict[str, object]] = None,
) -> List[Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    existing = list(output_dir.glob("page-*.png"))
    if existing:
        raise RuntimeError(f"Output directory {output_dir} already contains page-*.png files; refusing to overwrite")

    sane_device = str(options.get("sane_device", ""))
    sane_hint = str(options.get("sane_hint", "")) or "Scanner"
    command_template = str(options.get("command", ""))
    extra_args = str(options.get("extra_args", ""))
    source = str(options.get("source", ""))
    duplex = bool(options.get("duplex", False))
    page_width_mm = float(options.get("page_width_mm") or 0.0)
    page_height_mm = float(options.get("page_height_mm") or 0.0)

    device_id = resolve_sane_device(sane_device, sane_hint)
    sane_mode = "Color" if color_mode == "RGB24" else "Gray"
    if sane_mode == "Gray" and "epsonscan2:" in device_id:
        # epson's epsonscan2 backend expects "Grayscale" instead of "Gray"
        sane_mode = "Grayscale"

    batch_pattern = output_dir / "page-%03d.png"

    def execute_scan_command(command: Union[str, List[str]], *, shell: bool) -> Tuple[str, str]:
        if progress_cb:
            progress_cb("scanning - invoking scanimage")
        start_time = time.time()
        proc = subprocess.Popen(
            command,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if job_entry is not None:
            job_entry["process"] = proc
        stdout_parts: List[str] = []
        stderr_parts: List[str] = []
        try:
            while True:
                try:
                    stdout, stderr = proc.communicate(timeout=1)
                    stdout_parts.append(stdout or "")
                    stderr_parts.append(stderr or "")
                    break
                except subprocess.TimeoutExpired:
                    if job_entry and job_entry.get("cancel_requested"):
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        raise ScanCancelled("Scan cancelled")
                    if time.time() - start_time > SCAN_TIMEOUT:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        raise RuntimeError(f"scanimage timed out after {SCAN_TIMEOUT} seconds")
        finally:
            if job_entry is not None:
                job_entry["process"] = None
        stdout_text = "".join(stdout_parts)
        stderr_text = "".join(stderr_parts)
        if proc.returncode != 0:
            msg = stderr_text.strip() or stdout_text.strip() or "scanimage failed"
            raise RuntimeError(f"scanimage failed ({proc.returncode}): {msg}")
        return stdout_text.strip(), stderr_text.strip()

    print(f"[sane_scan_to_directory] device={device_id} hint={sane_hint} dpi={dpi} mode={sane_mode} dest={batch_pattern}")

    if command_template:
        command = command_template.format(
            device=device_id,
            dpi=dpi,
            sane_mode=sane_mode,
            mode=sane_mode,
            color_mode=color_mode,
            batch_pattern=str(batch_pattern),
            batch_dir=str(output_dir),
        )
        print(f"[sane_scan_to_directory] executing template command: {command}")
        stdout, stderr = execute_scan_command(command, shell=True)
    else:
        args = [
            "scanimage",
            f"--device={device_id}",
            f"--resolution={dpi}",
            f"--mode={sane_mode}",
            "--format=png",
            f"--batch={batch_pattern}",
        ]
        if source:
            args.append(f"--source={source}")
        if extra_args:
            args.extend(shlex.split(extra_args))
        if (not duplex) and ("--batch-count" not in extra_args) and (not source.lower().startswith("adf")):
            args.append("--batch-count=1")
        if page_width_mm > 0:
            args.extend(["-x", f"{page_width_mm:g}"])
        if page_height_mm > 0:
            args.extend(["-y", f"{page_height_mm:g}"])
        cmd_display = " ".join(shlex.quote(part) for part in args)
        print(f"[sane_scan_to_directory] executing command: {cmd_display}")
        stdout, stderr = execute_scan_command(args, shell=False)

    if stdout:
        print(f"[sane_scan_to_directory] stdout:\n{stdout}")
    if stderr:
        print(f"[sane_scan_to_directory] stderr:\n{stderr}")

    png_files = sorted(output_dir.glob("page-*.png"))
    if not png_files:
        raise RuntimeError("scanimage produced no pages")
    print(f"[sane_scan_to_directory] captured {len(png_files)} page(s) into {output_dir}")
    if progress_cb:
        progress_cb(f"scanning - captured {len(png_files)} page(s)")
    return png_files


def preserve_sane_raw_pages(png_files: List[Path], *, job_id: Optional[str]) -> Optional[Path]:
    if not DEBUG_KEEP_SANE_RAW or not png_files:
        return None
    dest_root = DEBUG_RAW_DIR
    dest_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = (job_id or f"sane_{timestamp}").replace("/", "_")
    dest_dir = dest_root / safe_name
    if dest_dir.exists():
        suffix = 1
        while True:
            candidate = dest_root / f"{safe_name}_{suffix:02d}"
            if not candidate.exists():
                dest_dir = candidate
                break
            suffix += 1
    dest_dir.mkdir(parents=True, exist_ok=True)
    for src in png_files:
        target = dest_dir / src.name
        shutil.copy2(src, target)
    print(f"[debug] preserved raw SANE pages at {dest_dir}")
    return dest_dir


def remove_debug_raw(job_id: str) -> None:
    if not DEBUG_RAW_DIR.exists():
        return
    prefix = job_id.replace("/", "_")
    for entry in DEBUG_RAW_DIR.iterdir():
        if entry.is_dir() and entry.name.startswith(prefix):
            shutil.rmtree(entry, ignore_errors=True)


def scan_with_sane(
    options: Dict[str, object],
    *,
    dpi: int,
    color_mode: str,
    processing_opts: Optional[Dict[str, object]],
    progress_cb: Optional[Callable[[str], None]] = None,
    job_entry: Optional[Dict[str, object]] = None,
    job_id: Optional[str] = None,
) -> ScanResult:
    tmpdir = Path(tempfile.mkdtemp(prefix="scanjob_"))
    try:
        png_files = sane_scan_to_directory(
            options,
            dpi=dpi,
            color_mode=color_mode,
            output_dir=tmpdir,
            progress_cb=progress_cb,
            job_entry=job_entry,
        )

        debug_dir = preserve_sane_raw_pages(png_files, job_id=job_id)
        if job_entry is not None and debug_dir is not None:
            job_entry["debug_raw_dir"] = str(debug_dir)

        finalized: List[Image.Image] = []
        for png_file in png_files:
            with Image.open(png_file) as pil:
                pil.load()
                finalized.append(finalize_page(pil, color_mode=color_mode, processing_opts=processing_opts))
        return ScanResult(pages=finalized, raw_dir=tmpdir, raw_paths=png_files)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
        print(f"[scan_with_sane] cleaned up {tmpdir}")


def collect_saned_logs(limit: int = 80) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["journalctl", "-u", "saned.socket", "-n", str(limit), "--no-pager"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    logs = proc.stdout.strip()
    return logs or None


def fetch_sane_backend_details(entry: Dict[str, object]) -> Dict[str, object]:
    options = dict(entry.get("options", {}))
    sane_device = str(options.get("sane_device", "") or "")
    sane_hint = str(options.get("sane_hint", "") or entry.get("label", ""))

    details: Dict[str, object] = {
        "status": "unknown",
        "configured": {
            "sane_device": sane_device or None,
            "sane_hint": sane_hint or None,
            "source": options.get("source") or None,
            "duplex": bool(options.get("duplex", False)),
            "command": options.get("command") or None,
            "extra_args": options.get("extra_args") or None,
        },
    }

    try:
        resolved_device = resolve_sane_device(sane_device, sane_hint)
        details["resolved_device"] = resolved_device
        details["status"] = "ok"
    except Exception as exc:
        details["status"] = "error"
        details["error"] = str(exc)
        return details

    try:
        proc = subprocess.run(
            ["scanimage", f"--device={resolved_device}", "-A"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=20,
        )
        if proc.returncode == 0:
            details["options_report"] = proc.stdout.strip()
        else:
            message = proc.stderr.strip() or proc.stdout.strip() or f"scanimage -A returned {proc.returncode}"
            details["options_report_error"] = message
    except FileNotFoundError:
        details["options_report_error"] = "scanimage command not available"
    except subprocess.TimeoutExpired:
        details["options_report_error"] = "scanimage -A timed out"

    logs = collect_saned_logs(limit=40)
    if logs:
        details["recent_logs"] = logs
    return details


def fetch_escl_backend_details(entry: Dict[str, object]) -> Dict[str, object]:
    url = str(entry.get("default_url") or "")
    auth = entry.get("auth")
    details: Dict[str, object] = {
        "status": "unknown",
        "default_url": url or None,
        "auth_configured": bool(auth and auth[0]),
    }
    if not url:
        details["status"] = "error"
        details["error"] = "No default_url configured for eSCL scanner"
        return details
    session = create_escl_session(auth=auth)
    try:
        caps = escl_scan.fetch_capabilities(session, url)
        details["capabilities"] = {
            "version": caps.version,
            "make_and_model": caps.make_and_model,
            "serial_number": caps.serial_number,
            "admin_uri": caps.admin_uri,
            "formats": caps.formats,
            "color_modes": caps.color_modes,
            "x_resolutions": caps.x_resolutions,
            "y_resolutions": caps.y_resolutions,
            "max_width": caps.max_width,
            "max_height": caps.max_height,
        }
    except Exception as exc:
        details["capabilities_error"] = str(exc)
    try:
        status = escl_scan.fetch_status(session, url)
        details["status"] = status.lower()
    except Exception as exc:
        details["status"] = "error"
        details["status_error"] = str(exc)
    return details


def gather_system_health() -> Dict[str, object]:
    severity_rank = {"ok": 0, "warning": 1, "error": 2}
    system_info = {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
    }

    uptime_seconds: Optional[float] = None
    if psutil is not None:
        try:
            uptime_seconds = max(0.0, time.time() - psutil.boot_time())
        except Exception:
            uptime_seconds = None
    if uptime_seconds is None:
        try:
            with open("/proc/uptime", "r", encoding="utf-8") as fp:
                uptime_seconds = float(fp.read().split()[0])
        except Exception:
            uptime_seconds = None

    memory_info: Optional[Dict[str, object]] = None
    if psutil is not None:
        try:
            mem = psutil.virtual_memory()
            memory_info = {
                "total_bytes": mem.total,
                "available_bytes": mem.available,
                "used_bytes": mem.used,
                "percent": mem.percent,
            }
        except Exception:
            memory_info = None
    if memory_info is None:
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            phys_pages = os.sysconf("SC_PHYS_PAGES")
            avail_pages = os.sysconf("SC_AVPHYS_PAGES")
            memory_info = {
                "total_bytes": int(phys_pages * page_size),
                "available_bytes": int(avail_pages * page_size),
            }
        except (ValueError, OSError, AttributeError):
            memory_info = None

    cpu_load: Optional[Dict[str, object]] = None
    try:
        load1, load5, load15 = os.getloadavg()  # type: ignore[attr-defined]
        cpu_load = {"load_1": load1, "load_5": load5, "load_15": load15}
    except (AttributeError, OSError):
        cpu_load = None

    resources = {
        "uptime_seconds": uptime_seconds,
        "cpu_count": os.cpu_count(),
        "cpu_load": cpu_load,
        "memory": memory_info,
    }

    components: Dict[str, object] = {}
    status_scores: List[int] = []

    def record(name: str, data: Dict[str, object]) -> None:
        status = str(data.get("status", "unknown")).lower()
        components[name] = data
        status_scores.append(severity_rank.get(status, 1))

    # Tesseract health
    try:
        version = pytesseract.get_tesseract_version()
        record("tesseract", {"status": "ok", "version": str(version)})
    except Exception as exc:
        record("tesseract", {"status": "error", "error": str(exc)})

    # img2pdf availability
    try:
        img2pdf_version = getattr(img2pdf, "__version__", None)
        payload = {"status": "ok"}
        if img2pdf_version:
            payload["version"] = img2pdf_version
        record("img2pdf", payload)
    except Exception as exc:
        record("img2pdf", {"status": "warning", "error": str(exc)})

    # OCRmyPDF availability
    try:
        ocrmypdf_version = getattr(ocrmypdf, "__version__", None)
        payload = {"status": "ok"}
        if ocrmypdf_version:
            payload["version"] = ocrmypdf_version
        record("ocrmypdf", payload)
    except Exception as exc:
        record("ocrmypdf", {"status": "error", "error": str(exc)})

    # saned health
    try:
        sane_devices = list_sane_devices()
        device_list = [{"id": dev_id, "description": desc} for dev_id, desc in sane_devices]
        status = "ok" if sane_devices else "warning"
        record("saned", {"status": status, "devices": device_list})
    except Exception as exc:
        record("saned", {"status": "error", "error": str(exc)})

    # eSCL scanners
    escl_entries: List[Dict[str, object]] = []
    escl_worst = 0
    for scanner_id, entry in SCANNER_REGISTRY.items():
        if entry.get("backend") != "escl":
            continue
        url = entry.get("default_url")
        auth = entry.get("auth")
        scanner_info: Dict[str, object] = {
            "id": scanner_id,
            "label": entry.get("label"),
            "url": url,
        }
        if not url:
            scanner_info.update({"status": "error", "error": "No default_url configured"})
            escl_worst = max(escl_worst, severity_rank["error"])
        else:
            session = create_escl_session(auth=auth)
            try:
                state = escl_scan.fetch_status(session, str(url)).lower()
                scanner_info["reported_state"] = state
                if state in {"idle", "ready"}:
                    scanner_info["status"] = "ok"
                    escl_worst = max(escl_worst, severity_rank["ok"])
                else:
                    scanner_info["status"] = "warning"
                    escl_worst = max(escl_worst, severity_rank["warning"])
            except Exception as exc:
                scanner_info["status"] = "error"
                scanner_info["error"] = str(exc)
                escl_worst = max(escl_worst, severity_rank["error"])
        escl_entries.append(scanner_info)
    if escl_entries:
        status = next((name for name, rank in severity_rank.items() if rank == escl_worst), "warning")
        record("escl", {"status": status, "scanners": escl_entries})

    # job worker status
    with PENDING_LOCK:
        pending_count = len(PENDING_JOBS)
    try:
        running_jobs = list(JOB_WORKER.running_jobs.keys())
    except RuntimeError:
        running_jobs = []
    worker_status = "ok" if JOB_WORKER.is_alive() else "warning"
    record(
        "job_worker",
        {
            "status": worker_status,
            "is_alive": JOB_WORKER.is_alive(),
            "pending_jobs": pending_count,
            "queue_depth": JOB_QUEUE.qsize(),
            "running_jobs": running_jobs,
        },
    )

    # database status
    db_info = {
        "path": str(DB_PATH),
        "exists": DB_PATH.exists(),
    }
    if DB_PATH.exists():
        try:
            stat = DB_PATH.stat()
            db_info["size_bytes"] = stat.st_size
            db_info["last_modified"] = datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z"
            db_status = "ok"
        except OSError as exc:
            db_info["error"] = str(exc)
            db_status = "warning"
    else:
        db_status = "warning"
    record("job_database", {"status": db_status, **db_info})

    overall_level = max(status_scores) if status_scores else 1
    overall_status = next((name for name, rank in severity_rank.items() if rank == overall_level), "warning")

    return {
        "status": overall_status,
        "system": system_info,
        "resources": resources,
        "components": components,
    }


# ---------------- Image cleanup ----------------
def light_cleanup(pil_img: Image.Image, *, perform_crop: bool = False) -> Image.Image:
    """
    For ET-3850 (flatbed): deskew + smart trim around content.
    Uses OpenCV when present; otherwise returns original.
    The caller supply orientation-corrected image (color preserved). Rotation/deskew is intentionally disabled.
    """
    return pil_img


def trim_white_borders(pil_img: Image.Image, *, threshold: int = 245, padding: int = 6) -> Image.Image:
    gray = np.array(pil_img.convert("L"))
    adaptive_threshold = min(threshold, np.percentile(gray, 70))
    mask = gray < adaptive_threshold
    rows = np.where(mask.any(axis=1))[0]
    cols = np.where(mask.any(axis=0))[0]
    if rows.size == 0 or cols.size == 0:
        return pil_img
    top = max(int(rows[0]) - padding, 0)
    bottom = min(int(rows[-1]) + padding + 1, gray.shape[0])
    left = max(int(cols[0]) - padding, 0)
    right = min(int(cols[-1]) + padding + 1, gray.shape[1])
    if right - left <= 0 or bottom - top <= 0:
        return pil_img
    return pil_img.crop((left, top, right, bottom))


def detect_osd_rotation(pil_img: Image.Image, lang: str = TESSERACT_LANG) -> Optional[int]:
    try:
        osd = pytesseract.image_to_osd(pil_img.convert("L"), lang=lang, config="--psm 0")
    except pytesseract.TesseractError:
        return None
    rotation = None
    for line in osd.splitlines():
        if line.startswith("Rotate"):
            try:
                rotation = int(line.split(":", 1)[1].strip()) % 360
            except ValueError:
                rotation = None
            break
    if rotation is None or rotation % 360 == 0:
        return 0
    return rotation


# ---------------- OCR via Tesseract ----------------
TESSERACT_LANG = os.environ.get("TESSERACT_LANG", "eng")


def ocr_page(pil_img: Image.Image, lang: str = TESSERACT_LANG) -> str:
    text = pytesseract.image_to_string(pil_img.convert("RGB"), lang=lang)
    return " ".join(text.split())


def finalize_page(
    pil_img: Image.Image,
    *,
    color_mode: str,
    processing_opts: Optional[Dict[str, object]] = None,
) -> Image.Image:
    opts = processing_opts or {}
    # Cropping and rotation are intentionally disabled to preserve full pages.
    page = pil_img

    page = light_cleanup(page, perform_crop=False)
    fallback_page = page.copy()

    mode = "RGB" if color_mode == "RGB24" else "L"
    page = page.convert(mode)
    if TARGET_WIDTH > 0 and TARGET_HEIGHT > 0 and (page.width > TARGET_WIDTH or page.height > TARGET_HEIGHT):
        page = ImageOps.contain(page, (TARGET_WIDTH, TARGET_HEIGHT), method=Image.LANCZOS)

    # fallback if the processed page ended up mostly blank
    gray = np.array(page.convert("L"))
    content_ratio = (gray < 230).mean()
    if content_ratio < 0.01 or not pytesseract.image_to_string(page, lang=TESSERACT_LANG, config="--psm 6").strip():
        fallback = fallback_page.convert(mode)
        if TARGET_WIDTH > 0 and TARGET_HEIGHT > 0 and (fallback.width > TARGET_WIDTH or fallback.height > TARGET_HEIGHT):
            fallback = ImageOps.contain(fallback, (TARGET_WIDTH, TARGET_HEIGHT), method=Image.LANCZOS)
        page = fallback
    return page


# ---------------- PDF assembly ----------------
def create_pdf_from_images(pages: List[Image.Image], out_path: Path, *, dpi: Optional[int]) -> None:
    """
    Use img2pdf to stitch the provided PIL images into a single PDF.
    """
    effective_dpi = dpi or 300
    image_streams: List[bytes] = []
    for img in pages:
        buf = io.BytesIO()
        working = img.convert("RGB") if img.mode not in {"RGB", "L"} else img
        working.save(buf, format="PNG")
        image_streams.append(buf.getvalue())
    layout_fun = img2pdf.get_fixed_dpi_layout_fun((effective_dpi, effective_dpi))
    pdf_bytes = img2pdf.convert(image_streams, layout_fun=layout_fun)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(pdf_bytes)


def run_ocr_on_pdf(
    input_pdf: Path,
    output_pdf: Path,
    *,
    language: str = TESSERACT_LANG,
    image_dpi: Optional[int] = None,
) -> None:
    output_pdf = Path(output_pdf)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    effective_dpi = image_dpi or 300
    try:
        os.environ["OMP_THREAD_LIMIT"] = os.environ.get("OMP_THREAD_LIMIT", "2")
        ocrmypdf.ocr(
            str(input_pdf),
            str(output_pdf),
            language=language,
            deskew=True,
            clean=False,
            clean_final=False,
            rotate_pages=False,
            rotate_pages_threshold=None,
            optimize=0,
            progress_bar=False,
            force_ocr=True,
            image_dpi=effective_dpi,
            tesseract_timeout=120,
            jobs=1,
        )
    except ExitCodeException as exc:
        raise RuntimeError(f"ocrmypdf failed ({exc.exit_code}): {exc}") from exc
    except MissingDependencyError as exc:
        raise RuntimeError(f"ocrmypdf dependency missing: {exc}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"ocrmypdf timed out: {exc}") from exc


def reocr_pdf(
    input_pdf: Path,
    *,
    color_mode: str,
    dpi: int = 300,
    output_pdf: Optional[Path] = None,
) -> Path:
    input_pdf = input_pdf.resolve()
    if output_pdf is None:
        output_pdf = input_pdf
    else:
        output_pdf = output_pdf.resolve()
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pdf", prefix="reocr_")
    os.close(tmp_fd)
    tmp_tmp_pdf = Path(tmp_path)
    try:
        run_ocr_on_pdf(input_pdf, tmp_tmp_pdf, image_dpi=dpi)
        tmp_tmp_pdf.replace(output_pdf)
    finally:
        if tmp_tmp_pdf.exists():
            tmp_tmp_pdf.unlink(missing_ok=True)
    return output_pdf



# ---------------- Main scanning policy ----------------
def capture_es_580w_letter_duplex_raw(
    base_url: str,
    dpi: int,
    color_mode: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[bytes]:
    session = session or create_escl_session()
    try:
        existing_jobs = escl_scan.list_jobs(session, base_url)
    except (requests.HTTPError, requests.RequestException):
        existing_jobs = []
    for job_uri, job_state in existing_jobs:
        if not job_uri:
            continue
        cleanup_url = f"{base_url.rstrip('/')}/{job_uri.lstrip('/')}"
        try:
            session.delete(cleanup_url, timeout=HTTP_TIMEOUT)
        except requests.RequestException:
            continue
        time.sleep(0.5)
    caps = escl_scan.fetch_capabilities(session, base_url)
    for _ in range(10):
        try:
            status = escl_scan.fetch_status(session, base_url)
        except (requests.HTTPError, requests.RequestException):
            status = "Unknown"
        if status == "Idle" or status == "Unknown":
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"Scanner not ready (status: {status})")
    region = (caps.max_width or 2550, caps.max_height or 3300)

    job_url, nextdoc = escl_post_scan_job(
        base_url=base_url,
        input_source="AdfDuplex",
        dpi=dpi,
        color_mode=color_mode,
        media_name=PWG_LETTER,
        session=session,
        scan_region=region,
    )
    print(f"[capture_es_580w_letter_duplex_raw] job_url={job_url} nextdoc={nextdoc}")
    jpeg_pages = escl_fetch_documents(nextdoc, session=session)
    if not jpeg_pages:
        raise RuntimeError("ES-580W produced no pages (check ADF load).")
    return jpeg_pages


def capture_et_3850_platen_raw(
    base_url: str,
    dpi: int,
    color_mode: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[bytes]:
    session = session or create_escl_session()
    caps = escl_scan.fetch_capabilities(session, base_url)
    if color_mode not in caps.color_modes:
        raise RuntimeError(f"Requested color mode {color_mode} not supported: {caps.color_modes}")
    resolution = escl_scan.resolve_resolution(str(dpi), options_x=caps.x_resolutions, options_y=caps.y_resolutions)
    width = caps.max_width or 2550
    height = caps.max_height or 3510

    scan_request = escl_scan.build_scan_request(
        version=caps.version,
        document_format="image/jpeg",
        color_mode=color_mode,
        resolution=resolution,
        width=width,
        height=height,
    )
    result_url = None
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            result_url = escl_scan.start_scan(session, base_url, scan_request)
            break
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 503 and attempt < max_attempts - 1:
                retry_after = exc.response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else 2.0
                for job_uri, job_state in escl_scan.list_jobs(session, base_url):
                    if not job_uri:
                        continue
                    cleanup_url = f"{base_url.rstrip('/')}/{job_uri.lstrip('/')}"
                    try:
                        session.delete(cleanup_url, timeout=HTTP_TIMEOUT)
                    except requests.RequestException:
                        continue
                time.sleep(delay)
                continue
            raise
    if not result_url:
        raise RuntimeError("Failed to start scan job after retries")
    jpeg_bytes = escl_scan.poll_for_document(session, result_url)
    return [jpeg_bytes]


def scan_es_580w_letter_duplex(base_url: str,
                               dpi: int,
                               color_mode: str,
                               session: Optional[requests.Session] = None,
                               processing_opts: Optional[Dict[str, object]] = None) -> List[Image.Image]:
    """
    ES-580W: force ADF duplex, Letter size, scan both sides.
    """
    jpeg_pages = capture_es_580w_letter_duplex_raw(base_url, dpi, color_mode, session=session)

    return [
        finalize_page(Image.open(io.BytesIO(b)), color_mode=color_mode, processing_opts=processing_opts)
        for b in jpeg_pages
    ]


def scan_et_3850_platen(base_url: str,
                        dpi: int,
                        color_mode: str,
                        session: Optional[requests.Session] = None,
                        processing_opts: Optional[Dict[str, object]] = None) -> List[Image.Image]:
    """
    ET-3850: scan from flatbed (platen), then deskew/trim.
    (Most devices only return one page for platen; we support more if available.)
    """
    jpeg_pages = capture_et_3850_platen_raw(base_url, dpi, color_mode, session=session)
    return [finalize_page(Image.open(io.BytesIO(jpeg_bytes)), color_mode=color_mode, processing_opts=processing_opts) for jpeg_bytes in jpeg_pages]


def build_scanner_registry() -> Dict[str, Dict[str, object]]:
    legacy_runners = {
        "et3850": scan_et_3850_platen,
        "es580w": scan_es_580w_letter_duplex,
    }
    registry: Dict[str, Dict[str, object]] = {}
    for section in CONFIG.sections():
        if not section.startswith("scanner:"):
            continue
        key = section.split(":", 1)[1]
        label = CONFIG.get(section, "label", fallback=key.upper())
        default_color_mode = CONFIG.get(section, "color_mode", fallback=COLOR_MODE)
        if default_color_mode not in {"Grayscale8", "RGB24"}:
            default_color_mode = COLOR_MODE

        backend = CONFIG.get(section, "backend", fallback="sane").strip().lower() or "sane"

        if backend == "sane":
            options = {
                "sane_device": CONFIG.get(section, "sane_device", fallback="").strip(),
                "sane_hint": CONFIG.get(section, "sane_hint", fallback=label).strip(),
                "command": CONFIG.get(section, "command", fallback="").strip(),
                "extra_args": CONFIG.get(section, "extra_args", fallback="").strip(),
                "source": CONFIG.get(section, "source", fallback="").strip(),
                "duplex": _cfg_get_bool(section, "duplex", False),
                "page_width_mm": _cfg_get_float(section, "page_width_mm", 0.0),
                "page_height_mm": _cfg_get_float(section, "page_height_mm", 0.0),
                "final_reduce_command": CONFIG.get(section, "final_reduce_command", fallback="").strip(),
            }
            entry: Dict[str, object] = {
                "label": label,
                "default_color_mode": default_color_mode,
                "backend": "sane",
                "options": options,
            }
        elif backend == "escl":
            url = CONFIG.get(section, "url", fallback="").strip()
            if not url:
                continue
            runner = legacy_runners.get(key)
            if not runner:
                continue
            username = CONFIG.get(section, "username", fallback="").strip()
            password = CONFIG.get(section, "password", fallback="").strip()
            auth = (username, password) if username and password else None
            entry = {
                "label": label,
                "default_color_mode": default_color_mode,
                "backend": "escl",
                "default_url": url,
                "runner": runner,
                "auth": auth,
            }
        else:
            continue

        registry[key] = entry

    return registry


SCANNER_REGISTRY = build_scanner_registry()
if not SCANNER_REGISTRY:
    raise RuntimeError("No scanners configured. Please provide at least one scanner in scanner.cfg")


DB_PATH = Path(os.getenv("SCAN_DB_PATH", "scan_jobs.sqlite3")).expanduser()


def normalize_crop_box(values: Optional[List[float]]) -> Optional[Tuple[float, float, float, float]]:
    if values is None:
        return None
    if len(values) != 4:
        raise HTTPException(status_code=400, detail="crop_box requires four numeric entries")
    left, top, right, bottom = values
    for name, value in zip(("left", "top", "right", "bottom"), values):
        if not 0.0 <= value <= 1.0:
            raise HTTPException(status_code=400, detail=f"crop_box {name} must be between 0 and 1")
    if not (left < right and top < bottom):
        raise HTTPException(status_code=400, detail="crop_box must satisfy left < right and top < bottom")
    return left, top, right, bottom


def determine_color_mode(requested: Optional[str], *, force_color: bool, default_mode: str) -> str:
    if force_color:
        return "RGB24"
    if requested:
        if requested not in {"Grayscale8", "RGB24"}:
            raise HTTPException(status_code=400, detail="color_mode must be 'Grayscale8' or 'RGB24'")
        return requested
    if default_mode not in {"Grayscale8", "RGB24"}:
        return COLOR_MODE
    return default_mode


class ScanCancelled(Exception):
    """Raised when a scan is cancelled mid-flight."""


T = TypeVar("T")


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_jobs (
                id TEXT PRIMARY KEY,
                scanner TEXT NOT NULL,
                status TEXT NOT NULL,
                params TEXT,
                result_path TEXT,
                error TEXT,
                stage TEXT,
                stage_detail TEXT,
                number_of_pages INTEGER,
                batch_count INTEGER,
                batches_completed INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(scan_jobs)")}
        if "stage" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN stage TEXT")
        if "stage_detail" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN stage_detail TEXT")
        if "number_of_pages" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN number_of_pages INTEGER")
        if "batch_count" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN batch_count INTEGER")
        if "batches_completed" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN batches_completed INTEGER")

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock, self._connect() as conn:
            self._ensure_schema(conn)

    def _with_schema_retry(self, func: Callable[[], T]) -> T:
        try:
            return func()
        except sqlite3.OperationalError as exc:
            if "no such table: scan_jobs" in str(exc).lower():
                self._init_db()
                return func()
            raise

    def create_job(self, job_id: str, scanner: str, params: Dict[str, object]) -> None:
        now = datetime.utcnow().isoformat()
        payload = json.dumps(params)
        def _insert() -> None:
            with self.lock, self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO scan_jobs (id, scanner, status, params, result_path, error, stage, stage_detail, number_of_pages, batch_count, batches_completed, created_at, updated_at)
                    VALUES (?, ?, 'pending', ?, NULL, NULL, ?, NULL, NULL, NULL, NULL, ?, ?)
                    """,
                    (job_id, scanner, payload, "queued", now, now),
                )

        self._with_schema_retry(_insert)

    def update_job(
        self,
        job_id: str,
        *,
        status: str,
        result_path: Optional[Path] = None,
        error: Optional[str] = None,
        stage: Optional[str] = None,
        stage_detail: Optional[str] = None,
        number_of_pages: Optional[int] = None,
        batch_count: Optional[int] = None,
        batches_completed: Optional[int] = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        result_value = str(result_path) if result_path else None
        def _update() -> None:
            with self.lock, self._connect() as conn:
                assignments = ["status = ?", "result_path = ?", "error = ?", "updated_at = ?"]
                values: List[object] = [status, result_value, error, now]
                if stage is not None:
                    assignments.append("stage = ?")
                    values.append(stage)
                if stage_detail is not None:
                    assignments.append("stage_detail = ?")
                    values.append(stage_detail)
                if number_of_pages is not None:
                    assignments.append("number_of_pages = ?")
                    values.append(number_of_pages)
                if batch_count is not None:
                    assignments.append("batch_count = ?")
                    values.append(batch_count)
                if batches_completed is not None:
                    assignments.append("batches_completed = ?")
                    values.append(batches_completed)
                sql = f"UPDATE scan_jobs SET {', '.join(assignments)} WHERE id = ?"
                values.append(job_id)
                conn.execute(sql, tuple(values))

        self._with_schema_retry(_update)

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, object]:
        data = dict(row)
        params = data.get("params")
        data["params"] = json.loads(params) if params else None
        return data

    def get_job(self, job_id: str) -> Optional[Dict[str, object]]:
        def _lookup() -> Optional[Dict[str, object]]:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM scan_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

        return self._with_schema_retry(_lookup)

    def delete_job(self, job_id: str) -> None:
        def _delete() -> None:
            with self.lock, self._connect() as conn:
                conn.execute("DELETE FROM scan_jobs WHERE id = ?", (job_id,))

        self._with_schema_retry(_delete)

    def list_jobs(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, object]], int]:
        def _list() -> Tuple[List[Dict[str, object]], int]:
            with self._connect() as conn:
                total = conn.execute("SELECT COUNT(*) FROM scan_jobs").fetchone()[0]
                rows = conn.execute(
                    "SELECT * FROM scan_jobs ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
            return [self._row_to_dict(row) for row in rows], total

        return self._with_schema_retry(_list)


JOB_STORE = JobStore(DB_PATH)
JOB_QUEUE: "queue.Queue[Optional[str]]" = queue.Queue()
PENDING_JOBS: Dict[str, Dict[str, object]] = {}
PENDING_LOCK = threading.Lock()
CANCELLED_JOBS: Set[str] = set()


def serialize_job(job: Dict[str, object]) -> Dict[str, object]:
    data = dict(job)
    data.pop("params", None)
    if "batch_count" in data:
        data["ocr_batch_count"] = data.pop("batch_count")
    if "batches_completed" in data:
        data["ocr_batches_completed"] = data.pop("batches_completed")
    duration = None
    created_at = data.get("created_at")
    try:
        if created_at:
            start_dt = datetime.fromisoformat(created_at)
            if data.get("status") in TERMINAL_STATUSES and data.get("updated_at"):
                end_dt = datetime.fromisoformat(str(data.get("updated_at")))
            else:
                end_dt = datetime.utcnow()
            duration = max((end_dt - start_dt).total_seconds(), 0.0)
    except Exception:
        duration = None
    data["duration_seconds"] = duration
    return data


def dispatch_scan(
    scanner_key: str,
    *,
    dpi: int,
    color_mode: str,
    processing_opts: Dict[str, object],
    progress_cb: Optional[Callable[[str], None]] = None,
    job_entry: Optional[Dict[str, object]] = None,
    job_id: Optional[str] = None,
) -> ScanResult:
    if scanner_key not in SCANNER_REGISTRY:
        raise RuntimeError(f"Unknown scanner: {scanner_key}")
    entry = SCANNER_REGISTRY[scanner_key]
    backend = entry.get("backend", "sane")
    print(f"[dispatch] scanner={scanner_key} backend={backend} dpi={dpi} color_mode={color_mode}")
    if backend == "sane":
        if progress_cb:
            progress_cb("scanning")
        result = scan_with_sane(
            entry.get("options", {}),
            dpi=dpi,
            color_mode=color_mode,
            processing_opts=processing_opts,
            progress_cb=progress_cb,
            job_entry=job_entry,
            job_id=job_id,
        )
        print(f"[dispatch] scanner={scanner_key} produced {len(result.pages)} page(s)")
        return result
    if backend == "escl":
        if progress_cb:
            progress_cb("scanning")
        session = create_escl_session(auth=entry.get("auth"))
        if job_entry and job_entry.get("cancel_requested"):
            raise ScanCancelled("Scan cancelled")
        pages = entry["runner"](entry["default_url"], dpi, color_mode, session=session, processing_opts=processing_opts)
        tmpdir = Path(tempfile.mkdtemp(prefix="scanjob_escl_"))
        return ScanResult(pages=pages, raw_dir=tmpdir, raw_paths=[])
    raise RuntimeError(f"Unsupported backend '{backend}' for scanner {scanner_key}")


class JobWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.stop_event = threading.Event()
        self.running_jobs: Dict[str, Dict[str, object]] = {}

    def shutdown(self) -> None:
        self.stop_event.set()
        JOB_QUEUE.put(None)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                job_id = JOB_QUEUE.get(timeout=0.5)
            except queue.Empty:
                continue
            if job_id is None:
                JOB_QUEUE.task_done()
                break
            with PENDING_LOCK:
                payload = PENDING_JOBS.pop(job_id, None)
                was_cancelled = job_id in CANCELLED_JOBS
                if was_cancelled:
                    CANCELLED_JOBS.discard(job_id)
            print(f"[worker] dequeued job {job_id} cancelled={was_cancelled} payload={'yes' if payload else 'no'}")
            if was_cancelled:
                JOB_STORE.update_job(job_id, status="cancelled", stage="cancelled")
                JOB_QUEUE.task_done()
                continue
            if payload is None:
                print(f"[worker] job {job_id} missing payload; marking failed")
                JOB_STORE.update_job(job_id, status="failed", error="Job payload missing", stage="failed")
                JOB_QUEUE.task_done()
                continue
            self._process(job_id, payload)
            JOB_QUEUE.task_done()

    def _process(self, job_id: str, payload: Dict[str, object]) -> None:
        try:
            print(f"[worker] starting job {job_id} with payload {payload}")
            JOB_STORE.update_job(job_id, status="running", stage="scanning", stage_detail="starting")
            self.running_jobs[job_id] = {
                "payload": payload,
                "stage": "scanning",
                "stage_detail": "starting",
                "cancel_requested": False,
                "process": None,
            }

            def update_stage(stage: str, detail: Optional[str] = None) -> None:
                normalized = stage if stage in STAGE_STATES else "scanning"
                JOB_STORE.update_job(job_id, status="running", stage=normalized, stage_detail=detail)
                entry = self.running_jobs.get(job_id)
                if entry is not None:
                    entry["stage"] = normalized
                    if detail is not None:
                        entry["stage_detail"] = detail

            entry = SCANNER_REGISTRY.get(payload["scanner"], {})
            result = dispatch_scan(
                payload["scanner"],
                dpi=int(payload["dpi"]),
                color_mode=str(payload["color_mode"]),
                processing_opts=payload.get("processing_opts", {}),
                progress_cb=lambda detail: update_stage("scanning", detail),
                job_entry=self.running_jobs[job_id],
                job_id=job_id,
            )
            pages = result.pages
            total_pages = len(pages)
            JOB_STORE.update_job(job_id, status="running", stage=None, number_of_pages=total_pages)
            job_dpi = int(payload.get("dpi") or DPI)
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_path = OUTPUT_DIR / f"{job_id}.pdf"
            update_stage("assembling")
            tmp_fd, tmp_raw = tempfile.mkstemp(suffix=".pdf", prefix=f"{job_id}_raw_")
            os.close(tmp_fd)
            tmp_raw_path = Path(tmp_raw)
            try:
                create_pdf_from_images(pages, tmp_raw_path, dpi=job_dpi)
                batch_size = 50
                total_batches = max(1, math.ceil(total_pages / batch_size))
                JOB_STORE.update_job(job_id, status="running", batch_count=total_batches, batches_completed=0)
                if total_batches == 1:
                    update_stage("ocr", "batch 1/1")
                    run_ocr_on_pdf(tmp_raw_path, output_path.with_suffix(".ocr.pdf"), image_dpi=job_dpi)
                    JOB_STORE.update_job(job_id, status="running", batches_completed=1)
                    tmp_pdf = output_path.with_suffix(".ocr.pdf")
                else:
                    import pikepdf

                    with pikepdf.Pdf.open(tmp_raw_path) as pdf:
                        chunk_paths: List[Path] = []
                        for idx in range(total_batches):
                            start = idx * batch_size
                            end = min(start + batch_size, len(pdf.pages))
                            chunk_file = tmp_raw_path.with_name(f"{job_id}_chunk_{idx + 1}.pdf")
                            chunk_pdf = pikepdf.Pdf.new()
                            for page in pdf.pages[start:end]:
                                chunk_pdf.pages.append(page)
                            chunk_pdf.save(chunk_file)
                            chunk_paths.append(chunk_file)

                    ocr_chunk_paths: List[Path] = []
                    for idx, chunk_path in enumerate(chunk_paths, start=1):
                        label = f"batch {idx}/{total_batches}"
                        update_stage("ocr", label)
                        JOB_STORE.update_job(job_id, status="running", stage="ocr", stage_detail=label, batches_completed=idx - 1)
                        ocr_out = chunk_path.with_suffix(".ocr.pdf")
                        run_ocr_on_pdf(chunk_path, ocr_out, image_dpi=job_dpi)
                        ocr_chunk_paths.append(ocr_out)
                        JOB_STORE.update_job(job_id, status="running", stage="ocr", stage_detail=label, batches_completed=idx)

                    update_stage("merging")
                    JOB_STORE.update_job(job_id, status="running", stage="merging", stage_detail=None)
                    merged_pdf = pikepdf.Pdf.new()
                    for ocr_chunk in ocr_chunk_paths:
                        with pikepdf.Pdf.open(ocr_chunk) as part:
                            merged_pdf.pages.extend(part.pages)
                    merged_pdf.save(output_path.with_suffix(".ocr.pdf"))
                    tmp_pdf = output_path.with_suffix(".ocr.pdf")
                    for path in chunk_paths + ocr_chunk_paths:
                        path.unlink(missing_ok=True)
                tmp_pdf.replace(output_path)
            finally:
                tmp_raw_path.unlink(missing_ok=True)
                shutil.rmtree(result.raw_dir, ignore_errors=True)
            final_reduce_command = entry.get("options", {}).get("final_reduce_command")
            if final_reduce_command:
                update_stage("reducing")
                reduced_output_path = output_path.with_suffix(".reduced.pdf")
                formatted_command = final_reduce_command.format(
                    output=shlex.quote(str(output_path)),
                    reduced_output=shlex.quote(str(reduced_output_path)),
                )
                subprocess.run(formatted_command, shell=True, check=True)
                if not reduced_output_path.exists():
                    raise RuntimeError("final_reduce_command did not produce reduced output")
                reduced_output_path.replace(output_path)
            update_stage("finalizing")
            JOB_STORE.update_job(job_id, status="completed", result_path=output_path, stage="completed")
            print(f"[worker] job {job_id} completed successfully; output={output_path}")
        except ScanCancelled as exc:
            JOB_STORE.update_job(job_id, status="cancelled", error=str(exc), stage="cancelled")
            print(f"[worker] job {job_id} cancelled by request")
        except Exception as exc:
            backend = SCANNER_REGISTRY.get(payload.get("scanner", ""), {}).get("backend", "")
            message = str(exc)
            if "document feeder out of documents" in message.lower():
                message = "Document feeder out of documents"
            if backend == "sane":
                saned_logs = collect_saned_logs()
                if saned_logs:
                    message = f"{message}\n--- saned logs (tail) ---\n{saned_logs}"
            JOB_STORE.update_job(job_id, status="failed", error=message, stage="failed")
            print(f"[worker] job {job_id} failed: {message}")
        finally:
            self.running_jobs.pop(job_id, None)


JOB_WORKER = JobWorker()


INDEX_HTML = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Page the Ripper API</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem; line-height: 1.5; }
    h1 { margin-top: 0; }
    form, .job-card, pre { border: 1px solid #ccc; border-radius: 6px; padding: 1rem; background: #fafafa; }
    form { max-width: 480px; }
    label { display: block; margin-bottom: 0.75rem; }
    select, input[type=\"number\"] { width: 100%; padding: 0.4rem; }
    button { padding: 0.4rem 0.9rem; margin-right: 0.5rem; }
    .job-card { margin-bottom: 1rem; }
    .job-status { font-weight: 600; }
    .job-actions button { margin-top: 0.4rem; }
    pre { overflow-x: auto; background: #f0f0f0; }
    code { font-family: ui-monospace, SFMono-Regular, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .note { color: #555; font-size: 0.95rem; }
  </style>
</head>
<body>
  <h1>Page the Ripper API</h1>
  <p>
    Use the interactive <a href=\"/docs\">Swagger UI</a> or <a href=\"/redoc\">ReDoc</a> to explore the API contract.
    The form below issues real scan jobs, and quick cURL recipes follow for scripts and automation.
  </p>

  <section>
    <h2>Start a Scan</h2>
    <form id=\"scan-form\">
      <label>
        Scanner
        <select id=\"scanner-select\" required></select>
      </label>
      <label>
        DPI (optional)
        <input type=\"number\" min=\"75\" max=\"1200\" step=\"25\" id=\"dpi\" placeholder=\"300\" />
      </label>
      <label>
        <input type=\"checkbox\" id=\"color\" /> Scan in color (RGB24)
      </label>
      <button type=\"submit\">Start Scan</button>
      <button type=\"button\" id=\"refresh-scanners\">Refresh Scanners</button>
      <p class=\"note\" id=\"form-status\"></p>
    </form>
  </section>

  <section>
    <h2>Jobs</h2>
    <div id=\"job-list\" class=\"note\">No jobs yet.</div>
  </section>

  <section>
    <h2>cURL Examples</h2>
    <h3>ET-3850 (Flatbed)</h3>
    <pre><code id=\"curl-create-et3850\"></code></pre>
    <pre><code id=\"curl-status-et3850\"></code></pre>
    <pre><code id=\"curl-result-et3850\"></code></pre>
    <pre><code id=\"curl-delete-et3850\"></code></pre>
    <h3>ES-580W (ADF Duplex)</h3>
    <pre><code id=\"curl-create-es580w\"></code></pre>
    <pre><code id=\"curl-status-es580w\"></code></pre>
    <pre><code id=\"curl-result-es580w\"></code></pre>
    <pre><code id=\"curl-delete-es580w\"></code></pre>
  </section>

  <script>
    const scannersSelect = document.getElementById('scanner-select');
    const refreshBtn = document.getElementById('refresh-scanners');
    const formStatus = document.getElementById('form-status');
    const jobList = document.getElementById('job-list');
    const form = document.getElementById('scan-form');

    async function loadScanners() {
      formStatus.textContent = 'Loading scanners…';
      try {
        const res = await fetch('/api/scanners');
        if (!res.ok) throw new Error(await res.text());
        const scanners = await res.json();
        scannersSelect.innerHTML = '';
        scanners.forEach(scanner => {
          const opt = document.createElement('option');
          opt.value = scanner.id;
          opt.textContent = `${scanner.label} (${scanner.backend})`;
          scannersSelect.appendChild(opt);
        });
        formStatus.textContent = scanners.length ? '' : 'No scanners configured.';
      } catch (err) {
        formStatus.textContent = `Failed to load scanners: ${err}`;
      }
    }

    function ensureJobListContainer() {
      if (jobList.dataset.hasJobs === 'true') return;
      jobList.dataset.hasJobs = 'true';
      jobList.innerHTML = '';
    }

    function addJobCard(jobId) {
      ensureJobListContainer();
      const card = document.createElement('div');
      card.className = 'job-card';
      card.dataset.jobId = jobId;
      card.innerHTML = `
        <div><strong>${jobId}</strong></div>
        <div>Status: <span class="job-status">pending</span></div>
        <div class="result"></div>
        <div class="job-actions">
          <button type="button" class="delete">Delete</button>
        </div>
      `;
      jobList.prepend(card);
      pollJob(jobId, card);
      card.querySelector('.delete').addEventListener('click', () => deleteJob(jobId, card));
    }

    async function deleteJob(jobId, card) {
      try {
        const res = await fetch(`/api/scans/${jobId}`, { method: 'DELETE' });
        if (res.status === 204) {
          card.querySelector('.job-status').textContent = 'deleted';
          card.querySelector('.result').textContent = '';
        } else {
          const text = await res.text();
          alert(`Failed to delete job: ${text}`);
        }
      } catch (err) {
        alert(`Failed to delete job: ${err}`);
      }
    }

    function pollJob(jobId, card) {
      const statusEl = card.querySelector('.job-status');
      const resultEl = card.querySelector('.result');
      const interval = setInterval(async () => {
        try {
          const res = await fetch(`/api/scans/${jobId}`);
          if (res.status === 404) {
            statusEl.textContent = 'deleted';
            clearInterval(interval);
            return;
          }
          if (!res.ok) throw new Error(await res.text());
          const data = await res.json();
          statusEl.textContent = data.status;
          if (data.status === 'completed') {
            clearInterval(interval);
            const link = document.createElement('a');
            link.href = `/api/scans/download/${jobId}`;
            link.textContent = 'Download PDF';
            link.target = '_blank';
            resultEl.innerHTML = '';
            resultEl.appendChild(link);
          } else if (data.status === 'failed') {
            clearInterval(interval);
            resultEl.textContent = data.error || 'Scan failed';
          }
        } catch (err) {
          statusEl.textContent = `error: ${err}`;
          clearInterval(interval);
        }
      }, 1500);
    }

    form.addEventListener('submit', async event => {
      event.preventDefault();
      const payload = { scanner: scannersSelect.value };
      const dpiValue = document.getElementById('dpi').value;
      if (dpiValue) payload.dpi = Number(dpiValue);
      if (document.getElementById('color').checked) payload.color = true;
      try {
        const res = await fetch('/api/scans', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (!res.ok) {
          const text = await res.text();
          formStatus.textContent = `Error: ${text}`;
          return;
        }
        const data = await res.json();
        formStatus.textContent = `Job ${data.job_id} enqueued.`;
        addJobCard(data.job_id);
      } catch (err) {
        formStatus.textContent = `Error: ${err}`;
      }
    });

    refreshBtn.addEventListener('click', event => {
      event.preventDefault();
      loadScanners();
    });

    function setCurlExamples() {
      const base = window.location.origin;
      document.getElementById('curl-create-et3850').textContent = `curl -X POST ${base}/api/scans \
  -H 'Content-Type: application/json' \
  -d '{\"scanner\":\"et3850\",\"dpi\":300,\"color\":true}'`;
      document.getElementById('curl-status-et3850').textContent = `curl ${base}/api/scans/<job_id>`;
      document.getElementById('curl-result-et3850').textContent = `curl -L -o et3850_<job_id>.pdf ${base}/api/scans/download/<job_id>`;
      document.getElementById('curl-delete-et3850').textContent = `curl -X DELETE ${base}/api/scans/<job_id>`;

      document.getElementById('curl-create-es580w').textContent = `curl -X POST ${base}/api/scans \
  -H 'Content-Type: application/json' \
  -d '{\"scanner\":\"es580w\",\"dpi\":300}'`;
      document.getElementById('curl-status-es580w').textContent = `curl ${base}/api/scans/<job_id>`;
      document.getElementById('curl-result-es580w').textContent = `curl -L -o es580w_<job_id>.pdf ${base}/api/scans/download/<job_id>`;
      document.getElementById('curl-delete-es580w').textContent = `curl -X DELETE ${base}/api/scans/<job_id>`;
    }

    loadScanners();
    setCurlExamples();
  </script>
</body>
</html>
"""
class ScanRequest(BaseModel):
    scanner: str = Field(
        ...,
        description="Scanner identifier from configuration (e.g., 'et3850' or 'es580w')",
        examples=["et3850", "es580w"],
    )
    dpi: Optional[int] = Field(
        None,
        ge=75,
        le=1200,
        description="Optional DPI override; defaults to configured scanner DPI when omitted",
        examples=[300, 600, 1200],
    )


class ScanCreateResponse(BaseModel):
    job_id: str
    status: str
    stage: str
    duration_seconds: float


class ScanJob(BaseModel):
    id: str
    scanner: str
    status: str
    result_path: Optional[str] = None
    error: Optional[str] = None
    created_at: str
    updated_at: str
    stage: Optional[str] = None
    stage_detail: Optional[str] = None
    number_of_pages: Optional[int] = None
    ocr_batch_count: Optional[int] = None
    ocr_batches_completed: Optional[int] = None
    duration_seconds: Optional[float] = None

    class Config:
        extra = "ignore"


class ScanJobPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[ScanJob]


class ScannerInfo(BaseModel):
    id: str
    label: str
    backend: str
    backend_status: str
    configured_device: Optional[str] = None
    in_use: bool


class ScannerDetails(BaseModel):
    id: str
    label: str
    backend: str
    backend_status: Optional[str] = None
    configured_device: Optional[str] = None
    backend_details: Optional[Dict[str, Any]] = None
    in_use: bool


app = FastAPI(
    title="Page the Ripper Service",
    version="1.1.0",
    description="REST interface for Epson scanners via SANE or eSCL with OCR and PDF output.",
    swagger_ui_parameters={
        "operationsSorter": "function (a, b) { var order = { get: 0, post: 1, put: 2, patch: 3, delete: 4, options: 5, head: 6 }; var methodA = order[(a.get('method') || '').toLowerCase()]; var methodB = order[(b.get('method') || '').toLowerCase()]; if (methodA === undefined) { methodA = 99; } if (methodB === undefined) { methodB = 99; } if (methodA === methodB) { return a.get('path').localeCompare(b.get('path')); } return methodA - methodB; }"
    },
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.on_event("startup")
def _startup() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not JOB_WORKER.is_alive():
        JOB_WORKER.start()


@app.on_event("shutdown")
def _shutdown() -> None:
    if JOB_WORKER.is_alive():
        JOB_WORKER.shutdown()
        JOB_WORKER.join(timeout=10)


@app.get("/api/scanners", tags=["scanners"], summary="List configured scanners", response_model=List[ScannerInfo])
def list_scanners() -> List[ScannerInfo]:
    out: List[Dict[str, object]] = []
    for key, entry in SCANNER_REGISTRY.items():
        backend = entry.get("backend", "sane")
        backend_status = "unknown"
        configured_device: Optional[str] = None
        in_use = any(
            (job.get("payload", {}) or {}).get("scanner") == key
            for job in JOB_WORKER.running_jobs.values()
        )
        if backend == "sane":
            details = fetch_sane_backend_details(entry)
            backend_status = details.get("status", "unknown")
            configured = details.get("configured", {}) if isinstance(details, dict) else {}
            configured_device = configured.get("sane_device") or details.get("resolved_device")
        elif backend == "escl":
            details = fetch_escl_backend_details(entry)
            backend_status = details.get("status", "unknown")
            configured_device = details.get("default_url")
        else:
            backend_status = "error"
        out.append(
            {
                "id": key,
                "label": entry["label"],
                "backend": backend,
                "backend_status": backend_status,
                "configured_device": configured_device,
                "in_use": in_use,
            }
        )
    return [ScannerInfo(**item) for item in out]


@app.get("/api/scanners/{scanner_id}", tags=["scanners"], summary="Get scanner details", response_model=ScannerDetails)
def get_scanner_details(scanner_id: str) -> ScannerDetails:
    entry = SCANNER_REGISTRY.get(scanner_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Unknown scanner '{scanner_id}'")
    backend = entry.get("backend", "sane")
    details: Dict[str, object] = {
        "id": scanner_id,
        "label": entry.get("label"),
        "backend": backend,
        "default_color_mode": entry.get("default_color_mode", COLOR_MODE),
    }
    if backend == "sane":
        details["backend_details"] = fetch_sane_backend_details(entry)
    elif backend == "escl":
        details["backend_details"] = fetch_escl_backend_details(entry)
    else:
        details["backend_details"] = {"status": "error", "error": f"Unsupported backend '{backend}'"}
    in_use = any(
        (job.get("payload", {}) or {}).get("scanner") == scanner_id
        for job in JOB_WORKER.running_jobs.values()
    )
    details["backend_status"] = details.get("backend_details", {}).get("status") if isinstance(details.get("backend_details"), dict) else None
    details["configured_device"] = details.get("backend_details", {}).get("configured", {}).get("sane_device") if backend == "sane" else details.get("backend_details", {}).get("default_url")
    details["in_use"] = in_use
    # prune default_color_mode from response_model; backend_details may still contain it
    details.pop("default_color_mode", None)
    return ScannerDetails(**details)


@app.get("/api/system", tags=["system"], summary="Overall system health and status")
def get_system_status() -> Dict[str, object]:
    return gather_system_health()


@app.get(
    "/api/scans",
    tags=["scans"],
    summary="List scan jobs with pagination",
    response_model=ScanJobPage,
)
def list_scan_jobs(
    page: int = Query(1, ge=1, description="1-based page index"),
    page_size: int = Query(100, ge=1, le=500, description="Number of jobs per page"),
) -> ScanJobPage:
    offset = (page - 1) * page_size
    items, total = JOB_STORE.list_jobs(offset=offset, limit=page_size)
    items = [ScanJob(**serialize_job(item)) for item in items]
    return ScanJobPage(page=page, page_size=page_size, total=total, items=items)


@app.post("/api/scans", status_code=202, tags=["scans"], summary="Start a new scan job")
def create_scan(request: ScanRequest) -> ScanCreateResponse:
    if request.scanner not in SCANNER_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown scanner '{request.scanner}'")
    entry = SCANNER_REGISTRY[request.scanner]
    dpi = int(request.dpi or DPI)
    color_mode = str(entry.get("default_color_mode", COLOR_MODE))
    processing_opts = {"do_crop": False, "auto_rotate": False, "crop_box": None}
    params_for_db = {"dpi": dpi}
    job_id = uuid.uuid4().hex
    JOB_STORE.create_job(job_id, request.scanner, params_for_db)
    payload = {
        "scanner": request.scanner,
        "dpi": dpi,
        "color_mode": color_mode,
        "processing_opts": processing_opts,
    }
    with PENDING_LOCK:
        PENDING_JOBS[job_id] = payload
    JOB_QUEUE.put(job_id)
    return ScanCreateResponse(job_id=job_id, status="pending", stage="queued", duration_seconds=0.0)


@app.get("/api/scans/{job_id}", tags=["scans"], summary="Check scan job status")
def get_scan_status(job_id: str) -> ScanJob:
    job = JOB_STORE.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return ScanJob(**serialize_job(job))


@app.get("/api/scans/download/{job_id}", tags=["scans"], summary="Download the finished PDF")
def get_scan_result(job_id: str) -> FileResponse:
    job = JOB_STORE.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "completed" or not job.get("result_path"):
        raise HTTPException(status_code=409, detail="Scan is not complete")
    path = Path(job["result_path"])
    if not path.exists():
        raise HTTPException(status_code=500, detail="Result file missing on disk")
    return FileResponse(path, media_type="application/pdf", filename=path.name)


@app.delete("/api/scans/{job_id}", status_code=204, tags=["scans"], summary="Delete a job and remove any artifacts")
def delete_scan(job_id: str) -> Response:
    job = JOB_STORE.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status = job.get("status", "")
    if status == "pending":
        with PENDING_LOCK:
            payload = PENDING_JOBS.pop(job_id, None)
            if payload is not None:
                CANCELLED_JOBS.add(job_id)
        JOB_STORE.update_job(job_id, status="cancelled", stage="cancelled")
        job = JOB_STORE.get_job(job_id) or job
    elif status == "running":
        with PENDING_LOCK:
            CANCELLED_JOBS.add(job_id)
        entry = JOB_WORKER.running_jobs.get(job_id)
        if entry is not None:
            entry["cancel_requested"] = True
            entry["stage"] = "cancelled"
            proc = entry.get("process")
            if proc is not None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        deadline = time.time() + CANCEL_WAIT_SECONDS
        while job_id in JOB_WORKER.running_jobs and time.time() < deadline:
            time.sleep(0.1)
        if job_id in JOB_WORKER.running_jobs:
            raise HTTPException(status_code=500, detail="Timed out waiting for scan cancellation")
        JOB_STORE.update_job(job_id, status="cancelled", stage="cancelled")
        job = JOB_STORE.get_job(job_id) or job
    else:
        JOB_STORE.update_job(job_id, status="deleted", stage="deleted")
    result_path = job.get("result_path")
    if result_path:
        path = Path(result_path)
        if path.exists():
            try:
                path.unlink()
            except OSError as exc:  # pragma: no cover - filesystem issues
                raise HTTPException(status_code=500, detail=f"Failed to delete file: {exc}") from exc
    remove_debug_raw(job_id)
    JOB_STORE.delete_job(job_id)
    return Response(status_code=204)


def main() -> None:
    import uvicorn

    uvicorn.run(
        "main:app",
        host=os.getenv("SCAN_SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SCAN_SERVER_PORT", "8000")),
        reload=False,
    )


if __name__ == "__main__":
    main()
