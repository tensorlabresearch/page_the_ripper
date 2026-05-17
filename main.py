import configparser
import io
import json
import math
import os
import platform
import queue
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import img2pdf
import numpy as np
import ocrmypdf
import pytesseract
import requests
import urllib3
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from ocrmypdf.exceptions import ExitCodeException, MissingDependencyError
import pikepdf
from PIL import Image, ImageOps
from pydantic import BaseModel, Field
from urllib3.exceptions import InsecureRequestWarning

# Cap Tesseract threading to avoid runaway resource usage.
os.environ.setdefault("OMP_THREAD_LIMIT", "2")

from tools import escl_scan

try:
    import psutil  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    psutil = None

# ====================== CONFIG ======================
CONFIG_PATH = Path(os.getenv("SCANNER_CFG", "scanner.cfg"))
DEFAULT_CONFIG: dict[str, dict[str, str]] = {
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
    pages: list[Image.Image]
    raw_dir: Path
    raw_paths: list[Path]


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
SCAN_TIMEOUT = int(os.getenv("SCAN_TIMEOUT", "900"))
# If scanimage hasn't produced page-001.png within this many seconds, fail
# fast with a clear "no paper / unreachable scanner" message. Catches the
# epsonds "ADF Duplex" hang on an empty feeder long before SCAN_TIMEOUT.
SCAN_INITIAL_PAGE_TIMEOUT = int(os.getenv("SCAN_INITIAL_PAGE_TIMEOUT", "60"))
# Once scanning is in progress, if no new page-NNN file appears for this long
# the scan is considered stalled (ADF ran out, paper jam, backend hang) and
# aborted. Captured pages remain on disk for recovery via /api/scans/{id}/recover.
SCAN_PAGE_IDLE_TIMEOUT = int(os.getenv("SCAN_PAGE_IDLE_TIMEOUT", "120"))
CANCEL_WAIT_SECONDS = int(os.getenv("SCAN_CANCEL_WAIT", "30"))
# scanimage exit code mapping (see scanimage(1) and SANE backend conventions).
# Code 3 = SANE_STATUS_BUSY — the scanner refused the session, almost always
# transient when sane-airscan / epsonds is woken cold.
SANE_BUSY_EXIT_CODE = 3
# Multi-function devices (e.g. ET-3850) serialize scan against print: while a
# print job is running, every scan request returns BUSY. The retry schedule
# below tolerates a moderate print job (~3.5 min total wall time) before
# giving up. Tune via SCAN_BUSY_MAX_ATTEMPTS / SCAN_BUSY_BACKOFF_SECONDS.
SCAN_BUSY_MAX_ATTEMPTS = int(os.getenv("SCAN_BUSY_MAX_ATTEMPTS", "7"))

SANE_IO_ERROR_EXIT_CODE = 9
# I/O errors (exit 9) on USB scanners are sometimes transient: the device's
# endpoint glitches during cold start or after a previous session and recovers
# on the next attempt. Persistent IO_ERROR almost always means a power-cycle
# is needed; we keep this retry budget short so the user gets the hardware
# hint quickly rather than waiting through long backoffs.
SCAN_IO_MAX_ATTEMPTS = int(os.getenv("SCAN_IO_MAX_ATTEMPTS", "2"))


def _parse_backoff_env(value: str | None) -> tuple[float, ...] | None:
    if not value:
        return None
    try:
        return tuple(float(x) for x in value.split(",") if x.strip())
    except ValueError:
        return None


SCAN_BUSY_BACKOFF_SECONDS: tuple[float, ...] = (
    _parse_backoff_env(os.getenv("SCAN_BUSY_BACKOFF_SECONDS"))
    or (3.0, 8.0, 15.0, 30.0, 60.0, 90.0)
)
SCAN_IO_BACKOFF_SECONDS: tuple[float, ...] = (
    _parse_backoff_env(os.getenv("SCAN_IO_BACKOFF_SECONDS"))
    or (3.0,)
)


def _retry_on_sane_busy(
    fn,
    *,
    cancel_check=None,
    progress_cb=None,
    sleep=None,
):
    """Run `fn()` with retry-and-backoff for transient SANE errors.

    Retries two classes of errors independently:
      * exit 3 (`SANE_STATUS_BUSY`) — typically multi-function-device print
        contention; long backoff (~3.5 min budget by default).
      * exit 9 (`SANE_STATUS_IO_ERROR`) — usually USB endpoint glitch on cold
        start; short backoff (~3s, 1 retry by default).

    All other errors propagate immediately. `fn` must surface failures as
    `RuntimeError("scanimage failed (N): …")`.

    cancel_check, progress_cb, and sleep are dependency-injected so tests can
    drive the retry loop without sleeping in real time.
    """
    sleep_fn = sleep if sleep is not None else time.sleep
    busy_attempts = 0
    io_attempts = 0
    last_busy_exc: RuntimeError | None = None
    last_io_exc: RuntimeError | None = None
    while True:
        try:
            return fn()
        except RuntimeError as exc:
            msg = str(exc)
            if f"scanimage failed ({SANE_BUSY_EXIT_CODE})" in msg:
                busy_attempts += 1
                last_busy_exc = exc
                if busy_attempts >= SCAN_BUSY_MAX_ATTEMPTS:
                    break
                delay = SCAN_BUSY_BACKOFF_SECONDS[
                    min(busy_attempts - 1, len(SCAN_BUSY_BACKOFF_SECONDS) - 1)
                ]
                label = "BUSY"
                progress_text = (
                    f"scanner busy; retry {busy_attempts}/"
                    f"{SCAN_BUSY_MAX_ATTEMPTS - 1} in {delay:g}s"
                )
            elif f"scanimage failed ({SANE_IO_ERROR_EXIT_CODE})" in msg:
                io_attempts += 1
                last_io_exc = exc
                if io_attempts >= SCAN_IO_MAX_ATTEMPTS:
                    break
                delay = SCAN_IO_BACKOFF_SECONDS[
                    min(io_attempts - 1, len(SCAN_IO_BACKOFF_SECONDS) - 1)
                ]
                label = "IO_ERROR"
                progress_text = (
                    f"scanner reported I/O error; retry {io_attempts}/"
                    f"{SCAN_IO_MAX_ATTEMPTS - 1} in {delay:g}s"
                )
            else:
                raise
            if cancel_check and cancel_check():
                raise ScanCancelled("Scan cancelled") from exc
            if progress_cb:
                progress_cb(progress_text)
            print(
                f"[scan] {label} on attempt {busy_attempts + io_attempts}; "
                f"sleeping {delay:g}s before retry",
                flush=True,
            )
            sleep_fn(delay)

    # Decide which exit path: prefer the most recent error class to surface.
    if last_io_exc is not None and (last_busy_exc is None or io_attempts > busy_attempts):
        raise RuntimeError(
            f"scanimage reported I/O_ERROR {io_attempts} times. Likely causes "
            "(in order of frequency for ADF scanners): "
            "(1) **paper jam or double-document feed** — open the ADF lid, "
            "remove all sheets, fan and re-stack the stack, then retry. "
            "Some backends (e.g. epsonds) report jams as opaque I/O errors "
            "rather than SANE_STATUS_JAMMED. "
            "(2) scanner USB endpoint stuck — power-cycle the scanner "
            "(unplug AC, wait ~5s, plug back in). "
            "(3) flaky USB cable or under-powered hub. "
            "(4) SANE driver in a bad state — try the airscan backend "
            "instead of epsonds in scanner.cfg. "
            f"Last error: {last_io_exc}"
        ) from last_io_exc
    total_wait = sum(
        SCAN_BUSY_BACKOFF_SECONDS[: max(0, SCAN_BUSY_MAX_ATTEMPTS - 1)]
    )
    raise RuntimeError(
        f"scanimage reported BUSY {busy_attempts} times across "
        f"~{total_wait:.0f}s of retries. The scanner may be: "
        "(1) actively printing — multi-function devices serialize scan against print; "
        "wait for the print job to finish and retry. "
        "(2) jammed or out of paper. "
        "(3) held open by another scan client. "
        "(4) in deep sleep — press a button on the front panel to wake it. "
        f"Last error: {last_busy_exc}"
    ) from last_busy_exc
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "deleted"}
JOB_MONITOR_INTERVAL = int(os.getenv("JOB_MONITOR_INTERVAL", "60"))
JOB_STALE_THRESHOLD = int(os.getenv("JOB_STALE_THRESHOLD", "600"))
# Stage-aware idle thresholds. ocrmypdf does not surface progress through our
# stage_detail, so a 168-page OCR pass legitimately looks idle for tens of
# minutes; the monitor must allow for that. Override individually via env
# (e.g. JOB_STALE_THRESHOLD_OCR=7200).
JOB_STALE_THRESHOLDS_BY_STAGE: dict[str, int] = {
    "ocr": int(os.getenv("JOB_STALE_THRESHOLD_OCR", "3600")),
    "recovering": int(os.getenv("JOB_STALE_THRESHOLD_RECOVERING", "3600")),
    "assembling": int(os.getenv("JOB_STALE_THRESHOLD_ASSEMBLING", "1800")),
    "merging": int(os.getenv("JOB_STALE_THRESHOLD_MERGING", "1800")),
    "cropping": int(os.getenv("JOB_STALE_THRESHOLD_CROPPING", "1800")),
}

# Registry of in-flight background-thread jobs (crop / composite / recover)
# so the monitor knows they're alive even though they don't go through
# JOB_WORKER. JOB_WORKER.running_jobs covers the scan worker; this covers
# everything else.
_ACTIVE_BACKGROUND_JOBS: dict[str, threading.Thread] = {}
_ACTIVE_BACKGROUND_LOCK = threading.Lock()


def _register_background_job(job_id: str, thread: threading.Thread) -> None:
    with _ACTIVE_BACKGROUND_LOCK:
        _ACTIVE_BACKGROUND_JOBS[job_id] = thread


def _unregister_background_job(job_id: str) -> None:
    with _ACTIVE_BACKGROUND_LOCK:
        _ACTIVE_BACKGROUND_JOBS.pop(job_id, None)


def _background_job_alive(job_id: str) -> bool:
    with _ACTIVE_BACKGROUND_LOCK:
        thread = _ACTIVE_BACKGROUND_JOBS.get(job_id)
    return bool(thread and thread.is_alive())


class _Heartbeat:
    """Bump updated_at periodically so JobMonitor sees the job is progressing.

    Use as a context manager around long subprocess calls (e.g. ocrmypdf):
        with _Heartbeat(job_id, interval=60):
            run_ocr_on_pdf(...)
    """

    def __init__(self, job_id: str, *, interval: float = 60.0):
        self.job_id = job_id
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_Heartbeat":
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"heartbeat-{self.job_id[:8]}"
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                current = JOB_STORE.get_job(self.job_id)
                if current is None or current.get("status") not in {"running", "pending"}:
                    return
                JOB_STORE.touch(self.job_id)
            except Exception:
                # Heartbeat failures must never break the actual work.
                pass


def create_escl_session(verify_ssl: bool | None = None, auth: tuple[str, str] | None = None) -> requests.Session:
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
def escl_post_scan_job(
    base_url: str,
    input_source: str,
    dpi: int,
    color_mode: str,
    media_name: str | None,
    session: requests.Session | None = None,
    scan_region: tuple[int, int] | None = None,
) -> tuple[str, str]:
    """
    Create an eSCL ScanJob and return (job_url, next_document_url).
    input_source: "Platen", "Adf", or "AdfDuplex"
    color_mode: "Color" or "Gray"
    media_name: e.g. "na_letter_8.5x11in" (None to let device auto-size)
    """
    # eSCL uses XML; keep settings minimal + broadly compatible
    # Namespaces commonly accepted by devices
    # - Version is often PWG 2.1, but many ignore it.
    media_block = (
        f"""
        <pwg:MediaSize>
            <pwg:Name>{media_name}</pwg:Name>
        </pwg:MediaSize>
    """
        if media_name
        else ""
    )

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
    r = session.post(url, data=xml.encode("utf-8"), headers={"Content-Type": "text/xml"}, timeout=HTTP_TIMEOUT)
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


def escl_fetch_documents(nextdoc_url: str, session: requests.Session | None = None) -> list[bytes]:
    """
    Pull every document (page image) for the job until the device stops serving pages.
    Returns list of JPEG bytes.
    """
    out: list[bytes] = []
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
def list_sane_devices() -> list[tuple[str, str]]:
    """Return all devices reported by SANE as (id, description)."""
    proc = subprocess.run(
        ["scanimage", "-L"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode not in {0}:
        stderr = proc.stderr.strip()
        raise RuntimeError(f"scanimage -L failed ({proc.returncode}): {stderr or proc.stdout.strip()}")

    devices: list[tuple[str, str]] = []
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
    options: dict[str, object],
    *,
    dpi: int,
    color_mode: str,
    output_dir: Path,
    progress_cb: Callable[[str], None] | None = None,
    job_entry: dict[str, object] | None = None,
) -> list[Path]:
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

    def execute_scan_command(command: str | list[str], *, shell: bool) -> tuple[str, str]:
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
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
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
                    elapsed = time.time() - start_time
                    # Track the freshest page-*.* file's mtime so we can detect
                    # both "never started" and "stalled mid-scan" via the same
                    # filesystem peek. Globbing here is cheap (the dir holds
                    # at most a few hundred small page files).
                    existing_pages = sorted(output_dir.glob("page-*.*"))
                    latest_mtime = max(
                        (p.stat().st_mtime for p in existing_pages), default=0.0
                    )

                    # Fast-fail when scanimage produces no output at all. The
                    # classic case is epsonds + ADF Duplex with an empty
                    # feeder: scanimage blocks inside sane_start() and never
                    # writes a page, so the full SCAN_TIMEOUT would elapse for
                    # nothing.
                    if elapsed > SCAN_INITIAL_PAGE_TIMEOUT and not existing_pages:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        adf_hint = (
                            " For ADF scans, make sure paper is loaded squarely in the "
                            "feeder; some backends (e.g. epsonds) silently block on an "
                            "empty feeder."
                        ) if source and "adf" in source.lower() else ""
                        raise RuntimeError(
                            f"Scanner produced no pages in the first "
                            f"{SCAN_INITIAL_PAGE_TIMEOUT}s.{adf_hint}"
                        )

                    # Mid-scan stall: pages have started flowing, but nothing
                    # new for SCAN_PAGE_IDLE_TIMEOUT seconds. Most often this
                    # is an ADF that ran out partway through a long batch.
                    # The captured pages stay on disk; the user can hit Recover
                    # to keep them as a PDF.
                    if (
                        existing_pages
                        and latest_mtime > 0
                        and (time.time() - latest_mtime) > SCAN_PAGE_IDLE_TIMEOUT
                    ):
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        raise RuntimeError(
                            f"Scanner stalled after page {len(existing_pages)}: "
                            f"no new page in the last {SCAN_PAGE_IDLE_TIMEOUT}s. "
                            "If using an ADF, the feeder may have run out or jammed. "
                            "Captured pages remain on disk — click Recover (or Resume) "
                            "on the failed job to keep them."
                        )
                    if elapsed > SCAN_TIMEOUT:
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

    def _run_scanimage_with_busy_retry(
        cmd: str | list[str], *, shell: bool
    ) -> tuple[str, str]:
        return _retry_on_sane_busy(
            lambda: execute_scan_command(cmd, shell=shell),
            cancel_check=lambda: bool(job_entry and job_entry.get("cancel_requested")),
            progress_cb=progress_cb,
        )

    print(
        f"[sane_scan_to_directory] device={device_id} hint={sane_hint} dpi={dpi} mode={sane_mode} dest={batch_pattern}"
    )

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
        stdout, stderr = _run_scanimage_with_busy_retry(command, shell=True)
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
        stdout, stderr = _run_scanimage_with_busy_retry(args, shell=False)

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


def preserve_sane_raw_pages(png_files: list[Path], *, job_id: str | None) -> Path | None:
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
    options: dict[str, object],
    *,
    dpi: int,
    color_mode: str,
    processing_opts: dict[str, object] | None,
    progress_cb: Callable[[str], None] | None = None,
    job_entry: dict[str, object] | None = None,
    job_id: str | None = None,
) -> ScanResult:
    # Include job_id in the prefix so a crashed scan's raw pages can be
    # discovered later by /api/scans/{id}/recover.
    prefix = f"scanjob-{job_id}-" if job_id else "scanjob-"
    tmpdir = Path(tempfile.mkdtemp(prefix=prefix))
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

        finalized: list[Image.Image] = []
        for png_file in png_files:
            with Image.open(png_file) as pil:
                pil.load()
                finalized.append(finalize_page(pil, color_mode=color_mode, processing_opts=processing_opts))
        return ScanResult(pages=finalized, raw_dir=tmpdir, raw_paths=png_files)
    except Exception:
        # Intentionally do NOT delete tmpdir here. If pages were captured
        # before the failure, they're salvageable via POST /api/scans/{id}/recover.
        # JobWorker._process records the path in params after marking the job
        # failed, so the UI's Recover icon lights up automatically. /tmp gets
        # cleared by the OS on reboot, so this isn't a permanent leak.
        raise


def collect_saned_logs(limit: int = 80) -> str | None:
    try:
        proc = subprocess.run(
            ["journalctl", "-u", "saned.socket", "-n", str(limit), "--no-pager"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    logs = proc.stdout.strip()
    return logs or None


def fetch_sane_backend_details(entry: dict[str, object]) -> dict[str, object]:
    options = dict(entry.get("options", {}))
    sane_device = str(options.get("sane_device", "") or "")
    sane_hint = str(options.get("sane_hint", "") or entry.get("label", ""))

    details: dict[str, object] = {
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
            capture_output=True,
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


def fetch_escl_backend_details(entry: dict[str, object]) -> dict[str, object]:
    url = str(entry.get("default_url") or "")
    auth = entry.get("auth")
    details: dict[str, object] = {
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


def gather_system_health() -> dict[str, object]:
    severity_rank = {"ok": 0, "warning": 1, "error": 2}
    system_info = {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
    }

    uptime_seconds: float | None = None
    if psutil is not None:
        try:
            uptime_seconds = max(0.0, time.time() - psutil.boot_time())
        except Exception:
            uptime_seconds = None
    if uptime_seconds is None:
        try:
            with open("/proc/uptime", encoding="utf-8") as fp:
                uptime_seconds = float(fp.read().split()[0])
        except Exception:
            uptime_seconds = None

    memory_info: dict[str, object] | None = None
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

    cpu_load: dict[str, object] | None = None
    try:
        load1, load5, load15 = os.getloadavg()  # type: ignore[attr-defined]
        cpu_load = {"load_1": load1, "load_5": load5, "load_15": load15}
    except (AttributeError, OSError):
        cpu_load = None

    # Disk usage for the scan output volume and for /tmp (where in-flight raw
    # scan pages live). On most installs these are the same filesystem; we
    # still report both so the user can spot when /tmp is on tmpfs and tight.
    def _disk_usage(path: Path) -> dict[str, object] | None:
        try:
            usage = shutil.disk_usage(path)
        except OSError as exc:
            return {"status": "error", "error": str(exc), "path": str(path)}
        used = usage.total - usage.free
        percent = (used / usage.total * 100.0) if usage.total else 0.0
        return {
            "path": str(path),
            "total_bytes": usage.total,
            "free_bytes": usage.free,
            "used_bytes": used,
            "percent_used": round(percent, 1),
        }

    disks: list[dict[str, object]] = []
    seen_devices: set[int] = set()
    for label, candidate in (("scans", OUTPUT_DIR), ("tmp", Path(tempfile.gettempdir()))):
        try:
            real = candidate.resolve()
            real.mkdir(parents=True, exist_ok=True)
            dev = real.stat().st_dev
        except OSError:
            dev = None
        if dev is not None and dev in seen_devices:
            # Same underlying filesystem as a previously-reported mount; skip
            # to avoid duplicate cards in the UI.
            continue
        if dev is not None:
            seen_devices.add(dev)
        info = _disk_usage(real if dev is not None else candidate)
        if info is not None:
            info["label"] = label
            disks.append(info)

    resources = {
        "uptime_seconds": uptime_seconds,
        "cpu_count": os.cpu_count(),
        "cpu_load": cpu_load,
        "memory": memory_info,
        "disks": disks,
    }

    components: dict[str, object] = {}
    status_scores: list[int] = []

    def record(name: str, data: dict[str, object]) -> None:
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
    escl_entries: list[dict[str, object]] = []
    escl_worst = 0
    for scanner_id, entry in SCANNER_REGISTRY.items():
        if entry.get("backend") != "escl":
            continue
        url = entry.get("default_url")
        auth = entry.get("auth")
        scanner_info: dict[str, object] = {
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


def detect_osd_rotation(pil_img: Image.Image, lang: str = TESSERACT_LANG) -> int | None:
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
    processing_opts: dict[str, object] | None = None,
) -> Image.Image:
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
        if (
            TARGET_WIDTH > 0
            and TARGET_HEIGHT > 0
            and (fallback.width > TARGET_WIDTH or fallback.height > TARGET_HEIGHT)
        ):
            fallback = ImageOps.contain(fallback, (TARGET_WIDTH, TARGET_HEIGHT), method=Image.LANCZOS)
        page = fallback
    return page


# ---------------- PDF assembly ----------------
def create_pdf_from_images(pages: list[Image.Image], out_path: Path, *, dpi: int | None) -> None:
    """
    Use img2pdf to stitch the provided PIL images into a single PDF.
    """
    effective_dpi = dpi or 300
    image_streams: list[bytes] = []
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
    image_dpi: int | None = None,
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
    output_pdf: Path | None = None,
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
    session: requests.Session | None = None,
) -> list[bytes]:
    session = session or create_escl_session()
    try:
        existing_jobs = escl_scan.list_jobs(session, base_url)
    except (requests.HTTPError, requests.RequestException):
        existing_jobs = []
    for job_uri, _job_state in existing_jobs:
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
    session: requests.Session | None = None,
) -> list[bytes]:
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
                for job_uri, _job_state in escl_scan.list_jobs(session, base_url):
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


def scan_es_580w_letter_duplex(
    base_url: str,
    dpi: int,
    color_mode: str,
    session: requests.Session | None = None,
    processing_opts: dict[str, object] | None = None,
) -> list[Image.Image]:
    """
    ES-580W: force ADF duplex, Letter size, scan both sides.
    """
    jpeg_pages = capture_es_580w_letter_duplex_raw(base_url, dpi, color_mode, session=session)

    return [
        finalize_page(Image.open(io.BytesIO(b)), color_mode=color_mode, processing_opts=processing_opts)
        for b in jpeg_pages
    ]


def scan_et_3850_platen(
    base_url: str,
    dpi: int,
    color_mode: str,
    session: requests.Session | None = None,
    processing_opts: dict[str, object] | None = None,
) -> list[Image.Image]:
    """
    ET-3850: scan from flatbed (platen), then deskew/trim.
    (Most devices only return one page for platen; we support more if available.)
    """
    jpeg_pages = capture_et_3850_platen_raw(base_url, dpi, color_mode, session=session)
    return [
        finalize_page(Image.open(io.BytesIO(jpeg_bytes)), color_mode=color_mode, processing_opts=processing_opts)
        for jpeg_bytes in jpeg_pages
    ]


def build_scanner_registry() -> dict[str, dict[str, object]]:
    legacy_runners = {
        "et3850": scan_et_3850_platen,
        "es580w": scan_es_580w_letter_duplex,
    }
    registry: dict[str, dict[str, object]] = {}
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
            entry: dict[str, object] = {
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


def normalize_crop_box(values: list[float] | None) -> tuple[float, float, float, float] | None:
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


def determine_color_mode(requested: str | None, *, force_color: bool, default_mode: str) -> str:
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
                tags TEXT NOT NULL DEFAULT '[]',
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
        if "tags" not in columns:
            conn.execute("ALTER TABLE scan_jobs ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")

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

    def create_job(self, job_id: str, scanner: str, params: dict[str, object]) -> None:
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
        result_path: Path | None = None,
        error: str | None = None,
        stage: str | None = None,
        stage_detail: str | None = None,
        number_of_pages: int | None = None,
        batch_count: int | None = None,
        batches_completed: int | None = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        result_value = str(result_path) if result_path else None

        def _update() -> None:
            with self.lock, self._connect() as conn:
                assignments = ["status = ?", "result_path = ?", "error = ?", "updated_at = ?"]
                values: list[object] = [status, result_value, error, now]
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
    def _row_to_dict(row: sqlite3.Row) -> dict[str, object]:
        data = dict(row)
        params = data.get("params")
        data["params"] = json.loads(params) if params else None
        raw_tags = data.get("tags")
        try:
            parsed = json.loads(raw_tags) if raw_tags else []
        except (TypeError, ValueError):
            parsed = []
        data["tags"] = [t for t in parsed if isinstance(t, str)] if isinstance(parsed, list) else []
        return data

    def get_job(self, job_id: str) -> dict[str, object] | None:
        def _lookup() -> dict[str, object] | None:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM scan_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

        return self._with_schema_retry(_lookup)

    def set_tags(self, job_id: str, tags: list[str]) -> None:
        payload = json.dumps(tags)
        now = datetime.utcnow().isoformat()

        def _set() -> None:
            with self.lock, self._connect() as conn:
                conn.execute(
                    "UPDATE scan_jobs SET tags = ?, updated_at = ? WHERE id = ?",
                    (payload, now, job_id),
                )

        self._with_schema_retry(_set)

    def list_all_tags(self) -> list[str]:
        def _list() -> list[str]:
            with self._connect() as conn:
                rows = conn.execute("SELECT tags FROM scan_jobs").fetchall()
            seen: set[str] = set()
            for row in rows:
                raw = row["tags"] or "[]"
                try:
                    for tag in json.loads(raw):
                        if isinstance(tag, str):
                            seen.add(tag)
                except (TypeError, ValueError):
                    continue
            return sorted(seen)

        return self._with_schema_retry(_list)

    def touch(self, job_id: str) -> None:
        """Bump only updated_at; used as a heartbeat during long subprocess work
        so JobMonitor doesn't mistake an alive worker for a crashed one."""
        now = datetime.utcnow().isoformat()

        def _touch() -> None:
            with self.lock, self._connect() as conn:
                conn.execute(
                    "UPDATE scan_jobs SET updated_at = ? WHERE id = ?",
                    (now, job_id),
                )

        self._with_schema_retry(_touch)

    def update_params(self, job_id: str, params: dict[str, object]) -> None:
        payload = json.dumps(params)
        now = datetime.utcnow().isoformat()

        def _update() -> None:
            with self.lock, self._connect() as conn:
                conn.execute(
                    "UPDATE scan_jobs SET params = ?, updated_at = ? WHERE id = ?",
                    (payload, now, job_id),
                )

        self._with_schema_retry(_update)

    def delete_job(self, job_id: str) -> None:
        def _delete() -> None:
            with self.lock, self._connect() as conn:
                conn.execute("DELETE FROM scan_jobs WHERE id = ?", (job_id,))

        self._with_schema_retry(_delete)

    def list_jobs(self, *, offset: int, limit: int) -> tuple[list[dict[str, object]], int]:
        def _list() -> tuple[list[dict[str, object]], int]:
            with self._connect() as conn:
                total = conn.execute("SELECT COUNT(*) FROM scan_jobs").fetchone()[0]
                rows = conn.execute(
                    "SELECT * FROM scan_jobs ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
            return [self._row_to_dict(row) for row in rows], total

        return self._with_schema_retry(_list)


JOB_STORE = JobStore(DB_PATH)
JOB_QUEUE: "queue.Queue[str | None]" = queue.Queue()
PENDING_JOBS: dict[str, dict[str, object]] = {}
PENDING_LOCK = threading.Lock()
CANCELLED_JOBS: set[str] = set()


TAG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._/:\- ]*[a-z0-9.]$|^[a-z0-9]$")
MAX_TAGS_PER_JOB = 20
MAX_TAG_LENGTH = 40


def normalize_tags(raw: list[str]) -> list[str]:
    """Lowercase, trim, dedupe, sort. Reject malformed entries."""
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in raw:
        if not isinstance(entry, str):
            raise HTTPException(status_code=400, detail="tags must be strings")
        tag = entry.strip().lower()
        if not tag:
            continue
        if len(tag) > MAX_TAG_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"tag too long (max {MAX_TAG_LENGTH} chars): {entry!r}",
            )
        if not TAG_PATTERN.fullmatch(tag):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"tag {entry!r} contains invalid characters; allowed: "
                    "a-z, 0-9, . _ / : - space (cannot start/end with separator)"
                ),
            )
        if tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    if len(normalized) > MAX_TAGS_PER_JOB:
        raise HTTPException(
            status_code=400, detail=f"too many tags (max {MAX_TAGS_PER_JOB})"
        )
    normalized.sort()
    return normalized


def _compute_created_via(params: dict[str, object]) -> str | None:
    """Human-readable lineage hint derived from job params.

    Returns short labels for jobs created via the retry/resume/recovery
    flows so the UI can surface them as a subtitle.
    """
    if not isinstance(params, dict):
        return None
    for key, label in (
        ("retried_from", "retry"),
        ("resumed_from", "resume"),
    ):
        ref = params.get(key)
        if isinstance(ref, str) and ref:
            return f"{label} of {ref[:8]}"
    if params.get("auto_merged_from_resume"):
        return "resume auto-merge"
    if params.get("recovered"):
        return "recovered"
    if params.get("salvaged"):
        return "salvaged"
    return None


def serialize_job(job: dict[str, object]) -> dict[str, object]:
    data = dict(job)
    params = data.pop("params", None) or {}
    # A failed job is recoverable iff its raw scan dir from the previous run
    # is still on disk. Cleanup-on-startup writes the path into params; we
    # re-check the filesystem here so the flag reflects current state.
    recovery_path = params.get("recovery_path") if isinstance(params, dict) else None
    data["recovery_available"] = bool(
        data.get("status") == "failed"
        and recovery_path
        and Path(str(recovery_path)).is_dir()
    )
    data["created_via"] = _compute_created_via(params)
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
    processing_opts: dict[str, object],
    progress_cb: Callable[[str], None] | None = None,
    job_entry: dict[str, object] | None = None,
    job_id: str | None = None,
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
        escl_prefix = f"scanjob-escl-{job_id}-" if job_id else "scanjob-escl-"
        tmpdir = Path(tempfile.mkdtemp(prefix=escl_prefix))
        return ScanResult(pages=pages, raw_dir=tmpdir, raw_paths=[])
    raise RuntimeError(f"Unsupported backend '{backend}' for scanner {scanner_key}")


class JobWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.stop_event = threading.Event()
        self.running_jobs: dict[str, dict[str, object]] = {}

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

    def _process(self, job_id: str, payload: dict[str, object]) -> None:
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

            def update_stage(stage: str, detail: str | None = None) -> None:
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
                        chunk_paths: list[Path] = []
                        for idx in range(total_batches):
                            start = idx * batch_size
                            end = min(start + batch_size, len(pdf.pages))
                            chunk_file = tmp_raw_path.with_name(f"{job_id}_chunk_{idx + 1}.pdf")
                            chunk_pdf = pikepdf.Pdf.new()
                            for page in pdf.pages[start:end]:
                                chunk_pdf.pages.append(page)
                            chunk_pdf.save(chunk_file)
                            chunk_paths.append(chunk_file)

                    ocr_chunk_paths: list[Path] = []
                    for idx, chunk_path in enumerate(chunk_paths, start=1):
                        label = f"batch {idx}/{total_batches}"
                        update_stage("ocr", label)
                        JOB_STORE.update_job(
                            job_id, status="running", stage="ocr", stage_detail=label, batches_completed=idx - 1
                        )
                        ocr_out = chunk_path.with_suffix(".ocr.pdf")
                        run_ocr_on_pdf(chunk_path, ocr_out, image_dpi=job_dpi)
                        ocr_chunk_paths.append(ocr_out)
                        JOB_STORE.update_job(
                            job_id, status="running", stage="ocr", stage_detail=label, batches_completed=idx
                        )

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
            # If this scan was queued as the "remaining pages" half of a Resume
            # flow, automatically composite it with the previously-recovered
            # partial so the user ends up with a single merged PDF.
            auto_merge_with = payload.get("auto_merge_with") if isinstance(payload, dict) else None
            if auto_merge_with:
                try:
                    _queue_auto_merge(parent_id=auto_merge_with, child_id=job_id, child_path=output_path)
                except Exception as merge_exc:
                    print(f"[worker] auto-merge for {job_id} skipped: {merge_exc}")
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
            # If the SANE pipeline got far enough to render pages before the
            # failure, the tempdir survives (scan_with_sane no longer wipes on
            # exception). Stash its location in params so /api/scans/{id}/recover
            # can stitch a PDF from the partial scan without a service restart.
            try:
                raw_dir = _find_recoverable_raw_dir(job_id)
                if raw_dir is not None:
                    existing = JOB_STORE.get_job(job_id) or {}
                    params_map = existing.get("params") if isinstance(existing.get("params"), dict) else {}
                    new_params = dict(params_map or {})
                    new_params["recovery_path"] = str(raw_dir)
                    JOB_STORE.update_params(job_id, new_params)
                    print(f"[worker] job {job_id} recoverable from {raw_dir}")
            except Exception as probe_exc:
                # Recovery-path tagging is best-effort; never let it mask the
                # original failure.
                print(f"[worker] job {job_id} recovery probe failed: {probe_exc}")
        finally:
            self.running_jobs.pop(job_id, None)


JOB_WORKER = JobWorker()


class JobMonitor(threading.Thread):
    """Background thread that monitors running jobs for failures and timeouts."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop_event = threading.Event()

    def shutdown(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        print("[monitor] Job monitor started", flush=True)
        sys.stderr.write("[monitor] Job monitor started\n")
        sys.stderr.flush()
        while not self.stop_event.is_set():
            try:
                self._check_jobs()
            except Exception as exc:
                msg = f"[monitor] Error checking jobs: {exc}\n"
                print(msg, flush=True)
                sys.stderr.write(msg)
                sys.stderr.flush()
            self.stop_event.wait(JOB_MONITOR_INTERVAL)
        print("[monitor] Job monitor stopped", flush=True)
        sys.stderr.write("[monitor] Job monitor stopped\n")
        sys.stderr.flush()

    def _check_jobs(self) -> None:
        now = datetime.utcnow()
        jobs, _ = JOB_STORE.list_jobs(offset=0, limit=100)

        running_or_pending = [j for j in jobs if j.get("status") in {"running", "pending"}]
        if running_or_pending:
            msg = f"[monitor] Checking {len(running_or_pending)} running/pending job(s) out of {len(jobs)} total\n"
            sys.stderr.write(msg)
            sys.stderr.flush()

        for job in jobs:
            job_id = str(job.get("id", ""))
            status = str(job.get("status", ""))

            if status not in {"running", "pending"}:
                continue

            updated_at_str = job.get("updated_at")
            if not updated_at_str:
                continue

            try:
                updated_at = datetime.fromisoformat(str(updated_at_str))
                age_seconds = (now - updated_at).total_seconds()

                stage = str(job.get("stage", "") or "")
                stage_detail = job.get("stage_detail", "")
                threshold = JOB_STALE_THRESHOLDS_BY_STAGE.get(stage, JOB_STALE_THRESHOLD)

                if age_seconds > threshold:
                    # Crop / composite / recover run in their own daemon threads
                    # that aren't part of JOB_WORKER. If that thread is still
                    # alive, the job is progressing — leave it alone entirely.
                    if _background_job_alive(job_id):
                        continue

                    is_in_worker = job_id in JOB_WORKER.running_jobs

                    if not is_in_worker:
                        error_msg = (
                            f"Job became orphaned and was stuck in '{stage}' stage for "
                            f"{int(age_seconds / 60)} minutes without updates. "
                            f"This typically happens when the service is restarted or crashes during processing. "
                            f"Last known stage: {stage}"
                        )
                        if stage_detail:
                            error_msg += f" ({stage_detail})"

                        print(
                            f"[monitor] Marking orphaned job {job_id} as failed (age: {int(age_seconds)}s, stage: {stage})"
                        )
                        JOB_STORE.update_job(job_id, status="failed", error=error_msg, stage="failed")
                    else:
                        job_entry = JOB_WORKER.running_jobs.get(job_id, {})
                        process = job_entry.get("process")

                        if process is not None:
                            try:
                                if hasattr(process, "poll"):
                                    poll_result = process.poll()
                                    if poll_result is not None and poll_result != 0:
                                        error_msg = (
                                            f"Job process exited unexpectedly with code {poll_result} "
                                            f"while in '{stage}' stage after {int(age_seconds / 60)} minutes. "
                                            f"The OCR or scanning process may have crashed."
                                        )
                                        if stage_detail:
                                            error_msg += f" Last stage detail: {stage_detail}"

                                        print(
                                            f"[monitor] Marking job {job_id} as failed (process died with code {poll_result})"
                                        )
                                        JOB_STORE.update_job(job_id, status="failed", error=error_msg, stage="failed")
                                        JOB_WORKER.running_jobs.pop(job_id, None)
                            except Exception as check_exc:
                                print(f"[monitor] Error checking process for job {job_id}: {check_exc}")
                        else:
                            error_msg = (
                                f"Job stuck in '{stage}' stage for {int(age_seconds / 60)} minutes "
                                f"without updates and no process handle available. "
                                f"This may indicate a deadlock or infinite loop in the processing code."
                            )
                            if stage_detail:
                                error_msg += f" Last stage detail: {stage_detail}"

                            print(
                                f"[monitor] Marking stuck job {job_id} as failed (age: {int(age_seconds)}s, no process)"
                            )
                            JOB_STORE.update_job(job_id, status="failed", error=error_msg, stage="failed")
                            JOB_WORKER.running_jobs.pop(job_id, None)

            except (ValueError, TypeError) as exc:
                print(f"[monitor] Error parsing timestamp for job {job_id}: {exc}")
                continue


_cleanup_already_run = False


def cleanup_orphaned_jobs_on_startup() -> None:
    """Mark any running/pending jobs from previous instance as failed."""
    global _cleanup_already_run
    if _cleanup_already_run:
        msg = "[startup] Cleanup already run, skipping\n"
        sys.stderr.write(msg)
        sys.stderr.flush()
        return
    _cleanup_already_run = True

    msg = "[startup] Checking for orphaned jobs from previous run...\n"
    sys.stderr.write(msg)
    sys.stderr.flush()
    print(msg.rstrip(), flush=True)
    jobs, _ = JOB_STORE.list_jobs(offset=0, limit=1000)
    orphaned_count = 0

    for job in jobs:
        job_id = str(job.get("id", ""))
        status = str(job.get("status", ""))

        if status in {"running", "pending"}:
            stage = job.get("stage", "unknown")
            stage_detail = job.get("stage_detail", "")
            created_at = job.get("created_at", "")

            error_msg = (
                f"Job was left in '{status}' status (stage: '{stage}') when the service was restarted. "
                f"The job could not be completed because the process was terminated. "
                f"Please retry the scan if needed."
            )
            if stage_detail:
                error_msg += f" Last progress: {stage_detail}"

            msg = f"[startup] Marking orphaned job {job_id} as failed (was {status}, stage: {stage}, created: {created_at})\n"
            sys.stderr.write(msg)
            sys.stderr.flush()
            print(msg.rstrip(), flush=True)
            JOB_STORE.update_job(job_id, status="failed", error=error_msg, stage="failed")
            # If the SANE pipeline left raw page files in /tmp, record the
            # path so POST /api/scans/{id}/recover can stitch them later.
            raw_dir = _find_recoverable_raw_dir(job_id)
            if raw_dir is not None:
                existing_params = (job.get("params") or {}) if isinstance(job.get("params"), dict) else {}
                new_params = dict(existing_params)
                new_params["recovery_path"] = str(raw_dir)
                JOB_STORE.update_params(job_id, new_params)
                msg = f"[startup] Job {job_id} is recoverable from {raw_dir}\n"
                sys.stderr.write(msg)
                sys.stderr.flush()
                print(msg.rstrip(), flush=True)
            orphaned_count += 1

    if orphaned_count > 0:
        msg = f"[startup] Cleaned up {orphaned_count} orphaned job(s)\n"
    else:
        msg = "[startup] No orphaned jobs found\n"
    sys.stderr.write(msg)
    sys.stderr.flush()
    print(msg.rstrip(), flush=True)


JOB_MONITOR = JobMonitor()




class ScanRequest(BaseModel):
    scanner: str = Field(
        ...,
        description="Scanner identifier from configuration (e.g., 'et3850' or 'es580w')",
        examples=["et3850", "es580w"],
    )
    dpi: int | None = Field(
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
    result_path: str | None = None
    error: str | None = None
    created_at: str
    updated_at: str
    stage: str | None = None
    stage_detail: str | None = None
    number_of_pages: int | None = None
    ocr_batch_count: int | None = None
    ocr_batches_completed: int | None = None
    duration_seconds: float | None = None
    recovery_available: bool = False
    created_via: str | None = None
    tags: list[str] = []

    class Config:
        extra = "ignore"


class ScanJobPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: list[ScanJob]


class ScannerInfo(BaseModel):
    id: str
    label: str
    backend: str
    backend_status: str
    configured_device: str | None = None
    in_use: bool


class ScannerDetails(BaseModel):
    id: str
    label: str
    backend: str
    backend_status: str | None = None
    configured_device: str | None = None
    backend_details: dict[str, Any] | None = None
    in_use: bool


app = FastAPI(
    title="Page the Ripper Service",
    version="1.1.0",
    description="REST interface for Epson scanners via SANE or eSCL with OCR and PDF output.",
    swagger_ui_parameters={
        "operationsSorter": "function (a, b) { var order = { get: 0, post: 1, put: 2, patch: 3, delete: 4, options: 5, head: 6 }; var methodA = order[(a.get('method') || '').toLowerCase()]; var methodB = order[(b.get('method') || '').toLowerCase()]; if (methodA === undefined) { methodA = 99; } if (methodB === undefined) { methodB = 99; } if (methodA === methodB) { return a.get('path').localeCompare(b.get('path')); } return methodA - methodB; }"
    },
)


def _resolve_ui_dist() -> Path | None:
    env = os.getenv("PAGE_RIPPER_UI_DIST")
    candidates = []
    if env:
        candidates.append(Path(env))
    here = Path(__file__).resolve().parent
    candidates.append(here / "ui" / "dist")
    candidates.append(Path("/opt/page-the-ripper/ui/dist"))
    for c in candidates:
        if c.is_dir() and (c / "index.html").is_file():
            return c
    return None


UI_DIST_DIR = _resolve_ui_dist()
RESERVED_PREFIXES = ("api", "docs", "redoc", "openapi.json", "assets", "favicon")

if UI_DIST_DIR is not None:
    assets_dir = UI_DIST_DIR / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(UI_DIST_DIR / "index.html")

    @app.get("/favicon.svg", include_in_schema=False)
    def favicon_svg() -> FileResponse:
        target = UI_DIST_DIR / "favicon.svg"
        if not target.is_file():
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(target)

else:
    _UI_MISSING_NOTE = (
        "Page the Ripper UI bundle not found.\n"
        "Build it with `cd ui && npm install && npm run build` "
        "or set PAGE_RIPPER_UI_DIST to a directory containing index.html.\n"
        "API still available under /api/* and /docs."
    )

    @app.get("/", response_class=PlainTextResponse, include_in_schema=False)
    def index() -> PlainTextResponse:
        return PlainTextResponse(_UI_MISSING_NOTE, status_code=503)


@app.on_event("startup")
def _startup() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_orphaned_jobs_on_startup()
    if not JOB_WORKER.is_alive():
        JOB_WORKER.start()
    if not JOB_MONITOR.is_alive():
        JOB_MONITOR.start()


SHUTDOWN_JOIN_TIMEOUT = float(os.getenv("SCAN_SHUTDOWN_JOIN_TIMEOUT", "900"))  # seconds


@app.on_event("shutdown")
def _shutdown() -> None:
    deadline = time.time() + SHUTDOWN_JOIN_TIMEOUT
    if JOB_MONITOR.is_alive():
        JOB_MONITOR.shutdown()
        JOB_MONITOR.join(timeout=5)
    if JOB_WORKER.is_alive():
        running = list(JOB_WORKER.running_jobs.keys())
        if running:
            # An in-flight scan can take many minutes (long ADF runs, large
            # OCR jobs). Wait long enough that systemd's default 90s SIGKILL
            # is the only thing forcing an abort — the matching
            # TimeoutStopSec lives in debian/page-the-ripper.service.
            print(
                f"[shutdown] waiting up to {SHUTDOWN_JOIN_TIMEOUT:.0f}s for "
                f"{len(running)} in-flight scan(s) to finish: {running}",
                flush=True,
            )
        JOB_WORKER.shutdown()
        JOB_WORKER.join(timeout=max(1.0, deadline - time.time()))
        if JOB_WORKER.is_alive():
            print(
                f"[shutdown] scan worker did not finish within budget; "
                f"orphan cleanup will mark surviving jobs failed on next start",
                flush=True,
            )
    # Crop / composite / recover / auto-merge run in daemon threads that
    # JOB_WORKER doesn't manage. We still need to drain them so their
    # subprocesses (pdftoppm, ocrmypdf) aren't SIGTERM'd mid-flight by the
    # process exit. They register themselves in _ACTIVE_BACKGROUND_JOBS.
    with _ACTIVE_BACKGROUND_LOCK:
        background = {jid: t for jid, t in _ACTIVE_BACKGROUND_JOBS.items() if t.is_alive()}
    if background:
        print(
            f"[shutdown] waiting up to {max(0.0, deadline - time.time()):.0f}s for "
            f"{len(background)} background job(s) to finish: {list(background)}",
            flush=True,
        )
        for jid, thread in background.items():
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                print(f"[shutdown] no time left to join background job {jid}", flush=True)
                break
            thread.join(timeout=remaining)
            if thread.is_alive():
                print(f"[shutdown] background job {jid} still running at budget exhaustion", flush=True)


SCANNER_PROBE_CACHE_TTL = float(os.getenv("SCANNER_PROBE_CACHE_TTL", "300"))  # seconds
_SCANNER_PROBE_CACHE: dict[str, tuple[float, dict[str, object]]] = {}
_SCANNER_PROBE_LOCK = threading.Lock()


def _cached_backend_details(scanner_key: str, entry: dict[str, object], *, refresh: bool) -> dict[str, object]:
    """Return backend probe results from cache when fresh, otherwise re-probe.

    The probe (fetch_sane_backend_details / fetch_escl_backend_details) hits
    `scanimage -L` or an eSCL endpoint and can take many seconds. Callers
    pass refresh=True to bypass the cache.
    """
    backend = str(entry.get("backend", "sane"))
    cache_key = f"{backend}:{scanner_key}"
    now = time.time()
    if not refresh:
        with _SCANNER_PROBE_LOCK:
            cached = _SCANNER_PROBE_CACHE.get(cache_key)
            if cached is not None:
                ts, value = cached
                if now - ts <= SCANNER_PROBE_CACHE_TTL:
                    return value
    if backend == "sane":
        details = fetch_sane_backend_details(entry)
    elif backend == "escl":
        details = fetch_escl_backend_details(entry)
    else:
        details = {"status": "error", "error": f"Unsupported backend '{backend}'"}
    with _SCANNER_PROBE_LOCK:
        _SCANNER_PROBE_CACHE[cache_key] = (time.time(), details)
    return details


def _invalidate_scanner_probe_cache() -> None:
    with _SCANNER_PROBE_LOCK:
        _SCANNER_PROBE_CACHE.clear()


@app.get("/api/scanners", tags=["scanners"], summary="List configured scanners", response_model=list[ScannerInfo])
def list_scanners(
    refresh: bool = Query(False, description="Bypass the server-side probe cache and re-enumerate devices."),
) -> list[ScannerInfo]:
    if refresh:
        _invalidate_scanner_probe_cache()
    out: list[dict[str, object]] = []
    for key, entry in SCANNER_REGISTRY.items():
        backend = entry.get("backend", "sane")
        in_use = any((job.get("payload", {}) or {}).get("scanner") == key for job in JOB_WORKER.running_jobs.values())
        details = _cached_backend_details(key, entry, refresh=refresh)
        if backend == "sane":
            backend_status = details.get("status", "unknown")
            configured = details.get("configured", {}) if isinstance(details, dict) else {}
            configured_device = configured.get("sane_device") or details.get("resolved_device")
        elif backend == "escl":
            backend_status = details.get("status", "unknown")
            configured_device = details.get("default_url")
        else:
            backend_status = "error"
            configured_device = None
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
def get_scanner_details(
    scanner_id: str,
    refresh: bool = Query(False, description="Bypass the server-side probe cache and re-enumerate devices."),
) -> ScannerDetails:
    entry = SCANNER_REGISTRY.get(scanner_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Unknown scanner '{scanner_id}'")
    backend = entry.get("backend", "sane")
    details: dict[str, object] = {
        "id": scanner_id,
        "label": entry.get("label"),
        "backend": backend,
        "default_color_mode": entry.get("default_color_mode", COLOR_MODE),
    }
    if backend in ("sane", "escl"):
        details["backend_details"] = _cached_backend_details(scanner_id, entry, refresh=refresh)
    else:
        details["backend_details"] = {"status": "error", "error": f"Unsupported backend '{backend}'"}
    in_use = any(
        (job.get("payload", {}) or {}).get("scanner") == scanner_id for job in JOB_WORKER.running_jobs.values()
    )
    details["backend_status"] = (
        details.get("backend_details", {}).get("status") if isinstance(details.get("backend_details"), dict) else None
    )
    details["configured_device"] = (
        details.get("backend_details", {}).get("configured", {}).get("sane_device")
        if backend == "sane"
        else details.get("backend_details", {}).get("default_url")
    )
    details["in_use"] = in_use
    # prune default_color_mode from response_model; backend_details may still contain it
    details.pop("default_color_mode", None)
    return ScannerDetails(**details)


@app.get("/api/system", tags=["system"], summary="Overall system health and status")
def get_system_status() -> dict[str, object]:
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
    tags: list[str] = Query(
        default_factory=list,
        description=(
            "Filter to jobs tagged with ALL of the provided tags. Pass the parameter "
            "multiple times: ?tags=foo&tags=bar (AND semantics)."
        ),
    ),
) -> ScanJobPage:
    required_tags = normalize_tags(tags) if tags else []
    if not required_tags:
        offset = (page - 1) * page_size
        items, total = JOB_STORE.list_jobs(offset=offset, limit=page_size)
        return ScanJobPage(
            page=page,
            page_size=page_size,
            total=total,
            items=[ScanJob(**serialize_job(item)) for item in items],
        )
    # When filtering: fetch all rows (capped) then post-filter in Python.
    # The scale here is small enough (job counts in the hundreds) to make
    # the simpler approach correct without paging through SQLite JSON tricks.
    raw_items, _ = JOB_STORE.list_jobs(offset=0, limit=10000)
    required = set(required_tags)
    matching = [
        item
        for item in raw_items
        if required.issubset({t for t in (item.get("tags") or []) if isinstance(t, str)})
    ]
    total = len(matching)
    start = (page - 1) * page_size
    page_items = matching[start : start + page_size]
    return ScanJobPage(
        page=page,
        page_size=page_size,
        total=total,
        items=[ScanJob(**serialize_job(item)) for item in page_items],
    )


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
    # Also clean up any leftover raw-scan tempdir for this job. Failed scans
    # leave their pages on disk so the user can Recover/Resume them; once the
    # user deletes the job they're saying "I don't need those any more", so
    # we free the /tmp space too.
    params = job.get("params") or {}
    raw_dir: Path | None = None
    if isinstance(params, dict):
        rp = params.get("recovery_path")
        if rp:
            candidate = Path(str(rp))
            if candidate.is_dir():
                raw_dir = candidate
    if raw_dir is None:
        # Fallback: glob /tmp for the encoded job_id prefix.
        raw_dir = _find_recoverable_raw_dir(job_id)
    if raw_dir is not None and raw_dir.is_dir():
        try:
            shutil.rmtree(raw_dir, ignore_errors=True)
        except OSError:
            pass
    JOB_STORE.delete_job(job_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


class TagsRequest(BaseModel):
    tags: list[str] = Field(
        default_factory=list,
        description=(
            "Full tag list to apply to the job (idempotent). Server normalizes: "
            "lowercases, trims, dedupes, sorts. Allowed chars: a-z, 0-9, . _ / : - space. "
            f"Max {MAX_TAGS_PER_JOB} tags per job, {MAX_TAG_LENGTH} chars each."
        ),
    )


class TagsResponse(BaseModel):
    tags: list[str]


@app.put(
    "/api/scans/{job_id}/tags",
    tags=["scans"],
    summary="Replace the tag list for a job (idempotent)",
    response_model=TagsResponse,
)
def set_scan_tags(job_id: str, req: TagsRequest) -> TagsResponse:
    if JOB_STORE.get_job(job_id) is None:
        raise HTTPException(status_code=404, detail="Job not found")
    normalized = normalize_tags(req.tags)
    JOB_STORE.set_tags(job_id, normalized)
    return TagsResponse(tags=normalized)


@app.get(
    "/api/tags",
    tags=["scans"],
    summary="List every tag currently applied to at least one job",
    response_model=list[str],
)
def list_all_tags() -> list[str]:
    return JOB_STORE.list_all_tags()


# ---------------------------------------------------------------------------
# Page preview, crop, and composite features
# ---------------------------------------------------------------------------


def _completed_pdf_path(job_id: str) -> Path:
    job = JOB_STORE.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail=f"Job is not completed (status={job.get('status')})")
    rp = job.get("result_path")
    if not rp:
        raise HTTPException(status_code=409, detail="Job has no result PDF")
    p = Path(str(rp))
    if not p.is_file():
        raise HTTPException(status_code=410, detail="Result PDF is missing on disk")
    return p


def _pdf_page_dimensions(pdf_path: Path) -> list[dict[str, float]]:
    pages: list[dict[str, float]] = []
    with pikepdf.Pdf.open(pdf_path) as src:
        for idx, page in enumerate(src.pages):
            mediabox = page.mediabox
            width = float(mediabox[2]) - float(mediabox[0])
            height = float(mediabox[3]) - float(mediabox[1])
            pages.append({"index": idx, "width_pt": width, "height_pt": height})
    return pages


def _render_pdf_page_jpeg(pdf_path: Path, page_index: int, *, max_width: int, quality: int = 80) -> bytes:
    with tempfile.TemporaryDirectory(prefix="ptr-preview-") as wd:
        prefix = Path(wd) / "p"
        cmd = [
            "pdftoppm",
            "-jpeg",
            "-jpegopt",
            f"quality={quality}",
            "-scale-to-x",
            str(max_width),
            "-scale-to-y",
            "-1",
            "-f",
            str(page_index + 1),
            "-l",
            str(page_index + 1),
            "-singlefile",
            str(pdf_path),
            str(prefix),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=60)
        except subprocess.CalledProcessError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"pdftoppm failed: {exc.stderr.decode(errors='replace')[:400]}",
            ) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail="pdftoppm not available on host") from exc
        out_file = prefix.with_suffix(".jpg")
        if not out_file.is_file():
            raise HTTPException(status_code=500, detail="pdftoppm produced no output")
        return out_file.read_bytes()


class PdfPageInfo(BaseModel):
    index: int
    width_pt: float
    height_pt: float


class PdfPagesResponse(BaseModel):
    page_count: int
    pages: list[PdfPageInfo]


@app.get(
    "/api/scans/{job_id}/view",
    tags=["scans"],
    summary="Stream the completed PDF inline for browser preview",
)
def view_scan_pdf(job_id: str) -> FileResponse:
    pdf_path = _completed_pdf_path(job_id)
    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{pdf_path.name}"',
            "Cache-Control": "private, max-age=60",
        },
    )


@app.get(
    "/api/scans/{job_id}/pages",
    tags=["scans"],
    summary="List PDF pages and dimensions for a completed scan",
    response_model=PdfPagesResponse,
)
def list_scan_pages(job_id: str) -> PdfPagesResponse:
    pdf_path = _completed_pdf_path(job_id)
    pages = _pdf_page_dimensions(pdf_path)
    return PdfPagesResponse(page_count=len(pages), pages=[PdfPageInfo(**p) for p in pages])


@app.get(
    "/api/scans/{job_id}/pages/{page_index}/preview.jpg",
    tags=["scans"],
    summary="Rendered JPEG preview of a single PDF page",
    response_class=Response,
)
def get_scan_page_preview(
    job_id: str,
    page_index: int,
    max_width: int = Query(1000, ge=100, le=4000, description="Maximum width of the rendered JPEG in pixels"),
) -> Response:
    pdf_path = _completed_pdf_path(job_id)
    pages = _pdf_page_dimensions(pdf_path)
    if page_index < 0 or page_index >= len(pages):
        raise HTTPException(status_code=404, detail="Page index out of range")
    data = _render_pdf_page_jpeg(pdf_path, page_index, max_width=max_width)
    return Response(content=data, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=300"})


class CropRequest(BaseModel):
    box: list[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description=(
            "Normalized crop rectangle as [x0, y0, x1, y1] in 0..1 coordinates "
            "of the *rotated* page (origin top-left). When rotation is non-zero, "
            "the box is interpreted in the rotated coordinate space."
        ),
        examples=[[0.0, 0.05, 1.0, 0.95]],
    )
    reocr: bool = Field(True, description="Re-run OCR on the cropped output to keep it searchable.")
    dpi: int = Field(300, ge=75, le=1200, description="Render DPI used to rasterize each page during cropping.")
    rotation: float = Field(
        0.0,
        ge=-360.0,
        le=360.0,
        description=(
            "Clockwise rotation in degrees applied to every page before cropping. "
            "0 means no rotation. Supports both 90-degree presets and free-form angles."
        ),
    )


class CompositeRequest(BaseModel):
    sources: list[str] = Field(
        ...,
        min_length=1,
        description="Ordered list of completed scan job IDs to concatenate. Order is preserved.",
    )
    reocr: bool = Field(True, description="Re-run OCR on the merged output to keep the text layer aligned.")


def _validate_crop_box(box: list[float]) -> tuple[float, float, float, float]:
    x0, y0, x1, y1 = box
    for v in box:
        if not (0.0 <= v <= 1.0):
            raise HTTPException(status_code=400, detail="Crop box values must lie in [0, 1]")
    if x1 - x0 < 0.02 or y1 - y0 < 0.02:
        raise HTTPException(status_code=400, detail="Crop box is degenerate (width or height < 2%)")
    return x0, y0, x1, y1


def _run_crop_job(
    new_id: str,
    src_pdf: Path,
    box: tuple[float, float, float, float],
    dpi: int,
    reocr: bool,
    rotation: float = 0.0,
) -> None:
    out_path = OUTPUT_DIR / f"{new_id}.pdf"
    workdir: Path | None = None
    _register_background_job(new_id, threading.current_thread())
    try:
        JOB_STORE.update_job(new_id, status="running", stage="cropping", stage_detail="rasterizing pages")
        with tempfile.TemporaryDirectory(prefix=f"crop-{new_id}-") as wd:
            workdir = Path(wd)
            prefix = workdir / "page"
            subprocess.run(
                [
                    "pdftoppm",
                    "-jpeg",
                    "-jpegopt",
                    "quality=90",
                    "-r",
                    str(dpi),
                    str(src_pdf),
                    str(prefix),
                ],
                check=True,
                capture_output=True,
                timeout=600,
            )
            rendered = sorted(workdir.glob("page-*.jpg"))
            if not rendered:
                raise RuntimeError("pdftoppm produced no pages")
            JOB_STORE.update_job(
                new_id,
                status="running",
                stage="cropping",
                stage_detail=f"cropping {len(rendered)} pages",
                number_of_pages=len(rendered),
            )
            x0, y0, x1, y1 = box
            # Frontend reports clockwise degrees; PIL.Image.rotate is CCW.
            pil_rotation = -float(rotation) % 360.0
            cropped_images: list[Image.Image] = []
            for rendered_path in rendered:
                with Image.open(rendered_path) as img:
                    img.load()
                    if pil_rotation:
                        # expand=True grows the canvas to fit the rotated image;
                        # white fill keeps the visual consistent with scan paper.
                        img = img.rotate(
                            pil_rotation,
                            resample=Image.BICUBIC,
                            expand=True,
                            fillcolor="white" if img.mode in {"L", "RGB"} else None,
                        )
                    w, h = img.size
                    left = int(round(x0 * w))
                    upper = int(round(y0 * h))
                    right = int(round(x1 * w))
                    lower = int(round(y1 * h))
                    left = max(0, min(left, w - 1))
                    right = max(left + 1, min(right, w))
                    upper = max(0, min(upper, h - 1))
                    lower = max(upper + 1, min(lower, h))
                    cropped_images.append(img.crop((left, upper, right, lower)).copy())

            JOB_STORE.update_job(new_id, status="running", stage="assembling", stage_detail="building PDF")
            intermediate = out_path if not reocr else workdir / "merged.pdf"
            create_pdf_from_images(cropped_images, intermediate, dpi=dpi)

            if reocr:
                JOB_STORE.update_job(new_id, status="running", stage="ocr", stage_detail="re-running OCR")
                with _Heartbeat(new_id):
                    run_ocr_on_pdf(intermediate, out_path, image_dpi=dpi)
        JOB_STORE.update_job(
            new_id,
            status="completed",
            result_path=out_path,
            stage="finalized",
            stage_detail=None,
        )
    except subprocess.CalledProcessError as exc:
        msg = exc.stderr.decode(errors="replace")[:400] if exc.stderr else str(exc)
        JOB_STORE.update_job(new_id, status="failed", error=f"crop pipeline failed: {msg}", stage="error")
    except Exception as exc:  # pragma: no cover - defensive
        JOB_STORE.update_job(new_id, status="failed", error=str(exc), stage="error")
        if out_path.exists():
            try:
                out_path.unlink()
            except OSError:
                pass
    finally:
        _unregister_background_job(new_id)


def _run_composite_job(new_id: str, source_paths: list[Path], reocr: bool) -> None:
    out_path = OUTPUT_DIR / f"{new_id}.pdf"
    _register_background_job(new_id, threading.current_thread())
    try:
        JOB_STORE.update_job(new_id, status="running", stage="merging", stage_detail=f"merging {len(source_paths)} PDFs")
        with tempfile.TemporaryDirectory(prefix=f"composite-{new_id}-") as wd:
            workdir = Path(wd)
            intermediate = out_path if not reocr else workdir / "merged.pdf"
            with pikepdf.Pdf.new() as merged:
                total_pages = 0
                for src in source_paths:
                    with pikepdf.Pdf.open(src) as part:
                        merged.pages.extend(part.pages)
                        total_pages += len(part.pages)
                merged.save(intermediate)
            JOB_STORE.update_job(
                new_id,
                status="running",
                stage="merging",
                stage_detail=f"merged {total_pages} pages",
                number_of_pages=total_pages,
            )
            if reocr:
                JOB_STORE.update_job(new_id, status="running", stage="ocr", stage_detail="re-running OCR")
                with _Heartbeat(new_id):
                    run_ocr_on_pdf(intermediate, out_path)
        JOB_STORE.update_job(
            new_id,
            status="completed",
            result_path=out_path,
            stage="finalized",
            stage_detail=None,
        )
    except Exception as exc:  # pragma: no cover - defensive
        JOB_STORE.update_job(new_id, status="failed", error=str(exc), stage="error")
        if out_path.exists():
            try:
                out_path.unlink()
            except OSError:
                pass
    finally:
        _unregister_background_job(new_id)


def _queue_auto_merge(*, parent_id: str, child_id: str, child_path: Path) -> None:
    """Spin up an automatic composite of [parent_id, child_id] after the child
    completes (Resume flow). Skips silently if the parent isn't in a state
    that can be merged — caller wraps in try/except so a missing merge
    target can never fail the underlying scan."""
    parent = JOB_STORE.get_job(parent_id)
    if not parent or parent.get("status") != "completed":
        raise RuntimeError(f"parent job {parent_id} is not completed")
    parent_result = parent.get("result_path")
    if not parent_result:
        raise RuntimeError(f"parent job {parent_id} has no result PDF")
    parent_path = Path(str(parent_result))
    if not parent_path.is_file():
        raise RuntimeError(f"parent job {parent_id} result file missing on disk: {parent_path}")
    composite_id = uuid.uuid4().hex
    JOB_STORE.create_job(
        composite_id,
        scanner="__composite__",
        params={
            "sources": [parent_id, child_id],
            "reocr": False,
            "auto_merged_from_resume": True,
        },
    )
    threading.Thread(
        target=_run_composite_job,
        args=(composite_id, [parent_path, child_path], False),
        daemon=True,
        name=f"automerge-{composite_id[:8]}",
    ).start()
    print(f"[worker] queued auto-merge composite {composite_id} from [{parent_id}, {child_id}]")


RECOVERABLE_PAGE_GLOBS = ("page-*.png", "page-*.jpg", "page-*.jpeg")


def _find_recoverable_raw_dir(job_id: str) -> Path | None:
    """Locate a SANE scan tempdir left behind by a crashed run of `job_id`.

    Returns the directory only if it exists, contains at least one rendered
    page file, and matches the prefix encoded by `dispatch_scan` /
    `scan_with_sane` for that job id.
    """
    tmp_root = Path(tempfile.gettempdir())
    patterns = (
        f"scanjob-{job_id}-*",
        f"scanjob-escl-{job_id}-*",
    )
    for pattern in patterns:
        for candidate in sorted(tmp_root.glob(pattern), reverse=True):
            if not candidate.is_dir():
                continue
            for page_glob in RECOVERABLE_PAGE_GLOBS:
                if next(candidate.glob(page_glob), None) is not None:
                    return candidate
    return None


class RecoverRequest(BaseModel):
    reocr: bool = Field(False, description="Re-run OCR on the recovered PDF (slow on a Pi).")


def _run_recovery_job(job_id: str, raw_dir: Path, reocr: bool) -> None:
    out_path = OUTPUT_DIR / f"{job_id}.pdf"
    _register_background_job(job_id, threading.current_thread())
    try:
        JOB_STORE.update_job(
            job_id,
            status="running",
            stage="recovering",
            stage_detail="reading raw pages",
        )
        pages: list[Path] = []
        for glob_pat in RECOVERABLE_PAGE_GLOBS:
            pages.extend(raw_dir.glob(glob_pat))
        pages.sort()
        if not pages:
            raise RuntimeError("no raw pages found in recovery dir")

        JOB_STORE.update_job(
            job_id,
            status="running",
            stage="recovering",
            stage_detail=f"stitching {len(pages)} pages",
            number_of_pages=len(pages),
        )

        streams: list[bytes] = []
        for p in pages:
            with Image.open(p) as img:
                img.load()
                if img.mode not in {"RGB", "L"}:
                    img = img.convert("L")
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=85, optimize=True)
                streams.append(buf.getvalue())

        layout = img2pdf.get_fixed_dpi_layout_fun((300, 300))
        intermediate = out_path if not reocr else (raw_dir.parent / f"{job_id}-pre.pdf")
        intermediate.parent.mkdir(parents=True, exist_ok=True)
        intermediate.write_bytes(img2pdf.convert(streams, layout_fun=layout))

        if reocr:
            JOB_STORE.update_job(job_id, status="running", stage="ocr", stage_detail="re-running OCR")
            with _Heartbeat(job_id):
                run_ocr_on_pdf(intermediate, out_path, image_dpi=300)
            try:
                intermediate.unlink()
            except OSError:
                pass

        # Drop recovery_path from params so the icon disappears; tag the
        # job as having been recovered for the audit trail.
        existing = JOB_STORE.get_job(job_id) or {}
        params = existing.get("params") or {}
        if isinstance(params, dict):
            params.pop("recovery_path", None)
            params["recovered"] = True
            params["recovered_from"] = str(raw_dir)
            JOB_STORE.update_params(job_id, params)

        JOB_STORE.update_job(
            job_id,
            status="completed",
            result_path=out_path,
            stage="finalized",
            stage_detail=f"recovered {len(pages)} pages from {raw_dir.name}"
            + ("" if not reocr else " · OCR re-applied"),
        )
    except Exception as exc:  # pragma: no cover - defensive
        JOB_STORE.update_job(
            job_id,
            status="failed",
            error=f"recovery failed: {exc}",
            stage="failed",
        )
    finally:
        _unregister_background_job(job_id)


class ResumeRequest(BaseModel):
    reocr: bool = Field(False, description="Run OCR on the recovered partial before merge.")


class ResumeResponse(BaseModel):
    recovered_id: str = Field(description="Same id as the original failed job, now completed with the partial PDF.")
    new_scan_id: str = Field(description="Newly queued scan job that will be auto-merged with the recovered partial.")


@app.post(
    "/api/scans/{job_id}/resume",
    status_code=202,
    tags=["scans"],
    summary="Recover a failed scan's partial pages and queue a follow-up scan that auto-merges with it",
    response_model=ResumeResponse,
)
def resume_scan(job_id: str, req: ResumeRequest | None = None) -> ResumeResponse:
    job = JOB_STORE.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "failed":
        raise HTTPException(status_code=409, detail=f"Only failed jobs can be resumed (status={job.get('status')})")
    params = job.get("params") or {}
    recovery_path = params.get("recovery_path") if isinstance(params, dict) else None
    raw_dir = Path(str(recovery_path)) if recovery_path else None
    if raw_dir is None or not raw_dir.is_dir():
        raw_dir = _find_recoverable_raw_dir(job_id)
    if raw_dir is None:
        raise HTTPException(status_code=410, detail="No recoverable raw scan dir found to resume from")

    scanner_id = str(job.get("scanner") or "")
    if scanner_id not in SCANNER_REGISTRY:
        raise HTTPException(
            status_code=422,
            detail=f"Original scanner '{scanner_id}' is no longer configured; cannot queue a follow-up scan.",
        )

    # Step 1: recover the partial synchronously so the new scan has a stable
    # merge target by the time it completes. Recovery without OCR is fast
    # (PNG -> JPEG -> img2pdf), bounded by page count, not network/OCR.
    reocr = bool(req.reocr) if req is not None else False
    _run_recovery_job(job_id, raw_dir, reocr)

    recovered = JOB_STORE.get_job(job_id) or {}
    if recovered.get("status") != "completed":
        raise HTTPException(
            status_code=500,
            detail=f"Recovery of the partial scan did not complete cleanly: {recovered.get('error')}",
        )

    # Step 2: queue a fresh scan with the original scanner/DPI. The auto_merge_with
    # field on the payload tells the worker to composite the result with the
    # recovered partial when the new scan finishes.
    entry = SCANNER_REGISTRY[scanner_id]
    dpi = int((params.get("dpi") if isinstance(params, dict) else None) or DPI)
    color_mode = str(entry.get("default_color_mode", COLOR_MODE))
    processing_opts = {"do_crop": False, "auto_rotate": False, "crop_box": None}
    new_id = uuid.uuid4().hex
    JOB_STORE.create_job(
        new_id,
        scanner_id,
        {"dpi": dpi, "auto_merge_with": job_id, "resumed_from": job_id},
    )
    payload = {
        "scanner": scanner_id,
        "dpi": dpi,
        "color_mode": color_mode,
        "processing_opts": processing_opts,
        "auto_merge_with": job_id,
    }
    with PENDING_LOCK:
        PENDING_JOBS[new_id] = payload
    JOB_QUEUE.put(new_id)
    return ResumeResponse(recovered_id=job_id, new_scan_id=new_id)


@app.post(
    "/api/scans/{job_id}/retry",
    status_code=202,
    tags=["scans"],
    summary="Re-run a failed __crop__ or __composite__ job with its original parameters (new job_id)",
    response_model=ScanCreateResponse,
)
def retry_scan(job_id: str) -> ScanCreateResponse:
    job = JOB_STORE.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "failed":
        raise HTTPException(status_code=409, detail=f"Only failed jobs can be retried (status={job.get('status')})")
    scanner = str(job.get("scanner") or "")
    params = job.get("params") or {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=422, detail="Original job params are missing or unreadable")

    if scanner == "__crop__":
        source = params.get("source")
        raw_box = params.get("box")
        if not source or not raw_box:
            raise HTTPException(status_code=422, detail="Original crop is missing source or box; can't retry")
        src_pdf = _completed_pdf_path(str(source))
        validated_box = _validate_crop_box(list(raw_box))
        reocr = bool(params.get("reocr", True))
        dpi = int(params.get("dpi") or 300)
        rotation = float(params.get("rotation") or 0.0)
        new_id = uuid.uuid4().hex
        JOB_STORE.create_job(
            new_id,
            scanner="__crop__",
            params={
                "source": str(source),
                "box": list(validated_box),
                "reocr": reocr,
                "dpi": dpi,
                "rotation": rotation,
                "retried_from": job_id,
            },
        )
        threading.Thread(
            target=_run_crop_job,
            args=(new_id, src_pdf, validated_box, dpi, reocr, rotation),
            daemon=True,
            name=f"crop-retry-{new_id[:8]}",
        ).start()
        return ScanCreateResponse(job_id=new_id, status="pending", stage="queued", duration_seconds=0.0)

    if scanner == "__composite__":
        raw_sources = params.get("sources")
        if not isinstance(raw_sources, list) or not raw_sources:
            raise HTTPException(status_code=422, detail="Original composite has no sources; can't retry")
        # _completed_pdf_path validates each source still exists.
        source_paths = [_completed_pdf_path(str(sid)) for sid in raw_sources]
        reocr = bool(params.get("reocr", False))
        new_id = uuid.uuid4().hex
        JOB_STORE.create_job(
            new_id,
            scanner="__composite__",
            params={
                "sources": [str(sid) for sid in raw_sources],
                "reocr": reocr,
                "retried_from": job_id,
            },
        )
        threading.Thread(
            target=_run_composite_job,
            args=(new_id, source_paths, reocr),
            daemon=True,
            name=f"composite-retry-{new_id[:8]}",
        ).start()
        return ScanCreateResponse(job_id=new_id, status="pending", stage="queued", duration_seconds=0.0)

    raise HTTPException(
        status_code=422,
        detail=f"Retry is supported for __crop__ and __composite__ jobs (this is {scanner!r})",
    )


@app.post(
    "/api/scans/{job_id}/recover",
    status_code=202,
    tags=["scans"],
    summary="Rebuild a PDF for a failed scan from its leftover raw pages",
    response_model=ScanCreateResponse,
)
def recover_scan(job_id: str, req: RecoverRequest | None = None) -> ScanCreateResponse:
    job = JOB_STORE.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "failed":
        raise HTTPException(status_code=409, detail=f"Only failed jobs can be recovered (status={job.get('status')})")
    params = job.get("params") or {}
    recovery_path = params.get("recovery_path") if isinstance(params, dict) else None
    raw_dir = Path(str(recovery_path)) if recovery_path else None
    if raw_dir is None or not raw_dir.is_dir():
        # Re-probe in case the path moved or wasn't recorded.
        raw_dir = _find_recoverable_raw_dir(job_id)
    if raw_dir is None:
        raise HTTPException(status_code=410, detail="No recoverable raw scan dir found")

    reocr = bool(req.reocr) if req is not None else False
    threading.Thread(
        target=_run_recovery_job,
        args=(job_id, raw_dir, reocr),
        daemon=True,
        name=f"recover-{job_id[:8]}",
    ).start()
    return ScanCreateResponse(job_id=job_id, status="pending", stage="queued", duration_seconds=0.0)


@app.post(
    "/api/scans/{job_id}/crop",
    status_code=202,
    tags=["scans"],
    summary="Create a new cropped PDF from a completed scan",
    response_model=ScanCreateResponse,
)
def create_crop(job_id: str, req: CropRequest) -> ScanCreateResponse:
    src_pdf = _completed_pdf_path(job_id)
    box = _validate_crop_box(req.box)
    new_id = uuid.uuid4().hex
    JOB_STORE.create_job(
        new_id,
        scanner="__crop__",
        params={
            "source": job_id,
            "box": list(box),
            "reocr": req.reocr,
            "dpi": req.dpi,
            "rotation": req.rotation,
        },
    )
    threading.Thread(
        target=_run_crop_job,
        args=(new_id, src_pdf, box, req.dpi, req.reocr, req.rotation),
        daemon=True,
        name=f"crop-{new_id[:8]}",
    ).start()
    return ScanCreateResponse(job_id=new_id, status="pending", stage="queued", duration_seconds=0.0)


@app.post(
    "/api/scans/composite",
    status_code=202,
    tags=["scans"],
    summary="Create a new PDF by concatenating completed scans in the given order",
    response_model=ScanCreateResponse,
)
def create_composite(req: CompositeRequest) -> ScanCreateResponse:
    source_paths: list[Path] = []
    for sid in req.sources:
        source_paths.append(_completed_pdf_path(sid))
    if len(source_paths) < 1:
        raise HTTPException(status_code=400, detail="At least one source is required")
    new_id = uuid.uuid4().hex
    JOB_STORE.create_job(
        new_id,
        scanner="__composite__",
        params={"sources": list(req.sources), "reocr": req.reocr},
    )
    threading.Thread(
        target=_run_composite_job,
        args=(new_id, source_paths, req.reocr),
        daemon=True,
        name=f"composite-{new_id[:8]}",
    ).start()
    return ScanCreateResponse(job_id=new_id, status="pending", stage="queued", duration_seconds=0.0)


if UI_DIST_DIR is not None:

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str, request: Request) -> FileResponse:
        head = full_path.split("/", 1)[0]
        if head in RESERVED_PREFIXES:
            raise HTTPException(status_code=404, detail="Not found")
        accept = request.headers.get("accept", "")
        if full_path and "text/html" not in accept:
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(UI_DIST_DIR / "index.html")


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
