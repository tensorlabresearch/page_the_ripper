"""Standalone scanning CLI that mirrors the Page the Ripper pipeline.

This script runs entirely on the local machine. It reads the same
`scanner.cfg` entries that the API uses, talks to scanners via the local
SANE stack (e.g., AirSane devices exposed over the network), applies the
usual clean‑up/rotation steps, builds a PDF, and runs OCRmyPDF to produce
searchable output.
"""

from __future__ import annotations

import argparse
import configparser
import io
import os
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import img2pdf
except ImportError as exc:  # pragma: no cover - dependency missing at runtime
    raise SystemExit(
        "img2pdf is not installed in this Python environment. "
        "Run 'pip install img2pdf' (using the same interpreter you use for this script)."
    ) from exc

import numpy as np
import ocrmypdf
from ocrmypdf.exceptions import ExitCodeException, MissingDependencyError
from PIL import Image, ImageFilter, ImageOps
import pytesseract

CONFIG_PATH = Path(os.getenv("SCANNER_CFG", "scanner.cfg"))
DEFAULT_OUTPUT_DIR = Path(os.getenv("SCAN_OUTPUT_DIR", "scans")).expanduser()
TARGET_WIDTH = int(os.getenv("SCAN_TARGET_WIDTH", "405"))
TARGET_HEIGHT = int(os.getenv("SCAN_TARGET_HEIGHT", "636"))

DEFAULT_CONFIG = {
    "defaults": {
        "dpi": "600",
        "color_mode": "Grayscale8",
        "source": "Flatbed",
        "duplex": "false",
        "extra_args": "",
        "sane_hint": "Scanner",
    }
}

WHITE_THRESHOLD = int(os.getenv("SCAN_WHITE_THRESHOLD", "235"))
RAW_NAME_PATTERN = "page-%03d.png"


@dataclass
class ScannerEntry:
    key: str
    label: str
    sane_hint: str
    sane_device: str
    dpi: int
    color_mode: str
    source: str
    duplex: bool
    extra_args: str
    page_width_mm: float
    page_height_mm: float
    batch_count: Optional[int]
    allow_remote: bool


def load_config(path: Path) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read_dict(DEFAULT_CONFIG)
    if path.exists():
        cfg.read(path)
    return cfg


def build_registry(cfg: configparser.ConfigParser) -> Dict[str, ScannerEntry]:
    registry: Dict[str, ScannerEntry] = {}
    for section in cfg.sections():
        if not section.startswith("scanner:"):
            continue
        key = section.split(":", 1)[1]
        label = cfg.get(section, "label", fallback=key.upper())
        batch_raw = cfg.get(section, "batch_count", fallback="").strip()
        batch_val = int(batch_raw) if batch_raw else None
        entry = ScannerEntry(
            key=key,
            label=label,
            sane_hint=cfg.get(section, "sane_hint", fallback=label),
            sane_device=cfg.get(section, "sane_device", fallback=""),
            dpi=cfg.getint(section, "dpi", fallback=cfg.getint("defaults", "dpi", fallback=600)),
            color_mode=cfg.get(section, "color_mode", fallback=cfg.get("defaults", "color_mode", fallback="Grayscale8")),
            source=cfg.get(section, "source", fallback=cfg.get("defaults", "source", fallback="")),
            duplex=cfg.getboolean(section, "duplex", fallback=cfg.getboolean("defaults", "duplex", fallback=False)),
            extra_args=cfg.get(section, "extra_args", fallback=""),
            page_width_mm=cfg.getfloat(section, "page_width_mm", fallback=0.0),
            page_height_mm=cfg.getfloat(section, "page_height_mm", fallback=0.0),
            batch_count=batch_val,
            allow_remote=cfg.getboolean(section, "allow_remote", fallback=False),
        )
        registry[key] = entry
    return registry


# ---------- Image helpers ----------


def detect_osd_rotation(img: Image.Image) -> int:
    """Return rotation in degrees (clockwise) suggested by Tesseract OSD."""
    try:
        osd = pytesseract.image_to_osd(img, config="--psm 0")
    except Exception:
        return 0
    for line in osd.splitlines():
        if "Rotate:" in line:
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return 0
    return 0


def trim_white_borders(img: Image.Image) -> Image.Image:
    gray = np.array(img.convert("L"))
    mask = gray < WHITE_THRESHOLD
    if not mask.any():
        return img
    coords = np.argwhere(mask)
    y0, x0 = coords.min(axis=0)
    y1, x1 = coords.max(axis=0) + 1
    return img.crop((x0, y0, x1, y1))


def light_cleanup(img: Image.Image) -> Image.Image:
    cleaned = ImageOps.autocontrast(img)
    cleaned = cleaned.filter(ImageFilter.MedianFilter(size=3))
    return cleaned


def finalize_page(img: Image.Image, *, color_mode: str, auto_rotate: bool, do_crop: bool) -> Image.Image:
    page = img.convert("RGB")
    if auto_rotate:
        rotation = detect_osd_rotation(page)
        if rotation:
            page = page.rotate(-rotation, expand=True)
        elif page.width > page.height:
            page = page.rotate(90, expand=True)
    page = light_cleanup(page)
    if do_crop:
        page = trim_white_borders(page)
    mode = "RGB" if color_mode == "RGB24" else "L"
    page = page.convert(mode)
    if TARGET_WIDTH > 0 and TARGET_HEIGHT > 0:
        if page.width > TARGET_WIDTH or page.height > TARGET_HEIGHT:
            page = ImageOps.contain(page, (TARGET_WIDTH, TARGET_HEIGHT), method=Image.LANCZOS)
    return page


# ---------- SANE helpers ----------


def list_sane_devices() -> List[Tuple[str, str]]:
    proc = subprocess.run(["scanimage", "-L"], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "scanimage -L failed")
    devices = []
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
    if explicit:
        return explicit
    hint_lower = hint.lower()
    for dev_id, desc in list_sane_devices():
        if hint_lower in dev_id.lower() or hint_lower in desc.lower():
            return dev_id
    raise RuntimeError(f"Unable to locate SANE device matching '{hint}'")


def ensure_local_scanner(device_id: str) -> None:
    """Heuristic check to make sure we're targeting a local scanner."""
    # AirSane shares often expose devices like "airscan:"; disallow those when
    # the user explicitly asked for locally-attached scanners only.
    if device_id.startswith("airscan:"):
        raise RuntimeError(
            "This script only uses locally-attached scanners. "
            "Please pick a scanner that macOS exposes directly (not the remote AirSane entry)."
        )


def run_scan(entry: ScannerEntry, *, dpi: int, color_mode: str, batch_dir: Path, device_override: Optional[str] = None) -> List[Path]:
    batch_dir.mkdir(parents=True, exist_ok=True)
    pattern = batch_dir / RAW_NAME_PATTERN
    if device_override:
        device_id = device_override.strip()
        if not device_id:
            raise RuntimeError("Device override cannot be empty")
    else:
        device_id = resolve_sane_device(entry.sane_device, entry.sane_hint)
    if not entry.allow_remote:
        ensure_local_scanner(device_id)
    sane_mode = "Color" if color_mode == "RGB24" else "Gray"
    args = [
        "scanimage",
        f"--device={device_id}",
        f"--resolution={dpi}",
        f"--mode={sane_mode}",
        "--format=png",
        f"--batch={pattern}",
    ]
    if entry.source:
        args.append(f"--source={entry.source}")
    if entry.extra_args:
        args.extend(shlex.split(entry.extra_args))
    if not entry.duplex and "--batch-count" not in entry.extra_args:
        if entry.batch_count is not None:
            args.append(f"--batch-count={entry.batch_count}")
        else:
            args.append("--batch-count=1")
    elif entry.batch_count is not None and f"--batch-count" not in entry.extra_args:
        args.append(f"--batch-count={entry.batch_count}")
    if entry.page_width_mm > 0:
        args.extend(["-x", f"{entry.page_width_mm:g}"])
    if entry.page_height_mm > 0:
        args.extend(["-y", f"{entry.page_height_mm:g}"])

    print("[scan]", " ".join(shlex.quote(a) for a in args))
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "scanimage failed")
    pages = sorted(batch_dir.glob("page-*.png"))
    if not pages:
        raise RuntimeError("scanimage produced no pages")
    return pages


# ---------- PDF helpers ----------


def create_pdf_from_images(pages: Iterable[Image.Image], out_path: Path, *, dpi: int) -> None:
    image_streams: List[bytes] = []
    for img in pages:
        buf = io.BytesIO()
        working = img.convert("RGB") if img.mode not in {"RGB", "L"} else img
        working.save(buf, format="PNG")
        image_streams.append(buf.getvalue())
    layout = img2pdf.get_fixed_dpi_layout_fun((dpi, dpi))
    pdf_bytes = img2pdf.convert(image_streams, layout_fun=layout)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(pdf_bytes)


def run_ocr(input_pdf: Path, output_pdf: Path, *, dpi: int, language: str) -> None:
    try:
        ocrmypdf.ocr(
            str(input_pdf),
            str(output_pdf),
            language=language,
            force_ocr=True,
            deskew=True,
            clean=True,
            clean_final=True,
            rotate_pages=True,
            rotate_pages_threshold=12,
            optimize=3,
            image_dpi=dpi,
            progress_bar=False,
        )
    except ExitCodeException as exc:
        raise RuntimeError(f"ocrmypdf failed ({exc.exit_code}): {exc}") from exc
    except MissingDependencyError as exc:
        raise RuntimeError(f"ocrmypdf dependency missing: {exc}") from exc


def process_pages(raw_paths: List[Path], *, color_mode: str, auto_rotate: bool, do_crop: bool, dest_dir: Optional[Path] = None) -> List[Image.Image]:
    pages: List[Image.Image] = []
    if dest_dir:
        dest_dir.mkdir(parents=True, exist_ok=True)
    for path in raw_paths:
        with Image.open(path) as pil:
            pil.load()
            finalized = finalize_page(pil, color_mode=color_mode, auto_rotate=auto_rotate, do_crop=do_crop)
            pages.append(finalized)
            if dest_dir:
                out_path = dest_dir / path.name
                finalized.save(out_path, format="PNG")
    return pages


# ---------- CLI ----------


COMMANDS = {"scan", "process", "assemble", "ocr", "full"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local scanning helper")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to scanner.cfg (default: %(default)s)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_p = subparsers.add_parser("scan", help="Capture raw pages from a scanner")
    scan_p.add_argument("scanner", help="Scanner key from scanner.cfg")
    scan_p.add_argument("--raw-dir", required=True, help="Directory to store raw PNG pages")
    scan_p.add_argument("--dpi", type=int, default=300, help="DPI to request (default: 300)")
    scan_p.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], help="Override color mode")
    scan_p.add_argument("--device", help="Explicit SANE device identifier to use")

    proc_p = subparsers.add_parser("process", help="Cleanup/crop raw PNG pages")
    proc_p.add_argument("--raw-dir", required=True, help="Directory with raw PNG pages")
    proc_p.add_argument("--processed-dir", required=True, help="Directory to store processed PNG pages")
    proc_p.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], default="Grayscale8")
    proc_p.add_argument("--no-crop", action="store_true", help="Disable cropping")
    proc_p.add_argument("--no-auto-rotate", action="store_true", help="Disable autorotation")

    asm_p = subparsers.add_parser("assemble", help="Assemble processed pages into a PDF")
    asm_p.add_argument("--processed-dir", required=True, help="Directory with processed PNG pages")
    asm_p.add_argument("--output", required=True, help="Destination PDF path")
    asm_p.add_argument("--dpi", type=int, default=300, help="DPI for layout (default: 300)")

    ocr_p = subparsers.add_parser("ocr", help="Run OCR on an existing PDF")
    ocr_p.add_argument("--input", required=True, help="Input PDF path")
    ocr_p.add_argument("--output", required=True, help="Output PDF path")
    ocr_p.add_argument("--dpi", type=int, default=300, help="Image DPI hint (default: 300)")
    ocr_p.add_argument("--language", default=os.getenv("TESSERACT_LANG", "eng"), help="Tesseract language")

    full_p = subparsers.add_parser("full", help="Run the entire pipeline end-to-end")
    full_p.add_argument("scanner", help="Scanner key from scanner.cfg")
    full_p.add_argument("--dpi", type=int, help="Override DPI from config")
    full_p.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], help="Override color mode")
    full_p.add_argument("--output", help="Output PDF path (default: scans/<scanner>_<timestamp>.pdf)")
    full_p.add_argument("--raw-dir", help="Keep raw PNGs here instead of temp dir")
    full_p.add_argument("--keep-processed", help="Persist processed PNGs to this dir")
    full_p.add_argument("--no-crop", action="store_true", help="Disable cropping")
    full_p.add_argument("--no-auto-rotate", action="store_true", help="Disable autorotation")
    full_p.add_argument("--language", default=os.getenv("TESSERACT_LANG", "eng"), help="Tesseract language")
    full_p.add_argument("--device", help="Explicit SANE device identifier to use")

    argv = sys.argv[1:]
    if argv and not argv[0].startswith("-") and argv[0] not in COMMANDS:
        argv = ["full", *argv]
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    registry = build_registry(cfg)

    if args.command == "scan":
        if args.scanner not in registry:
            raise SystemExit(f"Unknown scanner '{args.scanner}'. Check {cfg_path}.")
        entry = registry[args.scanner]
        dpi = args.dpi or entry.dpi
        color_mode = args.color_mode or entry.color_mode
        raw_dir = Path(args.raw_dir).expanduser()
        raw_dir.mkdir(parents=True, exist_ok=True)
        print(f"[scan] writing raw pages to {raw_dir}")
        pages = run_scan(entry, dpi=dpi, color_mode=color_mode, batch_dir=raw_dir, device_override=args.device)
        print(f"[scan] completed; {len(pages)} page(s) captured")
        return

    if args.command == "process":
        raw_dir = Path(args.raw_dir)
        processed_dir = Path(args.processed_dir)
        raw_paths = sorted(raw_dir.glob("page-*.png"))
        if not raw_paths:
            raise SystemExit(f"No page-*.png files found in {raw_dir}")
        process_pages(
            raw_paths,
            color_mode=args.color_mode,
            auto_rotate=not args.no_auto_rotate,
            do_crop=not args.no_crop,
            dest_dir=processed_dir,
        )
        print(f"[process] wrote processed pages to {processed_dir}")
        return

    if args.command == "assemble":
        processed_dir = Path(args.processed_dir)
        output_path = Path(args.output)
        images = []
        for img_path in sorted(processed_dir.glob("page-*.png")):
            with Image.open(img_path) as pil:
                pil.load()
                images.append(pil.copy())
        if not images:
            raise SystemExit(f"No page-*.png files found in {processed_dir}")
        create_pdf_from_images(images, output_path, dpi=args.dpi)
        print(f"[assemble] wrote PDF to {output_path}")
        return

    if args.command == "ocr":
        input_pdf = Path(args.input)
        output_pdf = Path(args.output)
        if not input_pdf.exists():
            raise SystemExit(f"Input PDF {input_pdf} not found")
        run_ocr(input_pdf, output_pdf, dpi=args.dpi, language=args.language)
        print(f"[ocr] wrote searchable PDF to {output_pdf}")
        return

    if args.command == "full":
        if args.scanner not in registry:
            raise SystemExit(f"Unknown scanner '{args.scanner}'. Check {cfg_path}.")
        entry = registry[args.scanner]
        dpi = args.dpi or entry.dpi
        color_mode = args.color_mode or entry.color_mode
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_path = Path(args.output or DEFAULT_OUTPUT_DIR / f"{args.scanner}_{timestamp}.pdf")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        raw_dir_ctx = args.raw_dir
        processed_ctx = args.keep_processed
        raw_dir = Path(args.raw_dir).expanduser() if raw_dir_ctx else None
        processed_dir = Path(args.keep_processed).expanduser() if processed_ctx else None

        with tempfile.TemporaryDirectory(prefix="scan_cli_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            raw_target = raw_dir or (tmpdir_path / "raw")
            raw_target.mkdir(parents=True, exist_ok=True)
            pages = run_scan(entry, dpi=dpi, color_mode=color_mode, batch_dir=raw_target, device_override=args.device)
            processed_target = processed_dir or (tmpdir_path / "processed")
            processed_pages = process_pages(
                sorted(raw_target.glob("page-*.png")),
                color_mode=color_mode,
                auto_rotate=not args.no_auto_rotate,
                do_crop=not args.no_crop,
                dest_dir=processed_target,
            )
            tmp_pdf = tmpdir_path / "raw.pdf"
            create_pdf_from_images(processed_pages, tmp_pdf, dpi=dpi)
            run_ocr(tmp_pdf, output_path, dpi=dpi, language=args.language)
        print(f"[full] wrote searchable PDF to {output_path}")
        return

    raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
