from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence

from PIL import Image

import main


IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}

ESCL_CAPTURE_BY_RUNNER: Dict[Callable[..., object], Callable[..., List[bytes]]] = {
    main.scan_et_3850_platen: main.capture_et_3850_platen_raw,
    main.scan_es_580w_letter_duplex: main.capture_es_580w_letter_duplex_raw,
}


def resolve_color_mode(entry: Dict[str, object], *, requested: str | None, force_color: bool) -> str:
    default_mode = str(entry.get("default_color_mode", main.COLOR_MODE))
    return main.determine_color_mode(requested, force_color=force_color, default_mode=default_mode)


def gather_image_paths(inputs: Sequence[str]) -> List[Path]:
    files: List[Path] = []
    for item in inputs:
        path = Path(item)
        if path.is_dir():
            candidates = sorted(p for p in path.iterdir() if p.suffix.lower() in IMAGE_EXTENSIONS)
            if not candidates:
                raise FileNotFoundError(f"No image files found in directory {path}")
            files.extend(candidates)
        elif path.is_file():
            files.append(path)
        else:
            raise FileNotFoundError(f"Input path not found: {path}")
    if not files:
        raise FileNotFoundError("No input images found")
    return files


def capture_sane(args: argparse.Namespace) -> None:
    registry_entry = main.SCANNER_REGISTRY.get(args.scanner)
    if not registry_entry or registry_entry.get("backend") != "sane":
        raise SystemExit(f"Scanner '{args.scanner}' must be configured with backend 'sane'")

    dpi = args.dpi or main.DPI
    color_mode = resolve_color_mode(registry_entry, requested=args.color_mode, force_color=args.color)
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    options = registry_entry.get("options", {})
    files = main.sane_scan_to_directory(
        options,
        dpi=dpi,
        color_mode=color_mode,
        output_dir=output_dir,
        progress_cb=None,
        job_entry=None,
    )
    for path in files:
        print(path)
    print(f"Captured {len(files)} page(s) into {output_dir}")


def capture_escl(args: argparse.Namespace) -> None:
    registry_entry = main.SCANNER_REGISTRY.get(args.scanner)
    if not registry_entry or registry_entry.get("backend") != "escl":
        raise SystemExit(f"Scanner '{args.scanner}' must be configured with backend 'escl'")
    helper = ESCL_CAPTURE_BY_RUNNER.get(registry_entry.get("runner"))
    if helper is None:
        raise SystemExit(f"Scanner '{args.scanner}' does not expose a raw capture helper yet")
    url = registry_entry.get("default_url")
    if not url:
        raise SystemExit(f"Scanner '{args.scanner}' is missing a default_url in configuration")
    dpi = args.dpi or main.DPI
    color_mode = resolve_color_mode(registry_entry, requested=args.color_mode, force_color=args.color)
    session = main.create_escl_session(auth=registry_entry.get("auth"))
    raw_pages = helper(url, dpi, color_mode, session=session)
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for idx, payload in enumerate(raw_pages, start=1):
        target = output_dir / f"page-{idx:03d}.jpg"
        target.write_bytes(payload)
        written.append(target)
        print(target)
    print(f"Captured {len(written)} page(s) into {output_dir}")


def finalize_pages(args: argparse.Namespace) -> None:
    inputs = gather_image_paths(args.inputs)
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    color_mode = args.color_mode or "Grayscale8"
    processing_opts = {
        "do_crop": not args.no_crop,
        "crop_box": None,
    }

    written = []
    for idx, path in enumerate(inputs, start=1):
        with Image.open(path) as img:
            img.load()
            finalized = main.finalize_page(img, color_mode=color_mode, processing_opts=processing_opts)
        target = output_dir / f"page-{idx:03d}.png"
        finalized.save(target)
        written.append(target)
        print(target)
    print(f"Finalized {len(written)} page(s) into {output_dir}")


def orient_page(args: argparse.Namespace) -> None:
    input_path = Path(args.input).resolve()
    if not input_path.is_file():
        raise SystemExit(f"Input file not found: {input_path}")
    count = max(1, args.count)
    direction = args.direction
    if direction == "flip":
        angle = 180
    elif direction == "left":
        angle = 90 * count
    elif direction == "right":
        angle = -90 * count
    else:
        raise SystemExit(f"Unsupported direction: {direction}")

    with Image.open(input_path) as img:
        fmt = img.format or "PNG"
        rotated = img.rotate(angle, expand=True)
        output_path = Path(args.output).resolve() if args.output else input_path
        rotated.save(output_path, format=fmt)
    print(f"Saved rotated image to {output_path}")


def run_ocr(args: argparse.Namespace) -> None:
    inputs = gather_image_paths(args.inputs)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    combined: List[str] = []
    for idx, path in enumerate(inputs, start=1):
        with Image.open(path) as img:
            img.load()
            text = main.ocr_page(img)
        target = output_dir / f"page-{idx:03d}.txt"
        target.write_text(text)
        combined.append(text)
        print(target)

    if args.combined:
        combined_path = Path(args.combined).resolve()
        combined_path.write_text("\n\n".join(combined))
        print(f"Wrote combined OCR text to {combined_path}")


def build_pdf(args: argparse.Namespace) -> None:
    inputs = gather_image_paths(args.inputs)
    pages: List[Image.Image] = []
    try:
        for path in inputs:
            with Image.open(path) as img:
                img.load()
                pages.append(img.convert("RGB"))

        output_path = Path(args.output).resolve()
        tmp_fd, tmp_pdf = tempfile.mkstemp(suffix=".pdf", prefix="img2pdf_cli_")
        os.close(tmp_fd)
        tmp_pdf_path = Path(tmp_pdf)
        try:
            main.create_pdf_from_images(pages, tmp_pdf_path, dpi=args.dpi or 300)
            main.run_ocr_on_pdf(tmp_pdf_path, output_path, image_dpi=args.dpi or 300)
        finally:
            tmp_pdf_path.unlink(missing_ok=True)
        print(f"Built OCR'd PDF at {output_path}")
    finally:
        for img in pages:
            img.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI helpers for Page the Ripper scanning pipeline.")
    sub = parser.add_subparsers(dest="command")

    sane = sub.add_parser("capture-sane", help="Capture raw pages from a SANE scanner into a directory")
    sane.add_argument("scanner", help="Scanner identifier from configuration (backend must be 'sane')")
    sane.add_argument("output", help="Directory to write page-###.png files")
    sane.add_argument("--dpi", type=int, default=None, help=f"Override DPI (default {main.DPI})")
    sane.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], help="Explicit color mode override")
    sane.add_argument("--color", action="store_true", help="Force RGB24 color mode")
    sane.set_defaults(func=capture_sane)

    escl = sub.add_parser("capture-escl", help="Capture raw pages from an eSCL scanner into a directory")
    escl.add_argument("scanner", help="Scanner identifier from configuration (backend must be 'escl')")
    escl.add_argument("output", help="Directory to write page-###.jpg files")
    escl.add_argument("--dpi", type=int, default=None, help=f"Override DPI (default {main.DPI})")
    escl.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], help="Explicit color mode override")
    escl.add_argument("--color", action="store_true", help="Force RGB24 color mode")
    escl.set_defaults(func=capture_escl)

    finalize = sub.add_parser("finalize", help="Apply cleanup and scaling to scanned pages")
    finalize.add_argument("output", help="Directory to write processed page-###.png files")
    finalize.add_argument("inputs", nargs="+", help="Input image files or directories")
    finalize.add_argument("--color-mode", choices=["Grayscale8", "RGB24"], default="Grayscale8", help="Color mode for output images")
    finalize.add_argument("--no-crop", action="store_true", help="Disable auto-cropping")
    finalize.set_defaults(func=finalize_pages)

    orient = sub.add_parser("orient", help="Rotate a page by quarter turns")
    orient.add_argument("input", help="Input image file")
    orient.add_argument("--direction", choices=["left", "right", "flip"], default="left", help="Rotation direction (left=counter-clockwise)")
    orient.add_argument("--count", type=int, default=1, help="Number of 90° steps for left/right rotations")
    orient.add_argument("--output", help="Output path (defaults to in-place overwrite)")
    orient.set_defaults(func=orient_page)

    ocr = sub.add_parser("ocr", help="Run OCR on pages and emit text files")
    ocr.add_argument("inputs", nargs="+", help="Input image files or directories")
    ocr.add_argument("--output-dir", default=".", help="Directory to write page-###.txt files")
    ocr.add_argument("--combined", help="Optional path for combined OCR output")
    ocr.set_defaults(func=run_ocr)

    pdf = sub.add_parser("build-pdf", help="Assemble an OCR'd PDF from processed pages")
    pdf.add_argument("output", help="Destination PDF path")
    pdf.add_argument("inputs", nargs="+", help="Input image files or directories")
    pdf.add_argument("--dpi", type=int, default=300, help="Assumed DPI for layout when building the PDF")
    pdf.set_defaults(func=build_pdf)

    return parser


def main_cli(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    try:
        args.func(args)
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main_cli())
