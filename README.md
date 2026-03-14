# Page the Ripper

A self-hosted scanning service that turns network and USB document scanners into a REST API. Point it at your Epson (or any SANE/eSCL-compatible) scanner and it handles capture, image cleanup, and OCR to produce text-searchable PDFs.

Built with FastAPI and designed to run headless on a Raspberry Pi or any Debian-based Linux box.

## Features

- **SANE and eSCL backends** -- works with local USB scanners and network scanners
- **Automatic document feeder (ADF) support** with duplex scanning
- **Image processing pipeline** -- border trimming, auto-rotation, contrast correction, page scaling
- **OCR via Tesseract + OCRmyPDF** -- produces searchable PDFs with optional deskew and optimization
- **Async job queue** -- submit scans and poll for results; multiple jobs run concurrently
- **SQLite job tracking** -- persistent scan history with pagination
- **Built-in web UI** -- minimal HTML console served at `/`
- **Scanner auto-detection** -- `detect-scanners` script probes SANE devices and writes config

## Quick install

On a Raspberry Pi or any Debian/Ubuntu system:

```bash
curl -fsSL https://raw.githubusercontent.com/kai5263499/page_the_ripper/main/install.sh | sudo bash
```

This will:

1. Remove any previously installed `page-the-ripper` package
2. Download the latest `.deb` from GitHub Releases
3. Install it and pull in all system dependencies (`sane-utils`, `tesseract-ocr`, `ghostscript`, etc.)
4. Create a Python venv and install Python dependencies via pip
5. Auto-detect connected scanners and generate `/etc/page-the-ripper/scanner.cfg`
6. Enable and start the `page-the-ripper` systemd service
7. Print the service status

The API will be available at `http://<host>:8000`. Interactive docs are at `http://<host>:8000/docs`.

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/scanners` | List configured scanners with backend status |
| `GET` | `/api/scanners/{id}` | Scanner details and capabilities |
| `GET` | `/api/system` | System health and resource usage |
| `POST` | `/api/scans` | Start a new scan job (returns job ID) |
| `GET` | `/api/scans` | List scan jobs with pagination |
| `GET` | `/api/scans/{job_id}` | Check job status and progress |
| `GET` | `/api/scans/download/{job_id}` | Download the finished PDF |
| `DELETE` | `/api/scans/{job_id}` | Delete a job and its artifacts |

## Configuration

Scanner config lives at `/etc/page-the-ripper/scanner.cfg` (INI format). The installer generates this automatically, but you can edit it by hand:

```ini
[defaults]
dpi = 300
color_mode = Grayscale8
output_dir = /var/lib/page-the-ripper/scans

[scanner:flatbed]
label = Epson ET-3850
backend = sane
sane_hint = ET-3850
source = Flatbed
duplex = false

[scanner:adf]
label = Epson ES-580W
backend = sane
sane_hint = ES-580W
source = ADF Duplex
duplex = true
```

Re-run scanner detection at any time:

```bash
sudo /usr/lib/page-the-ripper/detect-scanners -o /etc/page-the-ripper/scanner.cfg
sudo systemctl restart page-the-ripper
```

## Service management

```bash
sudo systemctl status page-the-ripper
sudo systemctl restart page-the-ripper
sudo journalctl -u page-the-ripper -f
```

## CLI tools

The package also includes standalone CLI scripts that can be run outside the REST service:

- `scan_local.py` -- full scanning pipeline from the command line (scan, process, assemble, OCR)
- `tools/scan_cli.py` -- subcommands for individual pipeline stages
- `tools/escl_scan.py` -- eSCL protocol helpers for network scanners

## Building the .deb from source

```bash
git clone https://github.com/kai5263499/page_the_ripper.git
cd page_the_ripper
debian/build.sh
# Output: debian/build/page-the-ripper_1.0.0_all.deb
```

Pass a version number to override the default:

```bash
debian/build.sh 2.0.0
```

## Releasing

Push a Git tag matching `v*` and GitHub Actions will build the `.deb` and attach it to a new release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

## Package layout

| Path | Contents |
|------|----------|
| `/opt/page-the-ripper/` | Application code and venv |
| `/etc/page-the-ripper/scanner.cfg` | Scanner configuration (conffile, preserved on upgrade) |
| `/usr/lib/page-the-ripper/detect-scanners` | Scanner auto-detection script |
| `/lib/systemd/system/page-the-ripper.service` | Systemd unit |
| `/var/lib/page-the-ripper/` | Runtime data (scans directory, SQLite database) |

## System dependencies

Installed automatically by `apt` when using the `.deb`:

- `python3` (>= 3.11), `python3-venv`, `python3-dev`
- `sane-utils` (scanner access)
- `tesseract-ocr`, `tesseract-ocr-eng` (OCR)
- `ghostscript` (PDF optimization)
- `libxml2-dev`, `libxslt1-dev` (lxml build)
- `libjpeg-dev`, `zlib1g-dev` (Pillow build)
- `libqpdf-dev` (pikepdf build)

Optional: `unpaper` (scan cleanup), `pngquant` (image optimization).

## License

See repository for license details.
