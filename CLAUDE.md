This is a project for scanning and OCRing content from Epson network connected scanners and creating a text-selectable and searchable PDF as a result.

The project runs in a venv activated with `source .venv/bin/activate`.

### Epson Scanners Targeted
- 192.168.4.225 - ES-580W - This python app should by default attempt to perform front and back page scanning from the default source
- 192.168.4.105 - ET-3850 - This python app should by default attempt to scan from the flatbed scanner at some reasonable dps for text processing

### Tinytot service workflow
- Sync local changes to tinytot: `rsync -av --exclude 'scans/' --exclude 'scan_jobs.sqlite3' --exclude '.venv/' --exclude '__pycache__/' --exclude '.pytest_cache/' /Users/weswidner/code/personal/page_the_ripper/ tinytot:~/page_the_ripper/`.
- Install/refresh deps on tinytot if needed: `cd ~/page_the_ripper && .venv/bin/pip install -r requirements.txt`.
- Restart the REST service: `cd ~/page_the_ripper && make restart-uvicorn` (runs tmux session `page_ripper` with `./.venv/bin/python main.py`).
- Check logs: `tmux capture-pane -p -t page_ripper:0 | tail -n 50` (or `tmux attach -t page_ripper`).
- Inspect job records/log tail: `sqlite3 scan_jobs.sqlite3 'SELECT * FROM scan_jobs WHERE id="<job_id>";'`.
- If ET-3850 fails with `scanimage ... Invalid argument` against an IPv6 link-local device, set the explicit USB device in `scanner.cfg`:
  - `sane_device = epsonscan2:ET-3850 Series:583847343231373461:esci2:usb:ES0223:4481` (found via `scanimage -L`).

### Notes
- Tinytot runs `saned`; USB scanners are exposed via the `epsonds` backend while the ET-3850 is reachable through the SANE `escl` bridge. The service resolves device IDs dynamically using `scanimage -L`, so only high-level hints live in `scanner.cfg`.
- Orientation detection still scores 0/90/180/270 rotations with Tesseract, then trims margins using a final histogram-based white-border cut so covers stay intact.
- JPEG embedding (quality 85, no grayscale subsampling) keeps PDFs lightweight while preserving text clarity.
- A FastAPI service fronts the workflow: `POST /api/scans` enqueues a job in SQLite, `GET /api/scans` lists jobs with paging, `GET /api/scans/{id}` tracks status (embedding SANE log tails when available and exposing the current stage, e.g. `scanning`, `ocr`, `finalizing`), `GET /api/scans/{id}/result` streams the OCR’d PDF, and `DELETE /api/scans/{id}` cancels running jobs and removes any artifacts. An HTML control panel at `/` provides live job monitoring, while Swagger (`/docs`) and ReDoc (`/redoc`) cover interactive documentation.
- Tests use a temporary config and stubbed dispatcher to exercise the REST endpoints (including deletion); they skip automatically when PyMuPDF isn’t available on the host (arm64 build still required for integration runs).
