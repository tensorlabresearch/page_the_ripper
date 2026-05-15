# page_the_ripper Test Plan

Current coverage: ~48 tests across 4 test modules (test_escl_scan, test_image_helpers, test_job_store, test_scanning). Significant untested surface area exists.

## Goal: 75-80% line coverage with meaningful, fast tests.

---

## Existing Tests (Keep / Extend)

| Module | Tests |
|--------|-------|
| test_escl_scan.py | 12 — eSCL helper functions |
| test_image_helpers.py | 8 — PIL/numpy image helpers |
| test_job_store.py | 7 — SQLite persistence |  
| test_scanning.py | 2 — full FastAPI lifecycle (integration) |

## New Test Modules to Create

### test_config.py (~8 tests)
- `env_flag` — truthy/falsey values, default handling
- `_cfg_get / _cfg_get_int / _cfg_get_bool / _cfg_get_float` — missing section, missing option, type coercion, fallback
- `build_scanner_registry` — scanner parsing from DEFAULT_CONFIG + scanner.cfg

### test_jobworker.py (~10 tests, mocked)
- `JobWorker.__init__` / `shutdown` / `run` lifecycle
- `JobWorker._process` — success path with mocked dispatch_scan
- `JobWorker._process` — dispatch failure (caught exception -> job marked failed)
- `JobWorker._process` — cancellation via interrupt_event
- Queue behavior: empty queue, multiple jobs

### test_jobmonitor.py (~8 tests, mocked)
- `JobMonitor.__init__` / `shutdown` / `run` lifecycle
- `JobMonitor._check_jobs` — no stale jobs (no-op)
- `_check_jobs` — stale pending/running job gets failed after threshold
- Thread exit on `stop_event.set()`
- Error inside `_check_jobs` doesn't crash monitor
- JOB_STALE_THRESHOLD and JOB_MONITOR_INTERVAL are picked up from env

### test_sane_scanning.py (~6 tests)
- `list_sane_devices` — parse `scanimage -L` output, empty output, malformed lines
- `resolve_sane_device` — explicit device, hint matching, not-found fallback
- `execute_scan_command` — success, non-zero exit code, timeout
- `preserve_sane_raw_pages` — copy files to archive dir
- `remove_debug_raw` — cleanup
- **Caveat**: mark hardware-touching tests with `@pytest.mark.hardware` and skip in CI

### test_escl_http.py (~8 tests, mocked requests)
- `create_escl_session` — verify=True vs verify=False
- `escl_post_scan_job` — 200 response with Location header
- `escl_post_scan_job` — failure codes (4xx/5xx)
- `escl_fetch_documents` — page download, pagination, error handling
- Retry/backoff logic on transient failures

### test_ocr_pipeline.py (~6 tests, tmp_path + fake images)
- `ocr_page` — requires tesseract binary, skip if unavailable
- `create_pdf_from_images` — single page, multi-page, validates output starts with `%PDF`
- `run_ocr_on_pdf` — uses real ocrmypdf or skip if backend missing
- `reocr_pdf` — reprocessing already-ocr'd PDF
- `capture_es_580w_letter_duplex_raw` — mock HTTP to platen scan path
- `capture_et_3850_platen_raw` — mock HTTP to flatbed scan path

### test_api_endpoints.py (~10 tests, TestClient + mocked deps)
- `GET /` — HTML control panel loads (contains expected elements)
- `GET /docs` — swagger UI loads (status 200)
- `GET /api/scanners` — list matches scanner registry
- `GET /api/scanners/{id}` — 200 for known scanner, 404 for unknown
- `GET /api/status` — system health present (mock `gather_system_health`)
- `GET /api/scans` — pagination, offset/limit bounds
- `GET /api/scans/{unknown}` — 404
- `GET /api/scans/{id}` — stage + detail fields, completed job has result_path
- `GET /api/scans/{id}/result` — 200 with PDF content-type, 204 if pending
- `DELETE /api/scans/{id}` — running job cancellation, cleanup pathway
- `POST /api/scans` — 400 on invalid crop_box values, 400 on invalid color_mode

### test_app_lifecycle.py (~4 tests)
- `_startup` — JobWorker + JobMonitor threads start
- `_shutdown` — threads stopped gracefully
- `cleanup_orphaned_jobs_on_startup` — running/pending jobs reset at startup
- Invalid UUID as `job_id` returns proper 4xx

---

## Test Dependencies to Add

Add to `requirements.txt`:
```
httpx>=0.27.0          # fastapi.testclient requires it
responses>=0.25.0      # mock HTTP requests for eSCL tests
pytest-mock>=3.14.0    # mocker fixture convenience
```

Also add to `pyproject.toml` under a `[project.optional-dependencies]` to keep them installable but not primary runtime deps:
```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "httpx>=0.27.0",
    "responses>=0.25.0",
    "pytest-mock>=3.14.0",
]
```

---

## Markers / Skipping Strategy

```toml
[tool.pytest.ini_options]
markers = [
    "hardware: tests requiring a physical scanner or tesseract binary",
    "slow: tests > 2s",
    "integration: tests hitting real HTTP or subprocesses",
]
```

CI command: `pytest -m "not hardware"`
Full suite: `pytest`

---

## Coverage Baseline

Current (estimated): ~25-30% (4 out of ~50 testable units have coverage)
Target: 75-80%

---

## Suggested Order of Work

1. Fix: `httpx` is missing — add to requirements.txt / tinytot venv
2. Add pytest markers + skip decorators for hardware-dependent tests
3. `test_jobmonitor.py` — tinytot is the source of truth, this code is new
4. `test_jobworker.py` — core orchestration, highest ROI
5. `test_api_endpoints.py` — fill the gap between test_scanning.py and the full surface
6. `test_config.py` — quick wins
7. `test_sane_scanning.py` + `test_escl_http.py` — requires mocking strategy
8. `test_ocr_pipeline.py` — lowest priority; touches heavy external binaries
9. Wire up `pytest --cov=main --cov=tools` and add to CI

---

## CI Integration (optional next step)

Add to `.github/workflows/ci.yml`:
```yaml
- name: Run tests
  run: |
    source .venv/bin/activate
    pytest -m "not hardware" --cov=main --cov=tools --cov-report=term-missing --cov-fail-under=70
```
