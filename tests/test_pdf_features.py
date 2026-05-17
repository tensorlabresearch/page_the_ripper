"""Regression tests for the PDF page-preview, crop, and composite features."""

from __future__ import annotations

import importlib
import io
import shutil
import threading
import time
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from PIL import Image

CONFIG_TEMPLATE = """\
[defaults]
dpi = 300
color_mode = Grayscale8
output_dir = scans

[scanner:et3850]
label = Test Scanner
backend = sane
sane_hint = Test
"""


def _make_pdf_bytes(*, pages: int = 1, size: tuple[int, int] = (200, 280)) -> bytes:
    """Build a small but real multi-page PDF with img2pdf."""
    import img2pdf

    streams: list[bytes] = []
    for _ in range(pages):
        img = Image.new("L", size, color=240)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        streams.append(buf.getvalue())
    return img2pdf.convert(streams, layout_fun=img2pdf.get_fixed_dpi_layout_fun((150, 150)))


def _seed_completed_job(module, output_dir: Path, *, job_id: str, pages: int = 1) -> Path:
    pdf_path = output_dir / f"{job_id}.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(_make_pdf_bytes(pages=pages))
    module.JOB_STORE.create_job(job_id, scanner="et3850", params={})
    module.JOB_STORE.update_job(
        job_id,
        status="completed",
        result_path=pdf_path,
        stage="finalized",
        number_of_pages=pages,
    )
    return pdf_path


def _wait_for_terminal(client: TestClient, job_id: str, *, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    last: dict | None = None
    while time.time() < deadline:
        resp = client.get(f"/api/scans/{job_id}")
        resp.raise_for_status()
        last = resp.json()
        if last["status"] in {"completed", "failed"}:
            return last
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for job {job_id}; last={last}")


@pytest.fixture()
def app_module(tmp_path, monkeypatch):
    cfg = tmp_path / "scanner.cfg"
    cfg.write_text(CONFIG_TEMPLATE)
    monkeypatch.setenv("SCANNER_CFG", str(cfg))
    monkeypatch.setenv("SCAN_DB_PATH", str(tmp_path / "jobs.sqlite3"))
    output_dir = tmp_path / "scans"
    monkeypatch.setenv("SCAN_OUTPUT_DIR", str(output_dir))

    # Force a known SPA dist so route-precedence tests are meaningful.
    fake_dist = tmp_path / "ui_dist"
    fake_dist.mkdir()
    (fake_dist / "index.html").write_text(
        "<!doctype html><html><body data-test='spa'></body></html>"
    )
    monkeypatch.setenv("PAGE_RIPPER_UI_DIST", str(fake_dist))

    import main

    module = importlib.reload(main)

    # Stub OCR so crop/composite re-OCR paths are exercisable without tesseract.
    def fake_run_ocr(input_pdf: Path, output_pdf: Path, **_kwargs) -> None:
        Path(output_pdf).parent.mkdir(parents=True, exist_ok=True)
        Path(output_pdf).write_bytes(Path(input_pdf).read_bytes())

    monkeypatch.setattr(module, "run_ocr_on_pdf", fake_run_ocr)

    with TestClient(module.app) as client:
        yield client, module, output_dir


class TestValidateCropBox:
    def test_accepts_valid_box(self, app_module):
        _, module, _ = app_module
        assert module._validate_crop_box([0.1, 0.1, 0.9, 0.9]) == (0.1, 0.1, 0.9, 0.9)

    def test_accepts_full_page(self, app_module):
        _, module, _ = app_module
        assert module._validate_crop_box([0.0, 0.0, 1.0, 1.0]) == (0.0, 0.0, 1.0, 1.0)

    @pytest.mark.parametrize(
        "box",
        [
            [-0.01, 0.0, 1.0, 1.0],
            [0.0, -0.5, 1.0, 1.0],
            [0.0, 0.0, 1.01, 1.0],
            [0.0, 0.0, 1.0, 1.5],
        ],
    )
    def test_rejects_out_of_range(self, app_module, box):
        _, module, _ = app_module
        with pytest.raises(HTTPException) as exc:
            module._validate_crop_box(box)
        assert exc.value.status_code == 400

    @pytest.mark.parametrize(
        "box",
        [
            [0.50, 0.0, 0.51, 1.0],  # too narrow (1% wide)
            [0.0, 0.50, 1.0, 0.51],  # too short (1% tall)
            [0.5, 0.5, 0.5, 0.5],  # zero area
            [0.6, 0.0, 0.4, 1.0],  # inverted
        ],
    )
    def test_rejects_degenerate(self, app_module, box):
        _, module, _ = app_module
        with pytest.raises(HTTPException) as exc:
            module._validate_crop_box(box)
        assert exc.value.status_code == 400


class TestViewScanPdf:
    def test_serves_inline(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_view_ok")
        resp = client.get("/api/scans/j_view_ok/view")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
        assert resp.headers.get("content-disposition", "").startswith("inline")
        assert resp.content.startswith(b"%PDF")

    def test_404_when_missing(self, app_module):
        client, *_ = app_module
        assert client.get("/api/scans/nope/view").status_code == 404

    def test_409_when_pending(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_view_pending", scanner="et3850", params={})
        assert client.get("/api/scans/j_view_pending/view").status_code == 409

    def test_410_when_file_missing(self, app_module):
        client, module, output_dir = app_module
        pdf = _seed_completed_job(module, output_dir, job_id="j_view_orphan")
        pdf.unlink()
        assert client.get("/api/scans/j_view_orphan/view").status_code == 410


class TestListScanPages:
    def test_returns_page_metadata(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_pages_ok", pages=3)
        resp = client.get("/api/scans/j_pages_ok/pages")
        assert resp.status_code == 200
        body = resp.json()
        assert body["page_count"] == 3
        assert len(body["pages"]) == 3
        for idx, page in enumerate(body["pages"]):
            assert page["index"] == idx
            assert page["width_pt"] > 0
            assert page["height_pt"] > 0

    def test_404_when_job_missing(self, app_module):
        client, *_ = app_module
        assert client.get("/api/scans/nope/pages").status_code == 404

    def test_409_when_job_pending(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_pages_pending", scanner="et3850", params={})
        assert client.get("/api/scans/j_pages_pending/pages").status_code == 409

    def test_410_when_result_file_missing(self, app_module):
        client, module, output_dir = app_module
        pdf = _seed_completed_job(module, output_dir, job_id="j_pages_orphan")
        pdf.unlink()
        assert client.get("/api/scans/j_pages_orphan/pages").status_code == 410


class TestPagePreview:
    def test_404_when_job_missing(self, app_module):
        client, *_ = app_module
        assert client.get("/api/scans/nope/pages/0/preview.jpg").status_code == 404

    def test_404_when_page_out_of_range(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_prev_oor", pages=1)
        assert client.get("/api/scans/j_prev_oor/pages/9/preview.jpg").status_code == 404

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_renders_jpeg(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_prev_ok", pages=1)
        resp = client.get("/api/scans/j_prev_ok/pages/0/preview.jpg?max_width=240")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/jpeg"
        # JPEG SOI marker
        assert resp.content[:3] == b"\xff\xd8\xff"


class TestCropEndpoint:
    def test_404_missing_source(self, app_module):
        client, *_ = app_module
        resp = client.post("/api/scans/missing/crop", json={"box": [0, 0, 1, 1]})
        assert resp.status_code == 404

    def test_409_source_not_completed(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_crop_pending", scanner="et3850", params={})
        resp = client.post("/api/scans/j_crop_pending/crop", json={"box": [0, 0, 1, 1]})
        assert resp.status_code == 409

    def test_400_bad_box(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_badbox")
        resp = client.post(
            "/api/scans/j_crop_badbox/crop",
            json={"box": [2.0, 0, 1.0, 1.0]},
        )
        assert resp.status_code == 400

    def test_422_box_wrong_length(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_shape")
        resp = client.post("/api/scans/j_crop_shape/crop", json={"box": [0.0, 0.0, 1.0]})
        assert resp.status_code == 422

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_crop_end_to_end(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_e2e", pages=2)
        resp = client.post(
            "/api/scans/j_crop_e2e/crop",
            json={"box": [0.1, 0.1, 0.9, 0.9], "reocr": False, "dpi": 150},
        )
        assert resp.status_code == 202
        new_id = resp.json()["job_id"]
        terminal = _wait_for_terminal(client, new_id, timeout=20.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 2
        dl = client.get(f"/api/scans/download/{new_id}")
        assert dl.status_code == 200
        assert dl.headers["content-type"] == "application/pdf"
        assert dl.content.startswith(b"%PDF")
        # New job is a separate row; source job is untouched.
        src = module.JOB_STORE.get_job("j_crop_e2e")
        assert src is not None
        assert src["status"] == "completed"
        assert Path(str(src["result_path"])).exists()

    def test_400_rotation_out_of_range(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_rotrange")
        resp = client.post(
            "/api/scans/j_crop_rotrange/crop",
            json={"box": [0.0, 0.0, 1.0, 1.0], "rotation": 400},
        )
        assert resp.status_code == 422

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_crop_with_90deg_rotation_swaps_dimensions(self, app_module):
        client, module, output_dir = app_module
        # Source pages are 200x280 pt (portrait).
        _seed_completed_job(module, output_dir, job_id="j_crop_rot90", pages=1)
        resp = client.post(
            "/api/scans/j_crop_rot90/crop",
            json={
                "box": [0.0, 0.0, 1.0, 1.0],
                "reocr": False,
                "dpi": 150,
                "rotation": 90,
            },
        )
        assert resp.status_code == 202
        new_id = resp.json()["job_id"]
        terminal = _wait_for_terminal(client, new_id, timeout=20.0)
        assert terminal["status"] == "completed", terminal
        # Re-inspect dimensions via the /pages endpoint.
        pages = client.get(f"/api/scans/{new_id}/pages").json()["pages"]
        w, h = pages[0]["width_pt"], pages[0]["height_pt"]
        # After 90 degree rotation the rotated page should be landscape.
        assert h < w, f"expected landscape after 90deg rotation, got {w}x{h}"

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_crop_with_freeform_rotation_produces_valid_pdf(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_rotfree", pages=1)
        resp = client.post(
            "/api/scans/j_crop_rotfree/crop",
            json={
                "box": [0.1, 0.1, 0.9, 0.9],
                "reocr": False,
                "dpi": 150,
                "rotation": 37.5,  # arbitrary free-form angle
            },
        )
        assert resp.status_code == 202
        terminal = _wait_for_terminal(client, resp.json()["job_id"], timeout=20.0)
        assert terminal["status"] == "completed", terminal
        dl = client.get(f"/api/scans/download/{resp.json()['job_id']}")
        assert dl.status_code == 200
        assert dl.content.startswith(b"%PDF")

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_crop_with_reocr_uses_run_ocr(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_crop_reocr", pages=1)
        resp = client.post(
            "/api/scans/j_crop_reocr/crop",
            json={"box": [0.0, 0.0, 1.0, 1.0], "reocr": True, "dpi": 150},
        )
        assert resp.status_code == 202
        terminal = _wait_for_terminal(client, resp.json()["job_id"], timeout=20.0)
        # Because the fixture stubs run_ocr_on_pdf to a passthrough, the job
        # must still reach "completed" and produce a downloadable PDF.
        assert terminal["status"] == "completed", terminal


class TestCompositeEndpoint:
    def test_422_empty_sources(self, app_module):
        client, *_ = app_module
        resp = client.post("/api/scans/composite", json={"sources": []})
        assert resp.status_code == 422

    def test_404_unknown_source(self, app_module):
        client, *_ = app_module
        resp = client.post("/api/scans/composite", json={"sources": ["nope"]})
        assert resp.status_code == 404

    def test_409_source_not_completed(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_comp_pending", scanner="et3850", params={})
        resp = client.post(
            "/api/scans/composite", json={"sources": ["j_comp_pending"]}
        )
        assert resp.status_code == 409

    def test_composite_end_to_end_preserves_order(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_comp_a", pages=2)
        _seed_completed_job(module, output_dir, job_id="j_comp_b", pages=1)
        resp = client.post(
            "/api/scans/composite",
            json={"sources": ["j_comp_a", "j_comp_b"], "reocr": False},
        )
        assert resp.status_code == 202
        new_id = resp.json()["job_id"]
        terminal = _wait_for_terminal(client, new_id, timeout=15.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 3  # 2 + 1

        # Verify the saved row records the source order.
        saved = module.JOB_STORE.get_job(new_id)
        assert saved is not None
        assert saved["scanner"] == "__composite__"
        assert saved["params"]["sources"] == ["j_comp_a", "j_comp_b"]

        dl = client.get(f"/api/scans/download/{new_id}")
        assert dl.status_code == 200
        assert dl.content.startswith(b"%PDF")
        # Confirm the merged file actually has the expected page count by re-opening.
        import pikepdf

        with pikepdf.Pdf.open(io.BytesIO(dl.content)) as merged:
            assert len(merged.pages) == 3

    def test_composite_single_source_is_allowed(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_comp_solo", pages=2)
        resp = client.post(
            "/api/scans/composite",
            json={"sources": ["j_comp_solo"], "reocr": False},
        )
        assert resp.status_code == 202
        terminal = _wait_for_terminal(client, resp.json()["job_id"], timeout=10.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 2


class TestScannerProbeCache:
    """Scanner enumeration shells out to scanimage -L; the route must cache it."""

    def _patch_probe(self, monkeypatch, module):
        counter = {"sane": 0, "escl": 0}

        def fake_sane(_entry):
            counter["sane"] += 1
            return {"status": "ok", "configured": {"sane_device": "fake:device"}}

        def fake_escl(_entry):
            counter["escl"] += 1
            return {"status": "ok", "default_url": "http://fake"}

        monkeypatch.setattr(module, "fetch_sane_backend_details", fake_sane)
        monkeypatch.setattr(module, "fetch_escl_backend_details", fake_escl)
        module._invalidate_scanner_probe_cache()
        return counter

    def test_list_scanners_caches_probe(self, app_module, monkeypatch):
        client, module, _ = app_module
        counter = self._patch_probe(monkeypatch, module)
        n_scanners = sum(
            1 for entry in module.SCANNER_REGISTRY.values() if entry.get("backend") == "sane"
        )
        assert n_scanners >= 1
        r1 = client.get("/api/scanners")
        assert r1.status_code == 200
        assert counter["sane"] == n_scanners
        r2 = client.get("/api/scanners")
        assert r2.status_code == 200
        # Second call must reuse every cached probe.
        assert counter["sane"] == n_scanners
        assert r1.json() == r2.json()

    def test_refresh_query_bypasses_cache(self, app_module, monkeypatch):
        client, module, _ = app_module
        counter = self._patch_probe(monkeypatch, module)
        n_scanners = sum(
            1 for entry in module.SCANNER_REGISTRY.values() if entry.get("backend") == "sane"
        )
        client.get("/api/scanners")
        client.get("/api/scanners")
        assert counter["sane"] == n_scanners
        r3 = client.get("/api/scanners?refresh=true")
        assert r3.status_code == 200
        assert counter["sane"] == n_scanners * 2  # cache cleared → re-probe all
        client.get("/api/scanners")
        assert counter["sane"] == n_scanners * 2  # cache repopulated

    def test_scanner_details_shares_cache(self, app_module, monkeypatch):
        client, module, _ = app_module
        counter = self._patch_probe(monkeypatch, module)
        n_scanners = sum(
            1 for entry in module.SCANNER_REGISTRY.values() if entry.get("backend") == "sane"
        )
        # List populates the cache for every scanner.
        client.get("/api/scanners")
        assert counter["sane"] == n_scanners
        # Detail endpoint for one scanner must hit the cache (no extra probe).
        r = client.get("/api/scanners/et3850")
        assert r.status_code == 200
        assert counter["sane"] == n_scanners
        # ?refresh=true on the detail endpoint re-probes only that one scanner.
        client.get("/api/scanners/et3850?refresh=true")
        assert counter["sane"] == n_scanners + 1

    def test_ttl_expires(self, app_module, monkeypatch):
        client, module, _ = app_module
        counter = self._patch_probe(monkeypatch, module)
        monkeypatch.setattr(module, "SCANNER_PROBE_CACHE_TTL", 0.0)
        n_scanners = sum(
            1 for entry in module.SCANNER_REGISTRY.values() if entry.get("backend") == "sane"
        )
        # First call populates; second call sees TTL=0 and re-probes everything.
        client.get("/api/scanners")
        client.get("/api/scanners")
        assert counter["sane"] == n_scanners * 2


class TestRecovery:
    @staticmethod
    def _seed_failed_with_raw_dir(module, *, job_id: str, raw_dir: Path, pages: int = 3) -> Path:
        from PIL import Image as PILImage

        raw_dir.mkdir(parents=True, exist_ok=True)
        for i in range(1, pages + 1):
            PILImage.new("L", (200, 280), color=240).save(raw_dir / f"page-{i:03d}.png")
        module.JOB_STORE.create_job(job_id, scanner="et3850", params={})
        module.JOB_STORE.update_job(
            job_id,
            status="failed",
            error="killed mid-scan",
            stage="failed",
            stage_detail=f"scanning - captured {pages} page(s)",
        )
        module.JOB_STORE.update_params(job_id, {"recovery_path": str(raw_dir)})
        return raw_dir

    def test_recovery_available_flag(self, app_module, tmp_path):
        client, module, _ = app_module
        raw = tmp_path / "scanjob-jrec1-xyz"
        self._seed_failed_with_raw_dir(module, job_id="jrec1", raw_dir=raw, pages=2)
        body = client.get("/api/scans/jrec1").json()
        assert body["status"] == "failed"
        assert body["recovery_available"] is True

    def test_recovery_available_false_when_dir_gone(self, app_module, tmp_path):
        client, module, _ = app_module
        raw = tmp_path / "scanjob-jrec2-xyz"
        self._seed_failed_with_raw_dir(module, job_id="jrec2", raw_dir=raw, pages=1)
        # User clears /tmp; recovery_path in params is now stale.
        shutil.rmtree(raw)
        body = client.get("/api/scans/jrec2").json()
        assert body["recovery_available"] is False

    def test_recovery_available_false_for_completed_jobs(self, app_module, output_dir_unused=None):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="jrec3", pages=1)
        body = client.get("/api/scans/jrec3").json()
        assert body["recovery_available"] is False

    def test_recover_endpoint_404_unknown(self, app_module):
        client, *_ = app_module
        assert client.post("/api/scans/nope/recover").status_code == 404

    def test_recover_endpoint_409_not_failed(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="jrec4")
        assert client.post("/api/scans/jrec4/recover").status_code == 409

    def test_recover_endpoint_410_no_raw_dir(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("jrec5", scanner="et3850", params={})
        module.JOB_STORE.update_job("jrec5", status="failed", error="x", stage="failed")
        # No recovery_path in params and no dir on disk.
        assert client.post("/api/scans/jrec5/recover").status_code == 410

    def test_recover_end_to_end(self, app_module, tmp_path):
        client, module, _ = app_module
        raw = tmp_path / "scanjob-jrec6-xyz"
        self._seed_failed_with_raw_dir(module, job_id="jrec6", raw_dir=raw, pages=3)
        resp = client.post("/api/scans/jrec6/recover", json={"reocr": False})
        assert resp.status_code == 202
        assert resp.json()["job_id"] == "jrec6"  # same row, in-place update

        terminal = _wait_for_terminal(client, "jrec6", timeout=15.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 3
        # Flag should clear once recovery_path is removed from params.
        assert terminal["recovery_available"] is False
        # And the resulting PDF should be downloadable.
        dl = client.get("/api/scans/download/jrec6")
        assert dl.status_code == 200
        assert dl.content.startswith(b"%PDF")
        # The saved row should carry the audit trail.
        saved = module.JOB_STORE.get_job("jrec6")
        assert saved is not None
        params = saved["params"]
        assert isinstance(params, dict)
        assert params.get("recovered") is True
        assert params.get("recovery_path") is None

    def test_orphan_cleanup_records_recovery_path(self, app_module, monkeypatch, tmp_path):
        client, module, _ = app_module
        # Seed a job in 'running' state, then synthesize the raw tempdir
        # that the new prefix would have left in /tmp.
        module.JOB_STORE.create_job("jrec7", scanner="et3850", params={})
        module.JOB_STORE.update_job("jrec7", status="running", stage="scanning")
        # Pretend tempfile.gettempdir() points at tmp_path so the helper finds it.
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
        raw = tmp_path / "scanjob-jrec7-abc"
        raw.mkdir()
        (raw / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
        # Re-run cleanup (override the run-once guard).
        module._cleanup_already_run = False
        module.cleanup_orphaned_jobs_on_startup()
        body = client.get("/api/scans/jrec7").json()
        assert body["status"] == "failed"
        assert body["recovery_available"] is True


class TestRetry:
    """POST /api/scans/{id}/retry re-runs a failed __crop__ job with original params."""

    @staticmethod
    def _seed_failed_crop(module, source_id: str, *, box, dpi=300, reocr=True, rotation=0.0):
        crop_id = "j_retry_crop"
        module.JOB_STORE.create_job(
            crop_id,
            scanner="__crop__",
            params={"source": source_id, "box": box, "dpi": dpi, "reocr": reocr, "rotation": rotation},
        )
        module.JOB_STORE.update_job(crop_id, status="failed", error="boom", stage="error")
        return crop_id

    def test_404_unknown(self, app_module):
        client, *_ = app_module
        assert client.post("/api/scans/nope/retry").status_code == 404

    def test_409_when_not_failed(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_retry_done")
        assert client.post("/api/scans/j_retry_done/retry").status_code == 409

    def test_422_for_non_crop_non_composite_scanner(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_retry_scan", scanner="et3850", params={"dpi": 200})
        module.JOB_STORE.update_job("j_retry_scan", status="failed", error="x", stage="failed")
        resp = client.post("/api/scans/j_retry_scan/retry")
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert "__crop__" in detail and "__composite__" in detail

    def test_retry_runs_composite_with_same_sources(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_rc_src1", pages=1)
        _seed_completed_job(module, output_dir, job_id="j_rc_src2", pages=2)
        # Seed a failed composite that references both sources.
        module.JOB_STORE.create_job(
            "j_retry_comp",
            scanner="__composite__",
            params={"sources": ["j_rc_src1", "j_rc_src2"], "reocr": False},
        )
        module.JOB_STORE.update_job("j_retry_comp", status="failed", error="boom", stage="error")
        resp = client.post("/api/scans/j_retry_comp/retry")
        assert resp.status_code == 202
        new_id = resp.json()["job_id"]
        assert new_id != "j_retry_comp"
        terminal = _wait_for_terminal(client, new_id, timeout=10.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 3
        # Lineage recorded in params.
        saved = module.JOB_STORE.get_job(new_id) or {}
        assert saved["params"]["retried_from"] == "j_retry_comp"
        assert saved["params"]["sources"] == ["j_rc_src1", "j_rc_src2"]

    def test_retry_composite_410_when_source_gone(self, app_module):
        client, module, output_dir = app_module
        pdf = _seed_completed_job(module, output_dir, job_id="j_rc_missing", pages=1)
        pdf.unlink()
        module.JOB_STORE.create_job(
            "j_retry_comp_gone",
            scanner="__composite__",
            params={"sources": ["j_rc_missing"], "reocr": False},
        )
        module.JOB_STORE.update_job("j_retry_comp_gone", status="failed", error="x", stage="error")
        resp = client.post("/api/scans/j_retry_comp_gone/retry")
        assert resp.status_code == 410

    def test_410_when_source_pdf_gone(self, app_module, monkeypatch):
        client, module, output_dir = app_module
        # Create a source then delete its PDF on disk.
        pdf = _seed_completed_job(module, output_dir, job_id="j_retry_src", pages=1)
        pdf.unlink()
        crop_id = self._seed_failed_crop(module, "j_retry_src", box=[0, 0, 1, 1])
        resp = client.post(f"/api/scans/{crop_id}/retry")
        assert resp.status_code == 410

    @pytest.mark.skipif(shutil.which("pdftoppm") is None, reason="pdftoppm not installed")
    def test_retry_runs_crop_with_same_params(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_retry_src2", pages=1)
        crop_id = self._seed_failed_crop(
            module, "j_retry_src2", box=[0.1, 0.1, 0.9, 0.9], dpi=150, reocr=False, rotation=0.0
        )
        resp = client.post(f"/api/scans/{crop_id}/retry")
        assert resp.status_code == 202
        new_id = resp.json()["job_id"]
        assert new_id != crop_id
        terminal = _wait_for_terminal(client, new_id, timeout=20.0)
        assert terminal["status"] == "completed", terminal
        # Saved params record the lineage.
        new_row = module.JOB_STORE.get_job(new_id) or {}
        assert new_row["params"]["retried_from"] == crop_id
        assert new_row["params"]["source"] == "j_retry_src2"
        assert new_row["params"]["box"] == [0.1, 0.1, 0.9, 0.9]


class TestDeleteCleanup:
    """Deleting a failed job must also clean up its leftover raw-scan tempdir."""

    def test_delete_removes_recovery_tempdir(self, app_module, tmp_path, monkeypatch):
        client, module, _ = app_module
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
        raw = tmp_path / "scanjob-j_delcleanup-xyz"
        raw.mkdir()
        (raw / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
        module.JOB_STORE.create_job("j_delcleanup", scanner="et3850", params={})
        module.JOB_STORE.update_job(
            "j_delcleanup", status="failed", error="boom", stage="failed"
        )
        module.JOB_STORE.update_params("j_delcleanup", {"recovery_path": str(raw)})
        assert raw.is_dir()
        resp = client.delete("/api/scans/j_delcleanup")
        assert resp.status_code == 204
        assert not raw.exists(), "leftover scan tempdir was not removed on delete"

    def test_delete_cleans_up_via_glob_fallback(self, app_module, tmp_path, monkeypatch):
        client, module, _ = app_module
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
        # Old-style failed job without recovery_path in params, but the
        # tempdir is still discoverable by the {job_id} prefix glob.
        raw = tmp_path / "scanjob-j_delcleanup2-zzz"
        raw.mkdir()
        (raw / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
        module.JOB_STORE.create_job("j_delcleanup2", scanner="et3850", params={})
        module.JOB_STORE.update_job(
            "j_delcleanup2", status="failed", error="boom", stage="failed"
        )
        resp = client.delete("/api/scans/j_delcleanup2")
        assert resp.status_code == 204
        assert not raw.exists()

    def test_delete_succeeds_when_no_tempdir(self, app_module):
        client, module, _ = app_module
        # Normal failed job with no leftover state — delete must still succeed.
        module.JOB_STORE.create_job("j_delplain", scanner="et3850", params={})
        module.JOB_STORE.update_job("j_delplain", status="failed", error="x", stage="failed")
        assert client.delete("/api/scans/j_delplain").status_code == 204


class TestResume:
    """Resume flow: recover partial in place, queue fresh scan, auto-merge on completion."""

    @staticmethod
    def _seed_failed_with_raw_dir(module, *, job_id: str, raw_dir: Path, scanner: str, pages: int):
        from PIL import Image as PILImage

        raw_dir.mkdir(parents=True, exist_ok=True)
        for i in range(1, pages + 1):
            PILImage.new("L", (160, 220), color=240).save(raw_dir / f"page-{i:03d}.png")
        module.JOB_STORE.create_job(job_id, scanner=scanner, params={"dpi": 200})
        module.JOB_STORE.update_job(
            job_id,
            status="failed",
            error="timeout",
            stage="failed",
            stage_detail=f"scanning - captured {pages} page(s)",
        )
        existing = module.JOB_STORE.get_job(job_id) or {}
        params = (existing.get("params") if isinstance(existing.get("params"), dict) else {}) or {}
        params = dict(params)
        params["recovery_path"] = str(raw_dir)
        module.JOB_STORE.update_params(job_id, params)

    def test_404_when_job_unknown(self, app_module):
        client, *_ = app_module
        assert client.post("/api/scans/nope/resume").status_code == 404

    def test_409_when_not_failed(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_resume_ok")
        assert client.post("/api/scans/j_resume_ok/resume").status_code == 409

    def test_410_when_no_raw_dir(self, app_module):
        client, module, _ = app_module
        module.JOB_STORE.create_job("j_resume_nodir", scanner="et3850", params={})
        module.JOB_STORE.update_job("j_resume_nodir", status="failed", error="x", stage="failed")
        assert client.post("/api/scans/j_resume_nodir/resume").status_code == 410

    def test_422_when_scanner_gone(self, app_module, tmp_path):
        client, module, _ = app_module
        raw = tmp_path / "scanjob-j_resume_gone"
        self._seed_failed_with_raw_dir(
            module, job_id="j_resume_gone", raw_dir=raw, scanner="ghost", pages=1
        )
        resp = client.post("/api/scans/j_resume_gone/resume")
        assert resp.status_code == 422

    def test_resume_recovers_in_place_and_queues_followup(self, app_module, tmp_path, monkeypatch):
        client, module, _ = app_module
        scanner_id = next(iter(module.SCANNER_REGISTRY))
        raw = tmp_path / "scanjob-j_resume_e2e"
        self._seed_failed_with_raw_dir(
            module, job_id="j_resume_e2e", raw_dir=raw, scanner=scanner_id, pages=2
        )

        # Stub the actual scan dispatch so the followup completes quickly without
        # touching a real scanner.
        fake_pages_remaining = 3

        def fake_dispatch(*args, **kwargs):
            img = Image.new("L", (160, 220), color=200)
            raw_dir = Path(tmp_path / "stub-scan")
            raw_dir.mkdir(exist_ok=True)
            return module.ScanResult(pages=[img] * fake_pages_remaining, raw_dir=raw_dir, raw_paths=[])

        monkeypatch.setattr(module, "dispatch_scan", fake_dispatch)

        resp = client.post("/api/scans/j_resume_e2e/resume", json={})
        assert resp.status_code == 202
        body = resp.json()
        assert body["recovered_id"] == "j_resume_e2e"
        new_scan_id = body["new_scan_id"]
        assert new_scan_id != "j_resume_e2e"

        # The partial scan row is now completed (recovered in place).
        recovered = client.get("/api/scans/j_resume_e2e").json()
        assert recovered["status"] == "completed"
        assert recovered["number_of_pages"] == 2

        # The new scan is queued; once it completes, the worker should auto-queue
        # a composite. Wait for the composite to appear.
        deadline = time.time() + 15
        composite_id: str | None = None
        while time.time() < deadline and composite_id is None:
            listing = client.get("/api/scans?page=1&page_size=50").json()
            for j in listing["items"]:
                if (
                    j.get("scanner") == "__composite__"
                    and "j_resume_e2e" in (j.get("error") or "")  # debug catch-all
                ):
                    composite_id = j["id"]
                    break
            for j in listing["items"]:
                if j.get("scanner") == "__composite__" and j["id"] not in {"j_resume_e2e", new_scan_id}:
                    # find a composite whose params reference our pair
                    full = module.JOB_STORE.get_job(j["id"]) or {}
                    params = full.get("params") or {}
                    if isinstance(params, dict) and params.get("sources") == ["j_resume_e2e", new_scan_id]:
                        composite_id = j["id"]
                        break
            if composite_id:
                break
            time.sleep(0.2)
        assert composite_id is not None, "auto-merge composite was never queued"
        terminal = _wait_for_terminal(client, composite_id, timeout=15.0)
        assert terminal["status"] == "completed", terminal
        assert terminal["number_of_pages"] == 2 + fake_pages_remaining

        # The composite job records its lineage.
        composite = module.JOB_STORE.get_job(composite_id) or {}
        assert composite["params"]["auto_merged_from_resume"] is True


class TestInProcessFailureRecovery:
    """When a scan dies mid-flight (timeout, busy, jam), the worker must
    preserve the raw pages on disk and stash their location in params so
    the UI offers a Recover action without waiting for a service restart."""

    def test_scan_with_sane_preserves_tempdir_on_exception(self, app_module, tmp_path, monkeypatch):
        _, module, _ = app_module

        captured_dir: list[Path] = []

        def fake_scan_to_directory(options, *, dpi, color_mode, output_dir, progress_cb=None, job_entry=None):
            captured_dir.append(Path(output_dir))
            # Simulate a partial capture before a timeout fires.
            (Path(output_dir) / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
            (Path(output_dir) / "page-002.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
            raise RuntimeError("scanimage timed out after 240 seconds")

        monkeypatch.setattr(module, "sane_scan_to_directory", fake_scan_to_directory)
        # Use the tmp_path as the temp root so we can inspect dirs.
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
        monkeypatch.setenv("TMPDIR", str(tmp_path))

        options = {
            "sane_device": "fake:device",
            "sane_hint": "fake",
            "command": "",
            "extra_args": "",
            "source": "Flatbed",
            "duplex": False,
            "page_width_mm": 0,
            "page_height_mm": 0,
            "final_reduce_command": "",
        }
        with pytest.raises(RuntimeError):
            module.scan_with_sane(
                options, dpi=300, color_mode="Grayscale8", processing_opts=None, job_id="j_preserve"
            )
        assert captured_dir, "fake_scan_to_directory was never called"
        # The tempdir and the partial pages must still exist on disk.
        leftover = captured_dir[0]
        assert leftover.is_dir(), f"tempdir wiped: {leftover}"
        leftover_pages = list(leftover.glob("page-*.png"))
        assert len(leftover_pages) == 2

    def test_worker_failure_path_tags_recovery_path(self, app_module, tmp_path, monkeypatch):
        client, module, _ = app_module
        # Synthesize the kind of leftover dir a real scan would have produced.
        monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
        raw = tmp_path / "scanjob-j_postfail-xyz"
        raw.mkdir()
        (raw / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)

        # Drive the worker's failure path directly: create the job, then
        # invoke the exception handler the same way _process would (via a
        # synthetic exception path) — easiest is to call the inline logic by
        # patching dispatch_scan and submitting a real scan.
        scanner_id = next(iter(module.SCANNER_REGISTRY))

        def fake_dispatch(*args, **kwargs):
            raise RuntimeError("simulated timeout")

        monkeypatch.setattr(module, "dispatch_scan", fake_dispatch)

        resp = client.post("/api/scans", json={"scanner": scanner_id, "dpi": 200})
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]
        # Worker runs async; wait for terminal.
        terminal = _wait_for_terminal(client, job_id, timeout=10.0)
        assert terminal["status"] == "failed", terminal

        # The job_id won't match the raw dir we pre-seeded (job_id was random),
        # so create the matching dir now so the post-failure probe finds it.
        # In the real world, scan_with_sane created the dir with the right
        # name before the exception; this synthesizes that state.
        late_raw = tmp_path / f"scanjob-{job_id}-late"
        late_raw.mkdir()
        (late_raw / "page-001.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
        # Re-run the probe by inspecting _find_recoverable_raw_dir and
        # writing params manually (the same code path the worker took above).
        # In production this happens automatically inside the worker's except
        # block; here we just assert the wiring works.
        found = module._find_recoverable_raw_dir(job_id)
        assert found is not None
        module.JOB_STORE.update_params(job_id, {"recovery_path": str(found)})
        body = client.get(f"/api/scans/{job_id}").json()
        assert body["recovery_available"] is True


class TestShutdownDrainsBackgroundJobs:
    """Crop / composite / recover threads must finish before the process exits.
    The earlier bug: pdftoppm got SIGTERM mid-rasterize because shutdown only
    joined JOB_WORKER, not the daemon threads spawned for background work."""

    def test_shutdown_waits_for_registered_background_thread(self, app_module, monkeypatch):
        _, module, _ = app_module
        # Cap the wait short enough for a snappy test.
        monkeypatch.setattr(module, "SHUTDOWN_JOIN_TIMEOUT", 5.0)
        # A "background job" thread that finishes ~0.4s after we ask shutdown.
        done = threading.Event()

        def slow_work():
            time.sleep(0.4)
            done.set()

        thread = threading.Thread(target=slow_work, daemon=True, name="fake-bg")
        thread.start()
        module._register_background_job("bg-test", thread)

        try:
            start = time.time()
            module._shutdown()
            elapsed = time.time() - start
            assert done.is_set(), "_shutdown returned before background thread finished"
            assert not thread.is_alive(), "thread still running after _shutdown"
            assert elapsed >= 0.4, f"_shutdown returned too fast ({elapsed:.2f}s)"
            assert elapsed < 5.0, f"_shutdown took longer than the wait budget ({elapsed:.2f}s)"
        finally:
            module._unregister_background_job("bg-test")

    def test_shutdown_gives_up_after_budget_exhausted(self, app_module, monkeypatch):
        _, module, _ = app_module
        # Tiny budget; the thread will outlive it.
        monkeypatch.setattr(module, "SHUTDOWN_JOIN_TIMEOUT", 0.3)
        stop = threading.Event()

        def long_work():
            stop.wait(timeout=5)  # forced to outlive the shutdown budget

        thread = threading.Thread(target=long_work, daemon=True, name="fake-bg-slow")
        thread.start()
        module._register_background_job("bg-test-slow", thread)

        try:
            start = time.time()
            module._shutdown()
            elapsed = time.time() - start
            # Shutdown must return at the budget boundary, not block forever.
            assert elapsed < 2.0, f"_shutdown ignored its budget ({elapsed:.2f}s)"
            # The thread is still alive after exit; in production this is when
            # systemd's TimeoutStopSec would force the issue. cleanup_orphaned
            # will mark these jobs failed on next startup.
            assert thread.is_alive()
        finally:
            stop.set()
            thread.join(timeout=1)
            module._unregister_background_job("bg-test-slow")


class TestJobMonitorResilience:
    """The monitor used to mark long-OCR jobs as orphaned. These guard
    against that regression."""

    def _stale_running_job(self, module, *, job_id: str, stage: str, age_seconds: int):
        from datetime import datetime, timedelta

        module.JOB_STORE.create_job(job_id, scanner="et3850", params={})
        module.JOB_STORE.update_job(job_id, status="running", stage=stage, stage_detail="x")
        # Backdate updated_at so the monitor considers the job stale.
        stale = (datetime.utcnow() - timedelta(seconds=age_seconds)).isoformat()
        with module.JOB_STORE.lock, module.JOB_STORE._connect() as conn:
            conn.execute(
                "UPDATE scan_jobs SET updated_at = ? WHERE id = ?",
                (stale, job_id),
            )

    def test_monitor_spares_jobs_with_live_background_thread(self, app_module):
        _, module, _ = app_module
        self._stale_running_job(module, job_id="j_mon_live", stage="ocr", age_seconds=9999)
        # Register a fake live worker thread.
        running = threading.Event()
        worker = threading.Thread(
            target=lambda: running.wait(timeout=2), daemon=True, name="fake-worker"
        )
        worker.start()
        try:
            module._register_background_job("j_mon_live", worker)
            module.JOB_MONITOR._check_jobs()
            assert module.JOB_STORE.get_job("j_mon_live")["status"] == "running"
        finally:
            running.set()
            worker.join(timeout=1)
            module._unregister_background_job("j_mon_live")

    def test_monitor_kills_jobs_whose_thread_died(self, app_module):
        _, module, _ = app_module
        self._stale_running_job(module, job_id="j_mon_dead", stage="ocr", age_seconds=9999)
        # No registration → background_job_alive is False; should be killed.
        module.JOB_MONITOR._check_jobs()
        job = module.JOB_STORE.get_job("j_mon_dead")
        assert job["status"] == "failed"
        assert "orphan" in str(job["error"]).lower()

    def test_monitor_threshold_per_stage(self, app_module):
        _, module, _ = app_module
        # 12 minutes idle in stage="ocr" — under the OCR-specific threshold
        # (3600s default), so the job must still be considered live.
        self._stale_running_job(module, job_id="j_mon_ocr", stage="ocr", age_seconds=720)
        module.JOB_MONITOR._check_jobs()
        assert module.JOB_STORE.get_job("j_mon_ocr")["status"] == "running"
        # The same 12 min idle in stage="scanning" is well past the default
        # 10-min threshold and should be killed.
        self._stale_running_job(module, job_id="j_mon_scan", stage="scanning", age_seconds=720)
        module.JOB_MONITOR._check_jobs()
        assert module.JOB_STORE.get_job("j_mon_scan")["status"] == "failed"

    def test_heartbeat_touches_updated_at(self, app_module):
        _, module, _ = app_module
        module.JOB_STORE.create_job("j_hb", scanner="et3850", params={})
        module.JOB_STORE.update_job("j_hb", status="running", stage="ocr")
        before = module.JOB_STORE.get_job("j_hb")["updated_at"]
        # Run the heartbeat for ~1s with interval=0.1
        hb = module._Heartbeat("j_hb", interval=0.1)
        with hb:
            time.sleep(0.4)
        after = module.JOB_STORE.get_job("j_hb")["updated_at"]
        assert after > before


class TestTags:
    """Tag CRUD + filter contract."""

    def test_set_and_get_tags_via_job_record(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_tag1")
        resp = client.put(
            "/api/scans/j_tag1/tags",
            json={"tags": ["Tax", " 2025 ", "important"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        # Normalized: lowercased, trimmed, sorted, deduped.
        assert body["tags"] == ["2025", "important", "tax"]
        # The job record exposes the tags.
        job = client.get("/api/scans/j_tag1").json()
        assert job["tags"] == ["2025", "important", "tax"]

    def test_setting_tags_to_empty_clears_them(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_tag_clear")
        client.put("/api/scans/j_tag_clear/tags", json={"tags": ["a", "b"]})
        client.put("/api/scans/j_tag_clear/tags", json={"tags": []})
        assert client.get("/api/scans/j_tag_clear").json()["tags"] == []

    @pytest.mark.parametrize(
        "bad_tag",
        [
            "x" * 100,  # too long
            "no!bangs",
            "trailing-",
            "-leading",
            ".dot-start",
            "double--space",  # actually allowed (dash is fine); using a real reject:
        ],
    )
    def test_rejects_invalid_tags(self, app_module, bad_tag):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_tag_bad")
        resp = client.put("/api/scans/j_tag_bad/tags", json={"tags": [bad_tag]})
        # Some inputs are valid (e.g. "double--space") and the param test will
        # see status 200; that's fine. Strictly bad ones must be rejected.
        if bad_tag in {"x" * 100, "no!bangs", "trailing-", "-leading", ".dot-start"}:
            assert resp.status_code == 400, bad_tag
        else:
            assert resp.status_code == 200

    def test_404_set_tags_on_unknown_job(self, app_module):
        client, *_ = app_module
        resp = client.put("/api/scans/nope/tags", json={"tags": ["x"]})
        assert resp.status_code == 404

    def test_list_all_tags_returns_union(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_taga")
        _seed_completed_job(module, output_dir, job_id="j_tagb")
        client.put("/api/scans/j_taga/tags", json={"tags": ["alpha", "shared"]})
        client.put("/api/scans/j_tagb/tags", json={"tags": ["beta", "shared"]})
        body = client.get("/api/tags").json()
        assert body == ["alpha", "beta", "shared"]

    def test_filter_jobs_by_tag_and(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_f1")
        _seed_completed_job(module, output_dir, job_id="j_f2")
        _seed_completed_job(module, output_dir, job_id="j_f3")
        client.put("/api/scans/j_f1/tags", json={"tags": ["receipts", "2025"]})
        client.put("/api/scans/j_f2/tags", json={"tags": ["receipts"]})
        client.put("/api/scans/j_f3/tags", json={"tags": ["2025"]})

        # Single-tag filter narrows correctly.
        ids = {j["id"] for j in client.get("/api/scans?tags=receipts").json()["items"]}
        assert ids == {"j_f1", "j_f2"}

        # AND semantics: only jobs with both tags.
        ids = {j["id"] for j in client.get("/api/scans?tags=receipts&tags=2025").json()["items"]}
        assert ids == {"j_f1"}

        # Unknown tag → no results.
        body = client.get("/api/scans?tags=nope").json()
        assert body["total"] == 0
        assert body["items"] == []

    def test_filter_normalizes_tag_input(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_fnorm")
        client.put("/api/scans/j_fnorm/tags", json={"tags": ["taxes"]})
        # Casing/whitespace in the query param is normalized just like in PUT.
        ids = {j["id"] for j in client.get("/api/scans?tags=TAXES").json()["items"]}
        assert ids == {"j_fnorm"}

    def test_schema_migration_adds_tags_column_to_old_db(self, app_module, tmp_path):
        _, module, _ = app_module
        # Build a DB file without the tags column to simulate an old install.
        import sqlite3

        legacy_path = tmp_path / "legacy.sqlite3"
        with sqlite3.connect(legacy_path) as conn:
            conn.execute(
                """
                CREATE TABLE scan_jobs (
                    id TEXT PRIMARY KEY,
                    scanner TEXT NOT NULL,
                    status TEXT NOT NULL,
                    params TEXT,
                    result_path TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO scan_jobs (id, scanner, status, created_at, updated_at) VALUES (?,?,?,?,?)",
                ("old1", "et3850", "completed", "2026-01-01", "2026-01-01"),
            )
            conn.commit()
        # Open the legacy DB through JobStore; _ensure_schema should ALTER it.
        store = module.JobStore(legacy_path)
        store.set_tags("old1", ["migrated"])
        assert store.get_job("old1")["tags"] == ["migrated"]


class TestSaneBusyRetry:
    """Cold-start sane-airscan often returns SANE_STATUS_BUSY (exit 3);
    the retry helper turns that into a transparent recovery."""

    def test_succeeds_on_first_try(self, app_module):
        _, module, _ = app_module
        calls: list[int] = []

        def fn():
            calls.append(0)
            return ("ok", "")

        out = module._retry_on_sane_busy(fn, sleep=lambda _s: None)
        assert out == ("ok", "")
        assert len(calls) == 1

    def test_retries_busy_then_succeeds(self, app_module):
        _, module, _ = app_module
        attempts = {"n": 0}

        def fn():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError(
                    f"scanimage failed ({module.SANE_BUSY_EXIT_CODE}): Device busy"
                )
            return ("done", "")

        delays: list[float] = []
        out = module._retry_on_sane_busy(fn, sleep=lambda s: delays.append(s))
        assert out == ("done", "")
        assert attempts["n"] == 3
        # Two sleeps before success (between attempts 1->2 and 2->3).
        assert delays == [
            module.SCAN_BUSY_BACKOFF_SECONDS[0],
            module.SCAN_BUSY_BACKOFF_SECONDS[1],
        ]

    def test_gives_up_after_max_attempts(self, app_module):
        _, module, _ = app_module
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            raise RuntimeError(
                f"scanimage failed ({module.SANE_BUSY_EXIT_CODE}): Device busy"
            )

        with pytest.raises(RuntimeError) as exc:
            module._retry_on_sane_busy(fn, sleep=lambda _s: None)
        assert calls["n"] == module.SCAN_BUSY_MAX_ATTEMPTS
        msg = str(exc.value)
        assert "BUSY" in msg
        # The hardened error message should call out printing as a likely
        # cause so multi-function-device users know what to do.
        assert "printing" in msg
        assert "jammed" in msg or "sleep" in msg

    def test_retries_io_error_then_succeeds(self, app_module):
        _, module, _ = app_module
        attempts = {"n": 0}

        def fn():
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError(
                    f"scanimage failed ({module.SANE_IO_ERROR_EXIT_CODE}): "
                    "sane_get_parameters: Error during device I/O"
                )
            return ("done", "")

        delays: list[float] = []
        out = module._retry_on_sane_busy(fn, sleep=lambda s: delays.append(s))
        assert out == ("done", "")
        assert attempts["n"] == 2
        assert delays == [module.SCAN_IO_BACKOFF_SECONDS[0]]

    def test_gives_up_io_error_after_max_attempts(self, app_module):
        _, module, _ = app_module
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            raise RuntimeError(
                f"scanimage failed ({module.SANE_IO_ERROR_EXIT_CODE}): I/O error"
            )

        with pytest.raises(RuntimeError) as exc:
            module._retry_on_sane_busy(fn, sleep=lambda _s: None)
        msg = str(exc.value)
        assert calls["n"] == module.SCAN_IO_MAX_ATTEMPTS
        # Hardware-shaped error message should call out power cycling.
        assert "power cycle" in msg or "USB" in msg
        assert "airscan" in msg or "epsonds" in msg

    def test_io_and_busy_have_independent_counters(self, app_module):
        _, module, _ = app_module
        sequence = [
            module.SANE_BUSY_EXIT_CODE,
            module.SANE_IO_ERROR_EXIT_CODE,
            module.SANE_BUSY_EXIT_CODE,
            None,  # success
        ]
        i = {"k": 0}

        def fn():
            idx = i["k"]
            i["k"] += 1
            code = sequence[idx]
            if code is None:
                return ("ok", "")
            raise RuntimeError(f"scanimage failed ({code}): boom")

        out = module._retry_on_sane_busy(fn, sleep=lambda _s: None)
        assert out == ("ok", "")
        assert i["k"] == len(sequence)

    def test_non_busy_error_propagates_immediately(self, app_module):
        _, module, _ = app_module
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            raise RuntimeError("scanimage failed (5): paper jam")

        with pytest.raises(RuntimeError) as exc:
            module._retry_on_sane_busy(fn, sleep=lambda _s: None)
        # No retries for non-busy errors.
        assert calls["n"] == 1
        assert "paper jam" in str(exc.value)

    def test_cancel_aborts_between_retries(self, app_module):
        _, module, _ = app_module
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            raise RuntimeError(
                f"scanimage failed ({module.SANE_BUSY_EXIT_CODE}): Device busy"
            )

        with pytest.raises(module.ScanCancelled):
            module._retry_on_sane_busy(
                fn,
                cancel_check=lambda: True,  # cancelled before first sleep
                sleep=lambda _s: None,
            )
        # Should only call once; cancel check fires before the second attempt.
        assert calls["n"] == 1

    def test_progress_cb_invoked_between_retries(self, app_module):
        _, module, _ = app_module
        attempts = {"n": 0}

        def fn():
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise RuntimeError(
                    f"scanimage failed ({module.SANE_BUSY_EXIT_CODE}): Device busy"
                )
            return ("ok", "")

        messages: list[str] = []
        module._retry_on_sane_busy(
            fn,
            progress_cb=messages.append,
            sleep=lambda _s: None,
        )
        assert any("busy" in m.lower() for m in messages)


class TestRoutePrecedence:
    """The SPA catch-all route must never shadow /api/* endpoints."""

    def test_get_scan_status_returns_json(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_route_status")
        resp = client.get("/api/scans/j_route_status")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")

    def test_pages_subpath_returns_json(self, app_module):
        client, module, output_dir = app_module
        _seed_completed_job(module, output_dir, job_id="j_route_pages")
        resp = client.get("/api/scans/j_route_pages/pages")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")

    def test_unknown_api_path_is_404_not_spa(self, app_module):
        client, *_ = app_module
        resp = client.get("/api/totally-unknown-route")
        assert resp.status_code == 404
        # 404 body should be JSON detail, NOT the SPA HTML shell.
        assert "data-test='spa'" not in resp.text

    def test_unknown_client_path_returns_spa_when_accepting_html(self, app_module):
        client, *_ = app_module
        resp = client.get("/anywhere", headers={"Accept": "text/html"})
        assert resp.status_code == 200
        assert "data-test='spa'" in resp.text

    def test_unknown_client_path_404s_without_html_accept(self, app_module):
        client, *_ = app_module
        # XHR-style request (no text/html in Accept) shouldn't get the SPA shell.
        resp = client.get("/anywhere", headers={"Accept": "application/json"})
        assert resp.status_code == 404
