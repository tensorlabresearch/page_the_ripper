from __future__ import annotations

import importlib
import time
from pathlib import Path
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient
from PIL import Image


CONFIG_TEMPLATE = """\n[defaults]\ndpi = 300\ncolor_mode = Grayscale8\nverify_ssl = false\ntarget_width = 405\ntarget_height = 636\noutput_dir = scans\n\n[scanner:et3850]\nlabel = Test ET-3850\nbackend = sane\nsane_hint = Test ET-3850\ncommand =\nsource = Flatbed\nduplex = false\nextra_args =\ncolor_mode = Grayscale8\n"""


@pytest.fixture()
def test_app(tmp_path, monkeypatch):
    cfg_path = tmp_path / "scanner.cfg"
    cfg_path.write_text(CONFIG_TEMPLATE)
    monkeypatch.setenv("SCANNER_CFG", str(cfg_path))
    monkeypatch.setenv("SCAN_DB_PATH", str(tmp_path / "jobs.sqlite3"))
    monkeypatch.setenv("SCAN_OUTPUT_DIR", str(tmp_path / "scans"))

    import main  # noqa: F401

    module = importlib.reload(main)

    def fake_dispatch(
        scanner_key: str,
        *,
        dpi: int,
        color_mode: str,
        processing_opts: Dict[str, object],
        progress_cb=None,
        job_entry=None,
        job_id=None,
    ):
        img = Image.new("L", (400, 600), color=240)
        return [img]

    monkeypatch.setattr(module, "dispatch_scan", fake_dispatch)
    monkeypatch.setattr(module, "ocr_page", lambda _: "dummy text")

    def fake_create_pdf(pages, out_path, *, dpi):
        out_path.write_bytes(b"%PDF-FAKE%")

    def fake_run_ocr(input_pdf: Path, output_pdf: Path, *, language: str = module.TESSERACT_LANG, image_dpi: int | None = None):
        output_pdf.write_bytes(Path(input_pdf).read_bytes())

    monkeypatch.setattr(module, "create_pdf_from_images", fake_create_pdf)
    monkeypatch.setattr(module, "run_ocr_on_pdf", fake_run_ocr)

    with TestClient(module.app) as client:
        yield client, module


def wait_for_completion(client: TestClient, job_id: str, *, timeout: float = 5.0) -> Dict[str, object]:
    deadline = time.time() + timeout
    last_payload: Dict[str, object] | None = None
    while time.time() < deadline:
        resp = client.get(f"/api/scans/{job_id}")
        resp.raise_for_status()
        payload = resp.json()
        last_payload = payload
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"Timed out waiting for job {job_id} completion; last payload={last_payload}")


def test_scan_job_lifecycle(test_app):
    client, module = test_app

    resp = client.post(
        "/api/scans",
        json={
            "scanner": "et3850",
            "dpi": 200,
            "color": False,
        },
    )
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    status_payload = wait_for_completion(client, job_id)
    assert status_payload["status"] == "completed"
    assert status_payload["result_path"]
    assert status_payload["stage"] == "completed"
    assert status_payload["duration_seconds"] >= 0

    list_resp = client.get("/api/scans")
    assert list_resp.status_code == 200
    listing = list_resp.json()
    assert listing["total"] >= 1
    ids = [item["id"] for item in listing["items"]]
    assert job_id in ids
    stages = {item["id"]: item.get("stage") for item in listing["items"]}
    assert stages.get(job_id) == "completed"
    durations = {item["id"]: item.get("duration_seconds") for item in listing["items"]}
    assert durations[job_id] >= 0
    stages = {item["id"]: item.get("stage") for item in listing["items"]}
    assert stages.get(job_id) == "completed"

    result = client.get(f"/api/scans/download/{job_id}")
    assert result.status_code == 200
    assert result.headers["content-type"] == "application/pdf"
    body = result.content
    assert body.startswith(b"%PDF"), "Expected a PDF file"

    stored = module.JOB_STORE.get_job(job_id)
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["stage"] == "completed"

    delete_resp = client.delete(f"/api/scans/{job_id}")
    assert delete_resp.status_code == 204
    status_after_delete = client.get(f"/api/scans/{job_id}")
    assert status_after_delete.status_code == 404
    assert module.JOB_STORE.get_job(job_id) is None

    list_after_delete = client.get("/api/scans")
    assert list_after_delete.status_code == 200
    assert job_id not in [item["id"] for item in list_after_delete.json()["items"]]


def test_unknown_scanner_returns_404(test_app):
    client, _ = test_app
    resp = client.post(
        "/api/scans",
        json={"scanner": "missing"},
    )
    assert resp.status_code == 404
