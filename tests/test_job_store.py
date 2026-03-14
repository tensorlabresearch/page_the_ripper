"""Tests for the JobStore SQLite persistence layer."""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def job_store(tmp_path, monkeypatch):
    """Create a fresh JobStore backed by a temporary database."""
    monkeypatch.setenv("SCANNER_CFG", str(tmp_path / "scanner.cfg"))
    cfg = tmp_path / "scanner.cfg"
    cfg.write_text(
        "[defaults]\ndpi = 300\ncolor_mode = Grayscale8\noutput_dir = scans\n\n"
        "[scanner:test]\nlabel = Test\nbackend = sane\nsane_hint = Test\n"
    )
    monkeypatch.setenv("SCAN_DB_PATH", str(tmp_path / "test.sqlite3"))
    monkeypatch.setenv("SCAN_OUTPUT_DIR", str(tmp_path / "scans"))

    import main

    module = importlib.reload(main)
    store = module.JobStore(tmp_path / "test_store.sqlite3")
    return store


class TestJobStore:
    def test_create_and_get(self, job_store):
        job_store.create_job("j1", "et3850", {"dpi": 300})
        job = job_store.get_job("j1")
        assert job is not None
        assert job["scanner"] == "et3850"
        assert job["status"] == "pending"
        assert job["params"] == {"dpi": 300}

    def test_get_nonexistent(self, job_store):
        assert job_store.get_job("missing") is None

    def test_update_status(self, job_store):
        job_store.create_job("j2", "test", {"dpi": 150})
        job_store.update_job("j2", status="running", stage="scanning")
        job = job_store.get_job("j2")
        assert job["status"] == "running"
        assert job["stage"] == "scanning"

    def test_update_with_result_path(self, job_store, tmp_path):
        job_store.create_job("j3", "test", {})
        result = tmp_path / "output.pdf"
        result.write_bytes(b"%PDF-1.4")
        job_store.update_job("j3", status="completed", result_path=result, stage="completed")
        job = job_store.get_job("j3")
        assert job["status"] == "completed"
        assert job["result_path"] == str(result)

    def test_update_number_of_pages(self, job_store):
        job_store.create_job("j4", "test", {})
        job_store.update_job("j4", status="running", number_of_pages=5, batch_count=2, batches_completed=1)
        job = job_store.get_job("j4")
        assert job["number_of_pages"] == 5
        assert job["batch_count"] == 2
        assert job["batches_completed"] == 1

    def test_delete_job(self, job_store):
        job_store.create_job("j5", "test", {})
        assert job_store.get_job("j5") is not None
        job_store.delete_job("j5")
        assert job_store.get_job("j5") is None

    def test_list_jobs(self, job_store):
        for i in range(5):
            job_store.create_job(f"list-{i}", "test", {"i": i})
        jobs, total = job_store.list_jobs(offset=0, limit=10)
        assert total == 5
        assert len(jobs) == 5

    def test_list_jobs_pagination(self, job_store):
        for i in range(5):
            job_store.create_job(f"page-{i}", "test", {})
        jobs, total = job_store.list_jobs(offset=2, limit=2)
        assert total == 5
        assert len(jobs) == 2
