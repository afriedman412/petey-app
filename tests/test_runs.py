"""
Tests for run history (server/runs.py) and page-count endpoint.
"""
import json
import os
from pathlib import Path

import pytest

os.environ["FIREBASE_AUTH_DISABLED"] = "1"

from server.runs import (
    create_run, update_run, finish_run, list_runs,
    get_run, delete_run, LOCAL_RUNS_PATH,
)

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = FIXTURES / "mci_page1.pdf"


# ---------------------------------------------------------------------------
# Runs CRUD (local backend)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_runs():
    """Remove local runs file before and after each test."""
    LOCAL_RUNS_PATH.unlink(missing_ok=True)
    yield
    LOCAL_RUNS_PATH.unlink(missing_ok=True)


class TestCreateRun:
    def test_creates_run_with_id(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        assert "id" in run
        assert run["filename"] == "test.pdf"
        assert run["status"] == "running"

    def test_run_has_timestamps(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        assert run["started_at"] is not None
        assert run["finished_at"] is None

    def test_run_has_defaults(self):
        run = create_run("local-dev", {})
        assert run["parser"] == "pymupdf"
        assert run["concurrency"] == 10
        assert run["pages_parsed"] == 0
        assert run["pages_extracted"] == 0


class TestListRuns:
    def test_empty_list(self):
        assert list_runs("local-dev") == []

    def test_lists_created_runs(self):
        create_run("local-dev", {"filename": "a.pdf"})
        create_run("local-dev", {"filename": "b.pdf"})
        runs = list_runs("local-dev")
        assert len(runs) == 2

    def test_newest_first(self):
        create_run("local-dev", {"filename": "first.pdf"})
        create_run("local-dev", {"filename": "second.pdf"})
        runs = list_runs("local-dev")
        assert runs[0]["filename"] == "second.pdf"

    def test_respects_limit(self):
        for i in range(5):
            create_run("local-dev", {"filename": f"{i}.pdf"})
        runs = list_runs("local-dev", limit=3)
        assert len(runs) == 3


class TestGetRun:
    def test_get_existing(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        fetched = get_run("local-dev", run["id"])
        assert fetched["id"] == run["id"]
        assert fetched["filename"] == "test.pdf"

    def test_get_nonexistent(self):
        assert get_run("local-dev", "nonexistent") is None


class TestUpdateRun:
    def test_update_fields(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        updated = update_run("local-dev", run["id"], {
            "pages_parsed": 5,
            "pages_extracted": 3,
        })
        assert updated["pages_parsed"] == 5
        assert updated["pages_extracted"] == 3

    def test_update_persists(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        update_run("local-dev", run["id"], {"status": "completed"})
        fetched = get_run("local-dev", run["id"])
        assert fetched["status"] == "completed"


class TestFinishRun:
    def test_finish_sets_status_and_timestamp(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        finished = finish_run("local-dev", run["id"], "completed", 10, 10)
        assert finished["status"] == "completed"
        assert finished["finished_at"] is not None
        assert finished["pages_parsed"] == 10
        assert finished["pages_extracted"] == 10

    def test_finish_partial(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        finished = finish_run("local-dev", run["id"], "cancelled", 5, 3)
        assert finished["status"] == "cancelled"
        assert finished["pages_parsed"] == 5
        assert finished["pages_extracted"] == 3


class TestDeleteRun:
    def test_delete_removes_run(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        delete_run("local-dev", run["id"])
        assert get_run("local-dev", run["id"]) is None

    def test_delete_nonexistent_no_error(self):
        delete_run("local-dev", "nonexistent")


# ---------------------------------------------------------------------------
# Page count endpoint
# ---------------------------------------------------------------------------

class TestPageCount:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from server.app import app
        return TestClient(app)

    def test_counts_pages(self, client):
        with open(MCI_PDF, "rb") as f:
            resp = client.post(
                "/page-count",
                files=[("files", ("mci_page1.pdf", f, "application/pdf"))],
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_pages"] > 0
        assert data["file_count"] == 1
        assert "mci_page1.pdf" in data["per_file"]

    def test_multiple_files(self, client):
        files = []
        for name in ["a.pdf", "b.pdf"]:
            files.append(("files", (name, open(MCI_PDF, "rb"), "application/pdf")))
        resp = client.post("/page-count", files=files)
        data = resp.json()
        assert data["file_count"] == 2
        assert data["total_pages"] == data["per_file"]["a.pdf"] + data["per_file"]["b.pdf"]
        for f in files:
            f[1][1].close()
