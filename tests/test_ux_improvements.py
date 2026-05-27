"""
Tests for UX improvements batch (2026-04-06).

Covers:
- Runs store and return schema_spec
- Demo CSV files served via static mount
- Favicon served
- No-key error messages from extract endpoints
- Template favicon links
"""
import json
import os
from pathlib import Path

import pytest

os.environ["FIREBASE_AUTH_DISABLED"] = "1"

from server.runs import (
    create_run, get_run, LOCAL_RUNS_PATH,
)


FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = FIXTURES / "mci_page1.pdf"
STATIC_DIR = Path(__file__).parent.parent / "static"
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"


# ---------------------------------------------------------------------------
# Runs: schema_spec persistence
# ---------------------------------------------------------------------------

class TestRunsSchemaSpec:
    @pytest.fixture(autouse=True)
    def clean_runs(self):
        LOCAL_RUNS_PATH.unlink(missing_ok=True)
        yield
        LOCAL_RUNS_PATH.unlink(missing_ok=True)

    def test_schema_spec_stored_on_create(self):
        schema = {
            "name": "test_schema",
            "mode": "table",
            "fields": {
                "name": {"type": "string", "description": "Person name"},
                "age": {"type": "number", "description": "Age"},
            },
        }
        run = create_run("local-dev", {
            "filename": "test.pdf",
            "schema_spec": schema,
        })
        fetched = get_run("local-dev", run["id"])
        assert fetched["schema_spec"] == schema

    def test_schema_spec_none_when_omitted(self):
        run = create_run("local-dev", {"filename": "test.pdf"})
        fetched = get_run("local-dev", run["id"])
        assert fetched["schema_spec"] is None

    def test_schema_spec_with_enum_fields(self):
        schema = {
            "name": "enum_test",
            "fields": {
                "status": {
                    "type": "enum",
                    "description": "Status",
                    "values": ["active", "inactive"],
                },
            },
        }
        run = create_run("local-dev", {"schema_spec": schema})
        fetched = get_run("local-dev", run["id"])
        assert fetched["schema_spec"]["fields"]["status"]["values"] == ["active", "inactive"]


# ---------------------------------------------------------------------------
# Static assets: favicon, demo CSVs
# ---------------------------------------------------------------------------

class TestStaticAssets:
    @pytest.fixture
    def client(self):
        """Reload app with correct BASE_DIR in case another test changed it."""
        import importlib
        import server.app as app_mod
        importlib.reload(app_mod)
        from fastapi.testclient import TestClient
        return TestClient(app_mod.app)

    def test_favicon_exists(self):
        favicon = STATIC_DIR / "favicon.svg"
        assert favicon.exists(), "favicon.svg missing from static/"
        content = favicon.read_text()
        assert "<svg" in content

    def test_favicon_served(self, client):
        resp = client.get("/static/favicon.svg")
        assert resp.status_code == 200
        assert "svg" in resp.headers.get("content-type", "")

    def test_demo_csvs_exist(self):
        demo_dir = STATIC_DIR / "demo"
        csvs = list(demo_dir.glob("*.csv"))
        assert len(csvs) > 0, "No demo CSV files found in static/demo/"

    def test_demo_csv_served(self, client):
        demo_dir = STATIC_DIR / "demo"
        csvs = list(demo_dir.glob("*.csv"))
        if not csvs:
            pytest.skip("No demo CSVs present")
        resp = client.get(f"/static/demo/{csvs[0].name}")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Templates: favicon link present
# ---------------------------------------------------------------------------

class TestTemplateFavicon:
    def test_all_templates_have_favicon(self):
        missing = []
        for tmpl in TEMPLATES_DIR.glob("*.html"):
            content = tmpl.read_text()
            if "favicon.svg" not in content:
                missing.append(tmpl.name)
        assert missing == [], f"Templates missing favicon link: {missing}"


# ---------------------------------------------------------------------------
# Extract endpoints: no-key error messages
# ---------------------------------------------------------------------------

class TestExtractNoKeyErrors:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from server.app import app
        return TestClient(app)

    def test_extract_no_openai_key(self, client):
        """Extract with OpenAI model but no key returns clear error."""
        from unittest.mock import patch
        mock_settings = {
            "model": "gpt-4o",
            "openai_api_key": "",
            "anthropic_api_key": "",
            "concurrency": 10,
        }
        with patch("server.app.get_settings", return_value=mock_settings):
            with open(MCI_PDF, "rb") as f:
                resp = client.post(
                    "/extract",
                    files=[("file", ("test.pdf", f, "application/pdf"))],
                    data={
                        "schema_spec": json.dumps({
                            "name": "test",
                            "fields": {"x": {"type": "string"}},
                        }),
                    },
                )
        assert resp.status_code == 400
        assert "API key" in resp.json().get("error", "")

    def test_extract_stream_no_anthropic_key(self, client):
        """Stream extract with Anthropic model but no key returns clear error."""
        from unittest.mock import patch
        mock_settings = {
            "model": "claude-sonnet-4-20250514",
            "openai_api_key": "",
            "anthropic_api_key": "",
            "concurrency": 10,
        }
        with patch("server.app.get_settings", return_value=mock_settings):
            with open(MCI_PDF, "rb") as f:
                resp = client.post(
                    "/extract/stream",
                    files=[("file", ("test.pdf", f, "application/pdf"))],
                    data={
                        "schema_spec": json.dumps({
                            "name": "test",
                            "mode": "table",
                            "fields": {"x": {"type": "string"}},
                        }),
                    },
                )
        assert resp.status_code == 400
        assert "API key" in resp.json().get("error", "")

    def test_extract_no_schema(self, client):
        """Extract without schema returns clear error."""
        from unittest.mock import patch
        mock_settings = {
            "model": "gpt-4o",
            "openai_api_key": "sk-test",
            "anthropic_api_key": "",
            "concurrency": 10,
        }
        with patch("server.app.get_settings", return_value=mock_settings):
            with open(MCI_PDF, "rb") as f:
                resp = client.post(
                    "/extract",
                    files=[("file", ("test.pdf", f, "application/pdf"))],
                )
        assert resp.status_code == 400
        assert "blueprint" in resp.json().get("error", "").lower()
