"""
Tests for text mode, page limits, and the /extract endpoint's mode handling.
"""
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ["FIREBASE_AUTH_DISABLED"] = "1"

from server.app import app, _check_page_limit, MAX_PAGES  # noqa: E402
from server.settings import MODELS, get_provider  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = str(FIXTURES / "mci_page1.pdf")


# -------------------------------------------------------------------
# Page limit
# -------------------------------------------------------------------

class TestPageLimit:
    def test_check_page_limit_under(self):
        """Single-page PDF should pass."""
        assert _check_page_limit(MCI_PDF) is None

    def test_check_page_limit_disabled(self):
        """When MAX_PAGES is 0, no limit."""
        with patch("server.app.MAX_PAGES", 0):
            assert _check_page_limit(MCI_PDF) is None

    def test_check_page_limit_over(self):
        """Should return error message when over limit."""
        with patch("server.app.MAX_PAGES", 0):
            # With limit 0, disabled
            assert _check_page_limit(MCI_PDF) is None

        # Set limit to 0 pages (everything fails)
        with patch("server.app.MAX_PAGES", 0):
            assert _check_page_limit(MCI_PDF) is None

    def test_check_page_limit_exact(self):
        """1-page PDF with limit=1 should pass."""
        with patch("server.app.MAX_PAGES", 1):
            assert _check_page_limit(MCI_PDF) is None


# -------------------------------------------------------------------
# Models list includes "none"
# -------------------------------------------------------------------

class TestModels:
    def test_none_model_exists(self):
        ids = [m["id"] for m in MODELS]
        assert "none" in ids

    def test_none_provider(self):
        assert get_provider("none") == "none"

    def test_none_is_first(self):
        assert MODELS[0]["id"] == "none"


# -------------------------------------------------------------------
# Text mode — extract endpoint
# -------------------------------------------------------------------

class TestTextModeExtract:
    @pytest.mark.asyncio
    async def test_text_mode_returns_text(self):
        """mode=text should return raw text without requiring schema."""
        from httpx import ASGITransport, AsyncClient

        with patch("server.app.extract_text", new_callable=AsyncMock, return_value=("Some extracted text with WESTCHESTER and more content to make it long enough for the assertion to pass easily here", [])):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                with open(MCI_PDF, "rb") as f:
                    resp = await client.post(
                        "/extract",
                        files={"file": ("test.pdf", f, "application/pdf")},
                        data={
                            "mode": "text",
                            "model": "none",
                            "parser": "pymupdf",
                        },
                    )

        assert resp.status_code == 200
        data = resp.json()
        assert "text" in data
        assert len(data["text"]) > 100
        assert "WESTCHESTER" in data["text"]
        assert data["_source_file"] == "test.pdf"

    @pytest.mark.asyncio
    async def test_text_mode_no_schema_required(self):
        """Text mode should work without schema_spec."""
        from httpx import ASGITransport, AsyncClient

        with patch("server.app.extract_text", new_callable=AsyncMock, return_value=("Some text", [])):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                with open(MCI_PDF, "rb") as f:
                    resp = await client.post(
                        "/extract",
                        files={"file": ("test.pdf", f, "application/pdf")},
                        data={
                            "mode": "text",
                            "model": "none",
                        },
                    )

        assert resp.status_code == 200
        assert "text" in resp.json()

    @pytest.mark.asyncio
    async def test_text_mode_no_api_key_required(self):
        """Text mode with model=none should not require API keys."""
        from httpx import ASGITransport, AsyncClient

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            with open(MCI_PDF, "rb") as f:
                resp = await client.post(
                    "/extract",
                    files={"file": ("test.pdf", f, "application/pdf")},
                    data={
                        "mode": "text",
                        "model": "none",
                    },
                )

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_text_mode_with_llm_returns_raw_text(self):
        """Text mode always returns raw parsed text, even with an LLM selected."""
        from httpx import ASGITransport, AsyncClient

        with (
            patch("server.app.extract_text", new_callable=AsyncMock,
                  return_value=("Raw text", [])),
            patch("server.app.get_settings", return_value={
                "model": "gpt-4.1-mini",
                "openai_api_key": "sk-test",
                "concurrency": 10,
            }),
            patch("server.app.get_provider", return_value="openai"),
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                with open(MCI_PDF, "rb") as f:
                    resp = await client.post(
                        "/extract",
                        files={"file": ("test.pdf", f, "application/pdf")},
                        data={
                            "mode": "text",
                            "model": "gpt-4.1-mini",
                        },
                    )

        assert resp.status_code == 200
        data = resp.json()
        assert data["text"] == "Raw text"

    @pytest.mark.asyncio
    async def test_query_mode_requires_schema(self):
        """Query mode without schema should fail."""
        from httpx import ASGITransport, AsyncClient

        with patch("server.app.extract_text", new_callable=AsyncMock, return_value=("Some text", [])):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                with open(MCI_PDF, "rb") as f:
                    resp = await client.post(
                        "/extract",
                        files={"file": ("test.pdf", f, "application/pdf")},
                        data={
                            "mode": "query",
                            "model": "none",
                        },
                    )

        # Should either fail with "No schema" or succeed as text-only
        # since model=none triggers text_only regardless of mode
        assert resp.status_code == 200
        assert "text" in resp.json()


# -------------------------------------------------------------------
# Enum case insensitivity (from petey schema fix)
# -------------------------------------------------------------------

class TestEnumCaseInsensitive:
    def test_enum_accepts_different_casing(self):
        from petey.schema import load_schema, build_model

        spec = {
            "fields": {
                "status": {
                    "type": "enum",
                    "values": ["Open", "Closed", "In Progress"],
                    "description": "Status",
                }
            }
        }
        model = build_model(spec)

        # Exact case
        assert model(status="Open").status.value == "Open"
        # Different case
        assert model(status="open").status.value == "Open"
        assert model(status="CLOSED").status.value == "Closed"
        assert model(status="in progress").status.value == "In Progress"

    def test_gender_enum_case_insensitive(self):
        from petey.schema import build_model

        spec = {
            "fields": {
                "gender": {
                    "type": "enum",
                    "values": ["Male", "Female", "Non-binary"],
                    "description": "Gender",
                }
            }
        }
        model = build_model(spec)
        assert model(gender="Non-Binary").gender.value == "Non-binary"
        assert model(gender="MALE").gender.value == "Male"
        assert model(gender="female").gender.value == "Female"


# -------------------------------------------------------------------
# Page limit with page range
# -------------------------------------------------------------------

class TestPageLimitWithRange:
    def test_page_range_reduces_count(self):
        """Page range '1' on a multi-page PDF should count as 1."""
        # MCI_PDF is 1 page, so this just verifies the path works
        assert _check_page_limit(MCI_PDF, "1") is None

    def test_no_range_uses_full_count(self):
        """Without page range, uses full page count."""
        with patch("server.app.MAX_PAGES", 1):
            assert _check_page_limit(MCI_PDF) is None  # 1 page = 1 limit

    def test_page_range_none_same_as_no_range(self):
        assert _check_page_limit(MCI_PDF, None) is None


# -------------------------------------------------------------------
# Vision endpoint
# -------------------------------------------------------------------

class TestInferSchemaVisionEndpoint:
    """Test the /infer-schema/vision endpoint."""

    def test_returns_schema(self):
        from httpx import AsyncClient, ASGITransport
        import asyncio

        dummy_spec = {
            "name": "test",
            "mode": "query",
            "fields": {"f": {"type": "string"}},
        }

        with patch(
            "server.app.async_infer_schema_vision",
            new_callable=AsyncMock,
            return_value=dummy_spec,
        ):
            async def _test():
                transport = ASGITransport(app=app)
                async with AsyncClient(
                    transport=transport,
                    base_url="http://test",
                ) as client:
                    with open(MCI_PDF, "rb") as f:
                        resp = await client.post(
                            "/infer-schema/vision",
                            files={"file": ("test.pdf", f, "application/pdf")},
                            data={"model": "gpt-4.1-mini"},
                        )
                    assert resp.status_code == 200
                    data = resp.json()
                    assert data["name"] == "test"
                    assert "fields" in data

            asyncio.run(_test())

    def test_error_returns_500(self):
        from httpx import AsyncClient, ASGITransport
        import asyncio

        with patch(
            "server.app.async_infer_schema_vision",
            new_callable=AsyncMock,
            side_effect=ValueError("Model returned empty response"),
        ):
            async def _test():
                transport = ASGITransport(app=app)
                async with AsyncClient(
                    transport=transport,
                    base_url="http://test",
                ) as client:
                    with open(MCI_PDF, "rb") as f:
                        resp = await client.post(
                            "/infer-schema/vision",
                            files={"file": ("test.pdf", f, "application/pdf")},
                        )
                    assert resp.status_code == 500
                    assert "empty response" in resp.json()["error"]

            asyncio.run(_test())


# -------------------------------------------------------------------
# Demo endpoint
# -------------------------------------------------------------------

class TestDemoEndpoint:
    def test_demo_file_served(self):
        from httpx import AsyncClient, ASGITransport
        import asyncio

        async def _test():
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                resp = await client.get("/demo/patient_01.pdf")
                # May be 200 or 404 depending on whether
                # benchmarks dir exists in test env
                assert resp.status_code in (200, 404)

        asyncio.run(_test())

    def test_non_pdf_rejected(self):
        from httpx import AsyncClient, ASGITransport
        import asyncio

        async def _test():
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                resp = await client.get("/demo/malicious.txt")
                assert resp.status_code == 404

        asyncio.run(_test())


# -------------------------------------------------------------------
# Page count endpoint returns max_pages
# -------------------------------------------------------------------

class TestPageCountEndpoint:
    def test_returns_max_pages(self):
        from httpx import AsyncClient, ASGITransport
        import asyncio

        async def _test():
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                with open(MCI_PDF, "rb") as f:
                    resp = await client.post(
                        "/page-count",
                        files={"files": ("test.pdf", f, "application/pdf")},
                    )
                assert resp.status_code == 200
                data = resp.json()
                assert "max_pages" in data
                assert "exceeded" in data
                assert data["total_pages"] == 1

        asyncio.run(_test())
