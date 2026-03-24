"""
Tests for the standalone parser service.

Starts the parser app on a test client and verifies
all endpoints return expected results.
"""
from pathlib import Path

import pytest
from httpx import AsyncClient, ASGITransport

from parser.app import app

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = FIXTURES / "mci_page1.pdf"


@pytest.fixture
def pdf_bytes():
    return MCI_PDF.read_bytes()


@pytest.fixture
def transport():
    return ASGITransport(app=app)


@pytest.mark.asyncio
async def test_health(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_page_count(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/page-count",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
        )
        assert resp.status_code == 200
        assert resp.json()["page_count"] >= 1


@pytest.mark.asyncio
async def test_parse_full(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/parse",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"parser": "pymupdf"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["page_count"] >= 1
        assert len(body["text"]) > 0
        assert "WESTCHESTER COUNTY" in body["text"]


@pytest.mark.asyncio
async def test_parse_single_page(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/parse/page",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"page_index": 0, "parser": "pymupdf"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["page_index"] == 0
        assert len(body["text"]) > 0


@pytest.mark.asyncio
async def test_parse_multiple_pages(transport, pdf_bytes):
    # Get page count first
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        count_resp = await client.post(
            "/page-count",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
        )
        page_count = count_resp.json()["page_count"]

        resp = await client.post(
            "/parse/pages",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"page_indices": "0", "parser": "pymupdf"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["page_index"] == 0


@pytest.mark.asyncio
async def test_parse_page_out_of_range(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/parse/page",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"page_index": 9999, "parser": "pymupdf"},
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_parse_unknown_parser(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/parse",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"parser": "nonexistent"},
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_parse_tables_parser(transport, pdf_bytes):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/parse",
            files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
            data={"parser": "tables"},
        )
        assert resp.status_code == 200
        assert len(resp.json()["text"]) > 0
