"""
Tests for local (in-process) parsing in server/parse_client.py.

These test the _local_* functions directly, using the fixture PDF.
No mocks needed — these hit real pymupdf/pdfplumber.
"""
from pathlib import Path

import pytest

from server.parse_client import (
    _local_parse,
    _local_page_parse,
    _local_page_count,
    _parse_pages_sync,
    _parse_single_sync,
)

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = str(FIXTURES / "mci_page1.pdf")


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------

class TestParsePagesSyncPymupdf:
    def test_returns_list(self):
        pages = _parse_pages_sync(MCI_PDF, "pymupdf")
        assert isinstance(pages, list)
        assert len(pages) == 1

    def test_text_not_empty(self):
        pages = _parse_pages_sync(MCI_PDF, "pymupdf")
        assert len(pages[0]) > 50

    def test_contains_expected_text(self):
        pages = _parse_pages_sync(MCI_PDF, "pymupdf")
        assert "CAPITAL IMPROVEMENT" in pages[0].upper()


class TestParsePagesSyncPdfplumber:
    def test_returns_list(self):
        pages = _parse_pages_sync(MCI_PDF, "pdfplumber")
        assert isinstance(pages, list)
        assert len(pages) == 1

    def test_text_not_empty(self):
        pages = _parse_pages_sync(MCI_PDF, "pdfplumber")
        assert len(pages[0]) > 50


class TestParseSingleSyncPymupdf:
    def test_returns_text(self):
        text = _parse_single_sync(MCI_PDF, 0, "pymupdf")
        assert isinstance(text, str)
        assert len(text) > 50

    def test_contains_expected_text(self):
        text = _parse_single_sync(MCI_PDF, 0, "pymupdf")
        assert "CAPITAL IMPROVEMENT" in text.upper()


class TestParseSingleSyncPdfplumber:
    def test_returns_text(self):
        text = _parse_single_sync(MCI_PDF, 0, "pdfplumber")
        assert isinstance(text, str)
        assert len(text) > 50


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

class TestLocalParse:
    @pytest.mark.asyncio
    async def test_pymupdf_returns_text(self):
        text = await _local_parse(MCI_PDF, "pymupdf")
        assert isinstance(text, str)
        assert len(text) > 50

    @pytest.mark.asyncio
    async def test_pdfplumber_returns_text(self):
        text = await _local_parse(MCI_PDF, "pdfplumber")
        assert isinstance(text, str)
        assert len(text) > 50


class TestLocalPageParse:
    @pytest.mark.asyncio
    async def test_returns_page_text(self):
        text = await _local_page_parse(MCI_PDF, 0, "pymupdf")
        assert isinstance(text, str)
        assert "CAPITAL IMPROVEMENT" in text.upper()

    @pytest.mark.asyncio
    async def test_pdfplumber_returns_page_text(self):
        text = await _local_page_parse(MCI_PDF, 0, "pdfplumber")
        assert isinstance(text, str)
        assert len(text) > 50


class TestLocalPageCount:
    @pytest.mark.asyncio
    async def test_returns_count(self):
        count = await _local_page_count(MCI_PDF)
        assert count == 1


# ---------------------------------------------------------------------------
# Routing: _USE_LOCAL flag
# ---------------------------------------------------------------------------

class TestRoutingFlag:
    def test_empty_parser_url_uses_local(self):
        """When PARSER_URL is empty, parse_fn should be the local impl."""
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"PARSER_URL": ""}, clear=False):
            # Re-evaluate the module-level flag
            import importlib
            import server.parse_client as pc
            importlib.reload(pc)
            assert pc._USE_LOCAL is True
            assert pc.parse_fn is pc._local_parse

    def test_set_parser_url_uses_remote(self):
        """When PARSER_URL is set, parse_fn should be the remote impl."""
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"PARSER_URL": "http://parser:8081"}):
            import importlib
            import server.parse_client as pc
            importlib.reload(pc)
            assert pc._USE_LOCAL is False
            assert pc.parse_fn is pc._remote_parse

    def test_local_keyword_uses_local(self):
        """PARSER_URL=local should use local parsing."""
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"PARSER_URL": "local"}):
            import importlib
            import server.parse_client as pc
            importlib.reload(pc)
            assert pc._USE_LOCAL is True
