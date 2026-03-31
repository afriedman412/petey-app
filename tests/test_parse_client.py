"""
Tests for the parser service client (server/parse_client.py).

All tests mock httpx to avoid needing a running parser service.
"""
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from server.parse_client import parse_fn, page_parse_fn, get_page_count

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = str(FIXTURES / "mci_page1.pdf")


# ---------------------------------------------------------------------------
# parse_fn
# ---------------------------------------------------------------------------

class TestParseFn:
    @pytest.mark.asyncio
    async def test_returns_text(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "hello world", "page_count": 1}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            result = await parse_fn(MCI_PDF, "pymupdf")

        assert result == "hello world"

    @pytest.mark.asyncio
    async def test_sends_correct_parser(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "ok", "page_count": 1}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            await parse_fn(MCI_PDF, "tables")

        call_kwargs = mock_client.post.call_args
        assert call_kwargs.kwargs["data"]["parser"] == "tables"

    @pytest.mark.asyncio
    async def test_posts_to_parse_endpoint(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "ok", "page_count": 1}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            await parse_fn(MCI_PDF)

        assert mock_client.post.call_args[0][0] == "/parse"

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock(),
        )

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            with pytest.raises(httpx.HTTPStatusError):
                await parse_fn(MCI_PDF)


# ---------------------------------------------------------------------------
# page_parse_fn
# ---------------------------------------------------------------------------

class TestPageParseFn:
    @pytest.mark.asyncio
    async def test_returns_text(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "page 0 text", "page_index": 0}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            result = await page_parse_fn(MCI_PDF, 0, "pymupdf")

        assert result == "page 0 text"

    @pytest.mark.asyncio
    async def test_sends_page_index(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "ok", "page_index": 3}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            await page_parse_fn(MCI_PDF, 3, "pymupdf")

        call_kwargs = mock_client.post.call_args
        assert call_kwargs.kwargs["data"]["page_index"] == 3

    @pytest.mark.asyncio
    async def test_posts_to_page_endpoint(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"text": "ok", "page_index": 0}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            await page_parse_fn(MCI_PDF, 0)

        assert mock_client.post.call_args[0][0] == "/parse/page"


# ---------------------------------------------------------------------------
# get_page_count
# ---------------------------------------------------------------------------

class TestGetPageCount:
    @pytest.mark.asyncio
    async def test_returns_count(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"page_count": 5}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            result = await get_page_count(MCI_PDF)

        assert result == 5

    @pytest.mark.asyncio
    async def test_posts_to_page_count_endpoint(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"page_count": 1}
        mock_resp.raise_for_status = MagicMock()

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            await get_page_count(MCI_PDF)

        assert mock_client.post.call_args[0][0] == "/page-count"

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock(),
        )

        with patch("server.parse_client._get_client") as mock_gc:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_gc.return_value = mock_client
            with pytest.raises(httpx.HTTPStatusError):
                await get_page_count(MCI_PDF)


# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------

class TestClientSingleton:
    def test_reuses_client(self):
        import server.parse_client as pc
        pc._client = None  # reset
        c1 = pc._get_client()
        c2 = pc._get_client()
        assert c1 is c2
        # cleanup
        pc._client = None

    def test_recreates_if_closed(self):
        import server.parse_client as pc
        pc._client = MagicMock(is_closed=True)
        c2 = pc._get_client()
        assert not c2.is_closed
        pc._client = None
