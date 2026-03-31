"""
Tests for API parser routing logic in server/extract.py.

Verifies that API-based parsers (datalab, etc.) skip the remote parse
service, while local parsers (pymupdf) use it.
"""
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ["FIREBASE_AUTH_DISABLED"] = "1"

# Import after setting env var so firebase_admin skip works
from server.extract import (  # noqa: E402
    async_extract,
    async_extract_pages,
    extract_text,
    _remote_parse_fn,
    _remote_page_parse_fn,
)

FIXTURES = Path(__file__).parent / "fixtures"
MCI_PDF = str(FIXTURES / "mci_page1.pdf")

MOCK_SETTINGS = {
    "model": "gpt-4o",
    "openai_api_key": "sk-test-key",
    "concurrency": 5,
    "parse_multiplier": 3,
}


# -------------------------------------------------------------------
# async_extract — parse_fn routing
# -------------------------------------------------------------------

class TestAsyncExtractRouting:
    @pytest.mark.asyncio
    async def test_local_parser_uses_remote_parse_fn(self):
        with (
            patch(
                "server.extract.get_settings",
                return_value=MOCK_SETTINGS,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),
            patch(
                "server.extract._extract_async",
                new_callable=AsyncMock,
            ) as mock_ext,

        ):
            mock_ext.return_value = MagicMock()
            await async_extract(
                MCI_PDF, MagicMock(),
                uid="test", parser="pymupdf",
            )

        kw = mock_ext.call_args.kwargs
        assert kw["parse_fn"] is _remote_parse_fn

    @pytest.mark.asyncio
    async def test_api_parser_skips_remote_parse_fn(self):
        with (
            patch(
                "server.extract.get_settings",
                return_value=MOCK_SETTINGS,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),
            patch(
                "server.extract._extract_async",
                new_callable=AsyncMock,
            ) as mock_ext,

        ):
            mock_ext.return_value = MagicMock()
            await async_extract(
                MCI_PDF, MagicMock(),
                uid="test", parser="datalab",
            )

        kw = mock_ext.call_args.kwargs
        assert kw["parse_fn"] is None

    @pytest.mark.asyncio
    async def test_anthropic_requires_key(self):
        settings = {
            **MOCK_SETTINGS,
            "model": "claude-sonnet-4-20250514",
            "anthropic_api_key": "",
        }
        with (
            patch(
                "server.extract.get_settings",
                return_value=settings,
            ),
            patch(
                "server.extract.get_provider",
                return_value="anthropic",
            ),

        ):
            with pytest.raises(ValueError, match="Anthropic"):
                await async_extract(
                    MCI_PDF, MagicMock(), uid="test",
                )

    @pytest.mark.asyncio
    async def test_openai_requires_key(self):
        settings = {
            **MOCK_SETTINGS,
            "model": "gpt-4o",
            "openai_api_key": "",
        }
        with (
            patch(
                "server.extract.get_settings",
                return_value=settings,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),

        ):
            with pytest.raises(ValueError, match="OpenAI"):
                await async_extract(
                    MCI_PDF, MagicMock(), uid="test",
                )


# -------------------------------------------------------------------
# async_extract_pages — parse_fn routing
# -------------------------------------------------------------------

class TestAsyncExtractPagesRouting:
    @pytest.mark.asyncio
    async def test_local_parser_uses_remote(self):
        with (
            patch(
                "server.extract.get_settings",
                return_value=MOCK_SETTINGS,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),
            patch(
                "server.extract._extract_pages_async",
                new_callable=AsyncMock,
            ) as mock_ext,

        ):
            mock_ext.return_value = []
            await async_extract_pages(
                MCI_PDF, MagicMock(),
                uid="test", parser="pymupdf",
            )

        kw = mock_ext.call_args.kwargs
        assert kw["parse_fn"] is _remote_page_parse_fn

    @pytest.mark.asyncio
    async def test_api_parser_skips_remote(self):
        with (
            patch(
                "server.extract.get_settings",
                return_value=MOCK_SETTINGS,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),
            patch(
                "server.extract._extract_pages_async",
                new_callable=AsyncMock,
            ) as mock_ext,

        ):
            mock_ext.return_value = []
            await async_extract_pages(
                MCI_PDF, MagicMock(),
                uid="test", parser="datalab",
            )

        kw = mock_ext.call_args.kwargs
        assert kw["parse_fn"] is None

    @pytest.mark.asyncio
    async def test_passes_concurrency_settings(self):
        with (
            patch(
                "server.extract.get_settings",
                return_value=MOCK_SETTINGS,
            ),
            patch(
                "server.extract.get_provider",
                return_value="openai",
            ),
            patch(
                "server.extract._extract_pages_async",
                new_callable=AsyncMock,
            ) as mock_ext,

            patch(
                "server.extract.configure_concurrency",
            ) as mock_cfg,
        ):
            mock_ext.return_value = []
            await async_extract_pages(
                MCI_PDF, MagicMock(), uid="test",
            )

        kw = mock_ext.call_args.kwargs
        assert kw["concurrency"] == 5
        assert "parse_multiplier" not in kw
        mock_cfg.assert_called_with(api_limit=5)


# -------------------------------------------------------------------
# extract_text — basic
# -------------------------------------------------------------------

class TestExtractText:
    @pytest.mark.asyncio
    async def test_returns_parsed_text(self):
        """extract_text returns text from the remote parse function."""
        long_text = "x" * 200
        with (
            patch(
                "server.extract._remote_parse_fn",
                new_callable=AsyncMock,
                return_value=long_text,
            ),
        ):
            text, info = await extract_text("/fake.pdf")

        assert text == long_text
        assert info == []
