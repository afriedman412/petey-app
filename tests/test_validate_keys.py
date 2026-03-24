"""
Tests for API key validation functions (server/validate_keys.py).

All tests mock httpx to avoid real network calls.
"""
import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock

from server.validate_keys import (
    validate_openai_key,
    validate_anthropic_key,
    validate_mistral_key,
    validate_datalab_key,
)


# ---------------------------------------------------------------------------
# Datalab key validation
# ---------------------------------------------------------------------------

class TestValidateDatalabKey:
    @pytest.mark.asyncio
    async def test_invalid_key_returns_false(self):
        mock_resp = MagicMock(status_code=401)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_datalab_key("bad-key")
        assert valid is False
        assert "Invalid" in msg

    @pytest.mark.asyncio
    async def test_valid_key_422(self):
        mock_resp = MagicMock(status_code=422)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_datalab_key("good-key")
        assert valid is True
        assert msg == "Valid"

    @pytest.mark.asyncio
    async def test_valid_key_400(self):
        mock_resp = MagicMock(status_code=400)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_datalab_key("good-key")
        assert valid is True

    @pytest.mark.asyncio
    async def test_timeout(self):
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = httpx.TimeoutException("timeout")
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_datalab_key("any-key")
        assert valid is False
        assert "timed out" in msg.lower()

    @pytest.mark.asyncio
    async def test_unexpected_status(self):
        mock_resp = MagicMock(status_code=500)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_datalab_key("any-key")
        assert valid is False
        assert "500" in msg

    @pytest.mark.asyncio
    async def test_sends_correct_header(self):
        mock_resp = MagicMock(status_code=422)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            await validate_datalab_key("dk_test123")
        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        assert call_kwargs.kwargs["headers"]["X-API-Key"] == "dk_test123"


# ---------------------------------------------------------------------------
# OpenAI key validation
# ---------------------------------------------------------------------------

class TestValidateOpenaiKey:
    @pytest.mark.asyncio
    async def test_valid_key(self):
        mock_resp = MagicMock(status_code=200)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_openai_key("sk-test")
        assert valid is True

    @pytest.mark.asyncio
    async def test_invalid_key(self):
        mock_resp = MagicMock(status_code=401)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_openai_key("bad-key")
        assert valid is False


# ---------------------------------------------------------------------------
# Anthropic key validation
# ---------------------------------------------------------------------------

class TestValidateAnthropicKey:
    @pytest.mark.asyncio
    async def test_valid_key(self):
        mock_resp = MagicMock(status_code=200)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_anthropic_key("sk-ant-test")
        assert valid is True

    @pytest.mark.asyncio
    async def test_valid_key_rate_limited(self):
        mock_resp = MagicMock(status_code=429)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_anthropic_key("sk-ant-test")
        assert valid is True
        assert "rate limited" in msg.lower()

    @pytest.mark.asyncio
    async def test_valid_key_billing_issue(self):
        mock_resp = MagicMock(status_code=400)
        mock_resp.json.return_value = {"error": {"message": "Your credit balance is too low"}}
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_anthropic_key("sk-ant-test")
        assert valid is True
        assert "billing" in msg.lower()

    @pytest.mark.asyncio
    async def test_invalid_key(self):
        mock_resp = MagicMock(status_code=401)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_anthropic_key("bad-key")
        assert valid is False


# ---------------------------------------------------------------------------
# Mistral key validation
# ---------------------------------------------------------------------------

class TestValidateMistralKey:
    @pytest.mark.asyncio
    async def test_valid_key(self):
        mock_resp = MagicMock(status_code=200)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_mistral_key("ms-test")
        assert valid is True

    @pytest.mark.asyncio
    async def test_invalid_key(self):
        mock_resp = MagicMock(status_code=401)
        with patch("server.validate_keys.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_client
            valid, msg = await validate_mistral_key("bad-key")
        assert valid is False
