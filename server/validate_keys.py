"""
Validate API keys by making lightweight calls to each provider.
"""
import httpx

OPENAI_MODELS_URL = "https://api.openai.com/v1/models"
ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
MISTRAL_MODELS_URL = "https://api.mistral.ai/v1/models"
DATALAB_CONVERT_URL = "https://www.datalab.to/api/v1/convert"


async def validate_openai_key(api_key: str) -> tuple[bool, str]:
    """Validate an OpenAI key by listing models."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                OPENAI_MODELS_URL,
                headers={"Authorization": f"Bearer {api_key}"},
            )
        if resp.status_code == 200:
            return True, "Valid"
        if resp.status_code == 401:
            return False, "Invalid API key"
        return False, f"Unexpected status {resp.status_code}"
    except httpx.TimeoutException:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)


async def validate_anthropic_key(api_key: str) -> tuple[bool, str]:
    """Validate an Anthropic key with a minimal messages request."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                ANTHROPIC_MESSAGES_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1,
                    "messages": [{"role": "user", "content": "hi"}],
                },
            )
        if resp.status_code == 200:
            return True, "Valid"
        if resp.status_code == 401:
            return False, "Invalid API key"
        # 400 with "credit balance is too low" still means key is valid
        if resp.status_code == 400:
            body = resp.json()
            msg = body.get("error", {}).get("message", "")
            if "credit" in msg.lower() or "billing" in msg.lower():
                return True, "Valid (billing issue detected)"
            return False, msg or "Bad request"
        # 429 means valid key, just rate limited
        if resp.status_code == 429:
            return True, "Valid (rate limited)"
        return False, f"Unexpected status {resp.status_code}"
    except httpx.TimeoutException:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)


async def validate_mistral_key(api_key: str) -> tuple[bool, str]:
    """Validate a Mistral key by listing models."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                MISTRAL_MODELS_URL,
                headers={"Authorization": f"Bearer {api_key}"},
            )
        if resp.status_code == 200:
            return True, "Valid"
        if resp.status_code == 401:
            return False, "Invalid API key"
        return False, f"Unexpected status {resp.status_code}"
    except httpx.TimeoutException:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)


async def validate_datalab_key(api_key: str) -> tuple[bool, str]:
    """Validate a Datalab API key by POSTing to /convert with no file.

    A valid key returns 422 (missing file); an invalid key returns 401.
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                DATALAB_CONVERT_URL,
                headers={"X-API-Key": api_key},
            )
        if resp.status_code == 401:
            return False, "Invalid API key"
        # Valid key with no file body returns 400 or 422
        if resp.status_code in (200, 400, 422):
            return True, "Valid"
        return False, f"Unexpected status {resp.status_code}"
    except httpx.TimeoutException:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)
