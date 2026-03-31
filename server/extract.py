"""
Web server extraction layer. Thin wrapper around the petey package.
"""
import asyncio
import os
from pathlib import Path

import yaml
from pydantic import BaseModel

from petey.schema import build_model, load_schema  # noqa: F401
from petey.extract import (
    extract_text as _raw_extract_text,
    extract_async as _extract_async,
    extract_pages_async as _extract_pages_async,
    infer_schema_async as _infer_schema_async,
    infer_schema_vision_async as _infer_schema_vision_async,
    TEXT_WARN_THRESHOLD,
    PARSERS,
)  # noqa: E501
from petey.concurrency import configure as configure_concurrency
from server.settings import get_settings, get_provider
from server.parse_client import parse_fn as _remote_parse_fn, page_parse_fn as _remote_page_parse_fn

BASE_DIR = Path(os.environ.get("PETEY_WEB_BASE", Path(__file__).resolve().parent.parent))
SCHEMAS_DIR = BASE_DIR / "schemas"

# Re-export for backwards compatibility
_build_model = build_model


async def extract_text(
    pdf_path: str,
    *,
    page_range: str | None = None,
    header_pages: int = 0,
) -> tuple[str, list[str]]:
    """Extract text from PDF.

    Returns (text, info_messages).
    info_messages contains human-readable status messages.
    """
    info = []

    if page_range or header_pages:
        from petey.extract import _parse_page_range
        from server.parse_client import get_page_count

        total = await get_page_count(pdf_path)

        # Parse all pages via remote service
        pages = []
        for i in range(total):
            pages.append(await _remote_page_parse_fn(
                pdf_path, i, "pymupdf",
            ))

        parts = []
        if header_pages > 0:
            parts.extend(pages[:header_pages])
        if page_range:
            indices = _parse_page_range(page_range, total)
            indices = [i for i in indices if i >= header_pages]
            parts.extend(pages[i] for i in indices if i < len(pages))
        else:
            parts.extend(pages[header_pages:])
        text = "\n\n".join(parts)
    else:
        text = await _remote_parse_fn(pdf_path, "pymupdf")

    return text, info


def check_text_length(text: str) -> str | None:
    if len(text) > TEXT_WARN_THRESHOLD:
        pages_est = text.count("\n\n") + 1
        return (
            f"Document is large ({len(text):,} chars, "
            f"~{pages_est} pages). "
            "Extraction may be slow or hit token limits."
        )
    return None


async def async_extract(
    pdf_path: str,
    response_model: type[BaseModel],
    uid: str,
    instructions: str = "",
    parser: str = "pymupdf",
    text: str | None = None,
) -> BaseModel:
    """Extract using the model/key from the user's settings."""
    _set_api_keys(uid)
    settings = get_settings(uid)
    model_id = settings["model"]
    provider = get_provider(model_id)
    if provider == "anthropic":
        api_key = settings.get("anthropic_api_key") or None
        if not api_key:
            raise ValueError(
                "No Anthropic API key configured. "
                "Add one in Settings before extracting."
            )
    else:
        api_key = settings.get("openai_api_key") or None
        if not api_key:
            raise ValueError(
                "No OpenAI API key configured. "
                "Add one in Settings before extracting."
            )

    # API-based parsers (datalab, etc.) are handled directly by petey —
    # only local parsers need the remote parse service.
    use_remote = not asyncio.iscoroutinefunction(PARSERS.get(parser))

    configure_concurrency(
        api_limit=settings.get("concurrency", 10),
    )

    return await _extract_async(
        pdf_path, response_model,
        model=model_id, api_key=api_key,
        instructions=instructions,
        parser=parser,
        text=text,
        parse_fn=_remote_parse_fn if use_remote else None,
    )


def _set_api_keys(uid: str):
    """Set API key env vars from user settings."""
    import os
    settings = get_settings(uid)
    for key_name, env_var in [
        ("datalab_api_key", "DATALAB_API_KEY"),
    ]:
        val = settings.get(key_name, "")
        if val:
            os.environ[env_var] = val


def _get_api_key(uid: str) -> tuple[str, str]:
    """Return (model_id, api_key) from user settings, raising if missing."""
    settings = get_settings(uid)
    model_id = settings["model"]
    provider = get_provider(model_id)
    if provider == "anthropic":
        api_key = settings.get("anthropic_api_key") or None
        if not api_key:
            raise ValueError(
                "No Anthropic API key configured. "
                "Add one in Settings before extracting."
            )
    else:
        api_key = settings.get("openai_api_key") or None
        if not api_key:
            raise ValueError(
                "No OpenAI API key configured. "
                "Add one in Settings before extracting."
            )
    return model_id, api_key


async def async_extract_pages(
    pdf_path: str,
    response_model: type[BaseModel],
    uid: str,
    instructions: str = "",
    parser: str = "pymupdf",
    pages_per_chunk: int = 1,
    header_pages: int = 0,
    page_range: str | None = None,
    on_result=None,
    on_parse=None,
) -> list[dict]:
    """Page-chunked extraction using the user's settings."""
    _set_api_keys(uid)
    model_id, api_key = _get_api_key(uid)
    settings = get_settings(uid)
    concurrency = settings.get("concurrency", 10)
    use_remote = not asyncio.iscoroutinefunction(PARSERS.get(parser))

    configure_concurrency(api_limit=concurrency)

    return await _extract_pages_async(
        pdf_path, response_model,
        model=model_id, api_key=api_key,
        instructions=instructions,
        parser=parser,
        pages_per_chunk=pages_per_chunk,
        header_pages=header_pages,
        page_range=page_range,
        on_result=on_result,
        on_parse=on_parse,
        concurrency=concurrency,
        parse_fn=_remote_page_parse_fn if use_remote else None,
    )


async def async_infer_schema(
    pdf_path: str,
    uid: str,
    max_pages: int = 2,
    model_override: str | None = None,
    page_range: str | None = None,
    header_pages: int = 0,
) -> dict:
    """Infer a schema from a sample PDF using the user's settings."""
    settings = get_settings(uid)
    model_id = model_override or settings["model"]
    provider = get_provider(model_id)
    if provider == "anthropic":
        api_key = settings.get("anthropic_api_key") or None
    else:
        api_key = settings.get("openai_api_key") or None
    kwargs = dict(
        model=model_id,
        api_key=api_key,
        max_pages=max_pages,
    )
    if page_range:
        kwargs["page_range"] = page_range
    if header_pages:
        kwargs["header_pages"] = header_pages
    return await _infer_schema_async(pdf_path, **kwargs)


async def async_infer_schema_vision(
    pdf_path: str,
    uid: str,
    max_pages: int = 2,
    model_override: str | None = None,
    page_range: str | None = None,
    header_pages: int = 0,
) -> dict:
    """Infer a schema using vision (PDF pages as images)."""
    settings = get_settings(uid)
    model_id = model_override or settings["model"]
    provider = get_provider(model_id)
    if provider == "anthropic":
        api_key = settings.get("anthropic_api_key") or None
    else:
        api_key = settings.get("openai_api_key") or None
    kwargs = dict(
        model=model_id,
        api_key=api_key,
        max_pages=max_pages,
    )
    if page_range:
        kwargs["page_range"] = page_range
    if header_pages:
        kwargs["header_pages"] = header_pages
    return await _infer_schema_vision_async(pdf_path, **kwargs)


def list_schemas() -> list[dict]:
    schemas = []
    for p in sorted(SCHEMAS_DIR.glob("*.yaml")):
        with open(p) as f:
            spec = yaml.safe_load(f)
        schemas.append({
            "file": p.name,
            "name": spec.get("name", p.stem),
            "description": spec.get("description", ""),
            "fields": list(spec.get("fields", {}).keys()),
        })
    return schemas
