"""
Web server extraction layer. Thin wrapper around the petey package.
"""
from pathlib import Path

import yaml
from pydantic import BaseModel

from petey.schema import build_model, load_schema  # noqa: F401
from petey.extract import (
    extract_text as _raw_extract_text,
    extract_async as _extract_async,
    extract_pages_async as _extract_pages_async,
    infer_schema_async as _infer_schema_async,
    TEXT_WARN_THRESHOLD,
    API_PARSERS,
)
from server.settings import get_settings, get_provider
from server.parse_client import parse_fn as _remote_parse_fn, page_parse_fn as _remote_page_parse_fn

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"

# Re-export for backwards compatibility
_build_model = build_model


async def extract_text(
    pdf_path: str,
    *,
    ocr_fallback: bool = True,
    page_range: str | None = None,
    header_pages: int = 0,
) -> tuple[str, list[str]]:
    """Extract text from PDF with optional OCR fallback.

    Returns (text, info_messages).
    info_messages contains human-readable status like
    "No usable text layer, using OCR".
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
                pdf_path, i, "pymupdf", "none",
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
        text = await _remote_parse_fn(pdf_path, "pymupdf", "none")

    if ocr_fallback and len(text.strip()) < 200:
        info.append("No usable text layer detected, using OCR")
        from server.par_extract import _ocr_pdf
        text = _ocr_pdf(pdf_path, force=True)

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
    ocr_fallback: bool = False,
    ocr_backend: str = "none",
    text: str | None = None,
) -> BaseModel:
    """Extract using the model/key from the user's settings."""
    _set_ocr_env(uid)
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

    # API-based parsers (marker, etc.) are handled directly by petey —
    # only local parsers need the remote parse service.
    use_remote = parser not in API_PARSERS

    return await _extract_async(
        pdf_path, response_model,
        model=model_id, api_key=api_key,
        instructions=instructions,
        parser=parser,
        ocr_fallback=ocr_fallback,
        ocr_backend=ocr_backend,
        text=text,
        parse_fn=_remote_parse_fn if use_remote else None,
    )


def _set_ocr_env(uid: str):
    """Set OCR API key env vars from user settings."""
    import os
    settings = get_settings(uid)
    for key_name, env_var in [
        ("mistral_api_key", "MISTRAL_API_KEY"),
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
    _set_ocr_env(uid)
    model_id, api_key = _get_api_key(uid)
    settings = get_settings(uid)
    concurrency = settings.get("concurrency", 10)
    parse_multiplier = settings.get("parse_multiplier", 5)
    use_remote = parser not in API_PARSERS

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
        parse_multiplier=parse_multiplier,
        parse_fn=_remote_page_parse_fn if use_remote else None,
    )


async def async_infer_schema(
    pdf_path: str,
    uid: str,
    ocr_fallback: bool = False,
    max_pages: int = 2,
) -> dict:
    """Infer a schema from a sample PDF using the user's settings."""
    model_id, api_key = _get_api_key(uid)
    return await _infer_schema_async(
        pdf_path,
        model=model_id,
        api_key=api_key,
        ocr_fallback=ocr_fallback,
        max_pages=max_pages,
    )


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
