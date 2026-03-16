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
)
from server.settings import get_settings, get_provider

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"

# Re-export for backwards compatibility
_build_model = build_model


def extract_text(
    pdf_path: str,
    *,
    ocr_fallback: bool = True,
) -> tuple[str, list[str]]:
    """Extract text from PDF with optional OCR fallback.

    Returns (text, info_messages).
    info_messages contains human-readable status like
    "No usable text layer, using OCR".
    """
    info = []
    text = _raw_extract_text(pdf_path)

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
    text: str | None = None,
) -> BaseModel:
    """Extract using the model/key from the user's settings."""
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

    return await _extract_async(
        pdf_path, response_model,
        model=model_id, api_key=api_key,
        instructions=instructions,
        parser=parser,
        ocr_fallback=ocr_fallback,
        text=text,
    )


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
) -> list[dict]:
    """Page-chunked extraction using the user's settings."""
    model_id, api_key = _get_api_key(uid)
    return await _extract_pages_async(
        pdf_path, response_model,
        model=model_id, api_key=api_key,
        instructions=instructions,
        parser=parser,
        pages_per_chunk=pages_per_chunk,
        header_pages=header_pages,
        page_range=page_range,
        on_result=on_result,
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
