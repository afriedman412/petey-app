"""
Web server extraction layer. Thin wrapper around the petey package.
"""
from pathlib import Path

import yaml
from pydantic import BaseModel

from petey.schema import build_model, load_schema  # noqa: F401
from petey.extract import (
    extract_text,  # noqa: F401
    extract_async as _extract_async,
    TEXT_WARN_THRESHOLD,
)
from server.settings import get_settings, get_provider

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"

# Re-export for backwards compatibility
_build_model = build_model


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
