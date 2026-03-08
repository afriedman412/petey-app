"""
Core PDF extraction. No web dependencies.
Configure via environment variables or pass explicitly.
"""
import asyncio
import os

import fitz
import instructor
from pydantic import BaseModel
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic

SYSTEM = "You extract structured data from documents. Use null for missing or unreadable values."

TEXT_WARN_THRESHOLD = 50_000


def extract_text(pdf_path: str) -> str:
    doc = fitz.open(pdf_path)
    return "\n\n".join(page.get_text("text") for page in doc)


def _get_provider(model: str) -> str:
    return "anthropic" if model.startswith("claude") else "openai"


def _make_client(model: str, api_key: str | None = None):
    """Build an instructor-wrapped async client for the given model."""
    provider = _get_provider(model)
    if provider == "anthropic":
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        return instructor.from_anthropic(AsyncAnthropic(api_key=key))
    else:
        key = api_key or os.environ.get("OPENAI_API_KEY")
        return instructor.from_openai(AsyncOpenAI(api_key=key))


def _make_messages(text: str, instructions: str = "") -> list[dict]:
    system = SYSTEM
    if instructions:
        system += "\n\nAdditional instructions:\n" + instructions
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": f"Extract fields from this document:\n\n{text}"},
    ]


async def extract_async(
    pdf_path: str,
    response_model: type[BaseModel],
    *,
    model: str = "gpt-4.1-mini",
    api_key: str | None = None,
    instructions: str = "",
) -> BaseModel:
    """Extract structured data from a PDF."""
    text = extract_text(pdf_path)
    client = _make_client(model, api_key)
    return await client.chat.completions.create(
        model=model,
        response_model=response_model,
        max_retries=2,
        messages=_make_messages(text, instructions),
        temperature=0,
    )


def extract(
    pdf_path: str,
    response_model: type[BaseModel],
    *,
    model: str = "gpt-4.1-mini",
    api_key: str | None = None,
    instructions: str = "",
) -> BaseModel:
    """Sync wrapper around extract_async."""
    return asyncio.run(
        extract_async(
            pdf_path, response_model,
            model=model, api_key=api_key, instructions=instructions,
        )
    )


async def extract_batch(
    pdf_paths: list[str],
    response_model: type[BaseModel],
    *,
    model: str = "gpt-4.1-mini",
    api_key: str | None = None,
    instructions: str = "",
    concurrency: int = 10,
    on_result=None,
) -> list[dict]:
    """
    Extract from multiple PDFs concurrently.

    Args:
        on_result: Optional callback(path, data_dict) called as each file completes.
        concurrency: Max concurrent API calls.

    Returns list of result dicts (with _source_file and optionally _error).
    """
    sem = asyncio.Semaphore(concurrency)
    client = _make_client(model, api_key)
    results = []

    async def _process(path: str):
        async with sem:
            try:
                text = extract_text(path)
                result = await client.chat.completions.create(
                    model=model,
                    response_model=response_model,
                    max_retries=2,
                    messages=_make_messages(text, instructions),
                    temperature=0,
                )
                data = result.model_dump()
                data["_source_file"] = os.path.basename(path)
            except Exception as e:
                data = {"_source_file": os.path.basename(path), "_error": str(e)}
            results.append(data)
            if on_result:
                on_result(path, data)

    await asyncio.gather(*[_process(p) for p in pdf_paths])
    return results
