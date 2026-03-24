"""
Client for the standalone parser service.

Provides async callables that match the parse_fn signature
expected by petey's extract functions:
  - parse_fn(pdf_path, parser, ocr_backend) -> str
  - page_parse_fn(pdf_path, page_index, parser, ocr_backend) -> str
"""
import os

import httpx


PARSER_URL = os.environ.get("PARSER_URL", "http://localhost:8081")

# Reusable async client (connection pooling)
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            base_url=PARSER_URL,
            timeout=120.0,
        )
    return _client


async def parse_fn(
    pdf_path: str,
    parser: str = "pymupdf",
    ocr_backend: str = "none",
) -> str:
    """Parse all pages of a PDF via the parser service.

    Matches the parse_fn signature for extract_async / extract_batch.
    """
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/parse",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
            data={"parser": parser},
        )
    resp.raise_for_status()
    return resp.json()["text"]


async def page_parse_fn(
    pdf_path: str,
    page_index: int,
    parser: str = "pymupdf",
    ocr_backend: str = "none",
) -> str:
    """Parse a single page of a PDF via the parser service.

    Matches the parse_fn signature for extract_pages_async.
    """
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/parse/page",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
            data={"page_index": page_index, "parser": parser},
        )
    resp.raise_for_status()
    return resp.json()["text"]


async def get_page_count(pdf_path: str) -> int:
    """Get the page count of a PDF via the parser service."""
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/page-count",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
        )
    resp.raise_for_status()
    return resp.json()["page_count"]
