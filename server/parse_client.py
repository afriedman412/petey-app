"""
Client for the standalone parser service.

When PARSER_URL is set, routes requests to the remote parser service.
When PARSER_URL is empty or "local", parses in-process (desktop mode).

Provides async callables that match the parse_fn signature
expected by petey's extract functions:
  - parse_fn(pdf_path, parser) -> str
  - page_parse_fn(pdf_path, page_index, parser) -> str
"""
import os

import httpx


PARSER_URL = os.environ.get("PARSER_URL", "").strip()
_USE_LOCAL = not PARSER_URL or PARSER_URL == "local"

# ---------------------------------------------------------------------------
# Remote (sidecar) implementation
# ---------------------------------------------------------------------------

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            base_url=PARSER_URL,
            timeout=120.0,
        )
    return _client


async def _remote_parse(pdf_path: str, parser: str = "pymupdf") -> str:
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/parse",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
            data={"parser": parser},
        )
    resp.raise_for_status()
    return resp.json()["text"]


async def _remote_page_parse(
    pdf_path: str, page_index: int, parser: str = "pymupdf",
) -> str:
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/parse/page",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
            data={"page_index": page_index, "parser": parser},
        )
    resp.raise_for_status()
    return resp.json()["text"]


async def _remote_page_count(pdf_path: str) -> int:
    client = _get_client()
    with open(pdf_path, "rb") as f:
        resp = await client.post(
            "/page-count",
            files={"file": (os.path.basename(pdf_path), f, "application/pdf")},
        )
    resp.raise_for_status()
    return resp.json()["page_count"]


# ---------------------------------------------------------------------------
# Local (in-process) implementation
# ---------------------------------------------------------------------------

async def _local_parse(pdf_path: str, parser: str = "pymupdf") -> str:
    import asyncio
    pages = await asyncio.to_thread(_parse_pages_sync, pdf_path, parser)
    return "\n\n".join(pages)


async def _local_page_parse(
    pdf_path: str, page_index: int, parser: str = "pymupdf",
) -> str:
    import asyncio
    return await asyncio.to_thread(
        _parse_single_sync, pdf_path, page_index, parser,
    )


async def _local_page_count(pdf_path: str) -> int:
    import fitz
    doc = fitz.open(pdf_path)
    count = len(doc)
    doc.close()
    return count


def _parse_pages_sync(pdf_path: str, parser: str) -> list[str]:
    if parser == "pdfplumber":
        import pdfplumber
        pages = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text(layout=True)
                pages.append(text or page.extract_text() or "")
        return pages
    # Default: pymupdf
    import fitz
    import pymupdf4llm
    try:
        chunks = pymupdf4llm.to_markdown(
            pdf_path, page_chunks=True, force_text=False,
        )
        return [chunk["text"] for chunk in chunks]
    except Exception:
        doc = fitz.open(pdf_path)
        pages = [page.get_text("text") for page in doc]
        doc.close()
        return pages


def _parse_single_sync(
    pdf_path: str, page_index: int, parser: str,
) -> str:
    if parser == "pdfplumber":
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_index]
            return page.extract_text(layout=True) or page.extract_text() or ""
    # Default: pymupdf
    import fitz
    import pymupdf4llm
    try:
        chunks = pymupdf4llm.to_markdown(
            pdf_path, pages=[page_index],
            page_chunks=True, force_text=False,
        )
        return chunks[0]["text"] if chunks else ""
    except Exception:
        doc = fitz.open(pdf_path)
        text = doc[page_index].get_text("text")
        doc.close()
        return text


# ---------------------------------------------------------------------------
# Public API — routes to local or remote based on config
# ---------------------------------------------------------------------------

parse_fn = _local_parse if _USE_LOCAL else _remote_parse
page_parse_fn = _local_page_parse if _USE_LOCAL else _remote_page_parse
get_page_count = _local_page_count if _USE_LOCAL else _remote_page_count
