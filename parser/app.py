"""
Standalone PDF parsing service.

Accepts a PDF + page index, returns extracted text.
Deployed as its own Cloud Run service or run locally alongside
the main petey-web app.
"""
import io
import tempfile
from pathlib import Path

import fitz
import pymupdf4llm
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from pydantic import BaseModel


app = FastAPI(title="petey-parser")


class ParseResponse(BaseModel):
    text: str
    page_count: int


class ParsePageResponse(BaseModel):
    text: str
    page_index: int


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/parse", response_model=ParseResponse)
async def parse_pdf(
    file: UploadFile = File(...),
    parser: str = Form("pymupdf"),
):
    """Parse all pages of a PDF and return concatenated text."""
    pdf_bytes = await file.read()
    pages = _extract_pages(pdf_bytes, parser)
    return ParseResponse(
        text="\n\n".join(pages),
        page_count=len(pages),
    )


@app.post("/parse/page", response_model=ParsePageResponse)
async def parse_page(
    file: UploadFile = File(...),
    page_index: int = Form(...),
    parser: str = Form("pymupdf"),
):
    """Parse a single page of a PDF and return its text."""
    pdf_bytes = await file.read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total = len(doc)
    doc.close()

    if page_index < 0 or page_index >= total:
        raise HTTPException(
            status_code=400,
            detail=f"page_index {page_index} out of range "
                   f"(document has {total} pages)",
        )

    text = _extract_single_page(pdf_bytes, page_index, parser)
    return ParsePageResponse(text=text, page_index=page_index)


@app.post("/parse/pages", response_model=list[ParsePageResponse])
async def parse_pages(
    file: UploadFile = File(...),
    page_indices: str = Form(...),
    parser: str = Form("pymupdf"),
):
    """Parse multiple pages of a PDF. page_indices is comma-separated."""
    pdf_bytes = await file.read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total = len(doc)
    doc.close()

    indices = [int(i.strip()) for i in page_indices.split(",")]
    for idx in indices:
        if idx < 0 or idx >= total:
            raise HTTPException(
                status_code=400,
                detail=f"page_index {idx} out of range "
                       f"(document has {total} pages)",
            )

    results = []
    for idx in indices:
        text = _extract_single_page(pdf_bytes, idx, parser)
        results.append(ParsePageResponse(text=text, page_index=idx))
    return results


@app.post("/page-count")
async def page_count(file: UploadFile = File(...)):
    """Return the number of pages in a PDF."""
    pdf_bytes = await file.read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    count = len(doc)
    doc.close()
    return {"page_count": count}


# --- Internal helpers ---

def _extract_pages(pdf_bytes: bytes, parser: str) -> list[str]:
    """Extract text from all pages of a PDF."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(pdf_bytes)
        tmp_path = f.name
    try:
        if parser == "pymupdf":
            return _pymupdf_pages(tmp_path)
        elif parser == "tables":
            return _tables_pages(pdf_bytes)
        elif parser == "pdfplumber":
            return _pdfplumber_pages(tmp_path)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown parser: {parser}",
            )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _extract_single_page(
    pdf_bytes: bytes, page_index: int, parser: str,
) -> str:
    """Extract text from a single page."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(pdf_bytes)
        tmp_path = f.name
    try:
        if parser == "pymupdf":
            return _pymupdf_single(tmp_path, page_index)
        elif parser == "tables":
            return _tables_single(pdf_bytes, page_index)
        elif parser == "pdfplumber":
            return _pdfplumber_single(tmp_path, page_index)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown parser: {parser}",
            )
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _pymupdf_pages(pdf_path: str) -> list[str]:
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


def _pymupdf_single(pdf_path: str, page_index: int) -> str:
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


def _tables_pages(pdf_bytes: bytes) -> list[str]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []
    for page in doc:
        pages.append(_tables_page(page))
    doc.close()
    return pages


def _tables_single(pdf_bytes: bytes, page_index: int) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    text = _tables_page(doc[page_index])
    doc.close()
    return text


def _tables_page(page) -> str:
    found = page.find_tables()
    if found.tables:
        parts = []
        for table in found.tables:
            rows = table.extract()
            tsv = "\n".join(
                "\t".join(str(cell) if cell else "" for cell in row)
                for row in rows
                if any(cell for cell in row)
            )
            if tsv:
                parts.append(tsv)
        return "\n\n".join(parts) if parts else page.get_text("text")
    return page.get_text("text")


def _pdfplumber_pages(pdf_path: str) -> list[str]:
    import pdfplumber
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(layout=True)
            pages.append(text or page.extract_text() or "")
    return pages


def _pdfplumber_single(pdf_path: str, page_index: int) -> str:
    import pdfplumber
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_index]
        return page.extract_text(layout=True) or page.extract_text() or ""
