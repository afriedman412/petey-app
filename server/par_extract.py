"""
Bespoke PAR Decision Extractor.

Uses few-shot prompting + validation + re-query for accurate extraction
from DHCR PAR (Petition for Administrative Review) decision PDFs.

Adapted from petey/par_rag_extract.py for the web app.
"""
import re
import json
import asyncio
import tempfile
from datetime import datetime
from pathlib import Path

import fitz


def extract_text(pdf_path: str) -> tuple[str, bool]:
    """Extract text from PDF, with OCR fallback for scanned pages.

    Returns (cleaned_text, used_ocr).
    """
    doc = fitz.open(pdf_path)
    pages = [page.get_text("text") for page in doc]
    raw = "\n\n".join(pages)

    # Check if extraction actually got the document content.
    # PyMuPDF sometimes extracts scattered fragments from scanned PDFs
    # that pass a simple length check but miss all the real content.
    needs_ocr = (
        len(raw.strip()) < 200
        or not any(marker in raw.upper() for marker in [
            "ADMINISTRATIVE REVIEW",
            "RENT ADMINISTRATOR",
            "PETITION",
        ])
    )

    if needs_ocr:
        raw = _ocr_pdf(pdf_path, force=True)

    return _clean_text(raw), needs_ocr


def _clean_text(text: str) -> str:
    """Remove NYSCEF filing headers and other noise that confuses the LLM."""
    lines = text.split('\n')
    cleaned = []
    skip_nyscef = False
    for line in lines:
        stripped = line.strip()
        # Skip NYSCEF electronic filing headers
        if 'NYSCEF' in stripped or 'FILED:' in stripped and 'NYSCEF' in stripped:
            skip_nyscef = True
            continue
        if skip_nyscef:
            # Skip blank lines immediately after NYSCEF header
            if not stripped:
                continue
            skip_nyscef = False
        # Skip page numbers and form footers
        if re.match(r'^-?\s*\d+\s*-?$', stripped):
            continue
        if stripped.startswith('INDEX NO.') or stripped.startswith('RECEIVED NYSCEF'):
            continue
        cleaned.append(line)

    # Collapse excessive blank lines
    result = re.sub(r'\n{4,}', '\n\n\n', '\n'.join(cleaned))
    return result.strip()


def _ocr_pdf(pdf_path: str, force: bool = False) -> str:
    """Run ocrmypdf to add text layer, then extract.

    force=True discards any existing text layer and re-OCRs
    (needed when the existing layer is garbage fragments).
    """
    import logging
    import ocrmypdf

    # Suppress noisy ocrmypdf warnings (file size, diacritics)
    logging.getLogger("ocrmypdf").setLevel(logging.ERROR)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        out_path = tmp.name
    try:
        ocrmypdf.ocr(
            pdf_path, out_path,
            force_ocr=force,
            skip_text=not force,
            deskew=True,
            optimize=0,
            progress_bar=False,
            tesseract_timeout=120,
        )
        doc = fitz.open(out_path)
        return "\n\n".join(page.get_text("text") for page in doc)
    finally:
        Path(out_path).unlink(missing_ok=True)

SYSTEM_PROMPT = """You extract structured data from DHCR PAR (Petition for Administrative Review) decisions. Return ONLY a JSON object.

CRITICAL: Read the ENTIRE document carefully before extracting. Every field has a specific location in the document — do not leave fields null unless the information is truly absent.

DOCUMENT STRUCTURE:

1. HEADER (top of first page — look for these labels):
   - "IN THE MATTER OF THE ADMINISTRATIVE APPEAL OF" → petitioner name follows
   - "ADMINISTRATIVE REVIEW DOCKET NO." → adm_review_docket (this is the PAR docket)
   - "RENT ADMINISTRATOR'S DOCKET NO." → ra_docket (DIFFERENT number from PAR docket!)
   - "OWNER:" and/or "TENANT:" labels → identify petitioner_type and other_party

2. FIRST PARAGRAPH (right after the title like "ORDER AND OPINION..."):
   - "On [DATE], the above-named petitioner... filed a Petition" → par_filed_date
   - "an order issued on [DATE]" or "order... issued on [DATE]" → ra_order_issued
   - Address and apartment appear here (e.g., "housing accommodations known as [ADDRESS]")
   - What the RA decided appears here → ra_determination
   - Sometimes: "This proceeding was commenced on [DATE]" → ra_case_filed

3. FINAL SECTION:
   - "THEREFORE... ORDERED, that this petition be... [denied/granted/etc.]" → determination
   - "ISSUED:" stamp with date → issue_date

FIELDS TO EXTRACT:
{
  "petitioner": "Person or company name from caption. null only if a generic label like 'PETITIONER X'.",
  "petitioner_type": "Owner or Tenant — based on the caption labels",
  "other_party": "The opposing party's name, or null if not named",
  "adm_review_docket": "PAR docket from header (labeled 'ADMINISTRATIVE REVIEW DOCKET NO.')",
  "ra_docket": "RA docket from header (labeled 'RENT ADMINISTRATOR'S DOCKET NO.') — this is a DIFFERENT number. If multiple, comma-separated.",
  "address": "Full street address with borough/city from the first paragraph",
  "apartment": "Apartment number from first paragraph, 'Various' for building-wide, or null",
  "determination": "PAR outcome from THEREFORE clause: Denied, Granted, Granted in Part, Dismissed, Revoked, Modified, Rescinded, Remanded, or Terminated",
  "ra_determination": "What the RA originally decided (from first paragraph): Granted, Denied, Granted in Part, Terminated, or null",
  "par_filed_date": "YYYY-MM-DD when PAR was filed (from first paragraph)",
  "ra_order_issued": "YYYY-MM-DD when RA order was issued (from first paragraph)",
  "ra_case_filed": "YYYY-MM-DD when original complaint was filed, or null",
  "issue_date": "YYYY-MM-DD from 'ISSUED:' stamp. OCR may split digits: 'NOV 1 2 2013' = Nov 12. null if unreadable."
}

RULES:
- The ra_docket and adm_review_docket are ALWAYS different numbers. If you find only one docket, look harder.
- Use null only when information is truly absent — never "Unknown", "N/A", or empty string.
- Dates in the text may appear as "June 21, 2013" or "Jun 21 2013" — always convert to YYYY-MM-DD.
- ra_determination: "denied the complaint" / "no overcharge" = Denied. "granted a rent reduction" / "directed restoration of services" = Granted.
- Return ONLY the JSON object."""

FEW_SHOT_EXAMPLES = [
    {
        "role": "user",
        "content": """Extract fields from this PAR decision:

STATE OF NEW YORK DIVISION OF HOUSING AND COMMUNITY RENEWAL OFFICE OF RENT ADMINISTRATION GERTZ PLAZA 92-31 UNION HALL STREET JAMAICA, NEW YORK 11433

IN THE MATTER OF THE ADMINISTRATIVE APPEAL OF
DOMINICK VALENTINO, PETITIONER

ADMINISTRATIVE REVIEW DOCKET NO.: ZG410017RT
RENT ADMINISTRATOR'S DOCKET NO.: YD410048R
OWNER: BCRE WEST 72 LLC and STELLAR 85, LLC
TENANT OF RECORD: DAVID VALENTINO

ORDER AND OPINION DENYING PETITION FOR ADMINISTRATIVE REVIEW

On April 9, 2010, the above-named petitioner-tenant filed a rent overcharge complaint concerning the housing accommodations known as Room 1609 in the Hotel Olcott located at 27 West 72nd Street in Manhattan.

On June 10, 2011, the Rent Administrator issued an Order Denying Application or Terminating Proceeding finding that, "...the tenant paid the same rental amount of $1100.00 from the base date through the present. Therefore, no overcharge is found."

On July 7, 2011, said petitioner-tenant timely filed a Petition for Administrative Review (PAR) against the above-referenced Rent Administrator's order.

[...body text about four-year rule, Thornton v. Baron, Grimm v. DHCR...]

THEREFORE, pursuant to the applicable statutes and regulations, it is ORDERED, that this PAR be, and the same hereby is, denied and that the Rent Administrator's order be, and the same hereby is, affirmed.

ISSUED: NOV 1 2 2013"""
    },
    {
        "role": "assistant",
        "content": json.dumps({
            "petitioner": "Dominick Valentino",
            "petitioner_type": "Tenant",
            "other_party": "BCRE West 72 LLC and Stellar 85, LLC",
            "adm_review_docket": "ZG410017RT",
            "ra_docket": "YD410048R",
            "address": "27 West 72nd Street, Manhattan",
            "apartment": "1609",
            "determination": "Denied",
            "ra_determination": "Denied",
            "par_filed_date": "2011-07-07",
            "ra_order_issued": "2011-06-10",
            "ra_case_filed": "2010-04-09",
            "issue_date": "2013-11-12"
        }, indent=2)
    },
    {
        "role": "user",
        "content": """Extract fields from this PAR decision:

STATE OF NEW YORK DIVISION OF HOUSING AND COMMUNITY RENEWAL OFFICE OF RENT ADMINISTRATION GERTZ PLAZA 92-31 UNION HALL STREET JAMAICA, NEW YORK 11433

IN THE MATTER OF THE ADMINISTRATIVE APPEAL OF
ERIC WARNER, PETITIONER

SJR NO. 14026
ADMINISTRATIVE REVIEW DOCKET NO. ZE420003RK
RENT ADMINISTRATOR'S DOCKET NO. WD420001LD
OWNER: MAYFLOWER DEVELOPMENT CORP.

ORDER AND OPINION DENYING FURTHER RECONSIDERATION ON PETITION FOR ADMINISTRATIVE REVIEW

On June 4, 2008, the above-named tenant filed a petition for administrative review (PAR) of an order issued on May 29, 2008 by a Rent Administrator concerning the housing accommodations known as 425 Riverside Drive, Apartment 161, New York, New York, wherein the Rent Administrator determined that the subject apartment was to be deregulated effective June 1, 2008, pursuant to Section 2200.2(f)(20) of the New York City Rent and Eviction Regulations.

[...body text about luxury decontrol, J-51 benefits, Roberts v. Tishman Speyer...]

THEREFORE, pursuant to the provisions of the applicable statutes and regulations, it is ORDERED, that this petition for administrative review be granted and that the Rent Administrator's order be modified in accordance with this order and opinion and affirmed in all other respects.

ISSUED: DEC 0 7 2011"""
    },
    {
        "role": "assistant",
        "content": json.dumps({
            "petitioner": "Eric Warner",
            "petitioner_type": "Tenant",
            "other_party": "Mayflower Development Corp.",
            "adm_review_docket": "ZE420003RK",
            "ra_docket": "WD420001LD",
            "address": "425 Riverside Drive, New York, New York",
            "apartment": "161",
            "determination": "Granted",
            "ra_determination": "Granted",
            "par_filed_date": "2008-06-04",
            "ra_order_issued": "2008-05-29",
            "ra_case_filed": None,
            "issue_date": "2011-12-07"
        }, indent=2)
    },
    {
        "role": "user",
        "content": """Extract fields from this PAR decision:

STATE OF NEW YORK DIVISION OF HOUSING AND COMMUNITY RENEWAL OFFICE OF RENT ADMINISTRATION GERTZ PLAZA 92-31 UNION HALL STREET JAMAICA, NEW YORK 11433

IN THE MATTER OF THE ADMINISTRATIVE APPEAL OF

PETITIONER X

ADMINISTRATIVE REVIEW DOCKET NO.: YH610037RO
RENT ADMINISTRATOR'S DOCKET NO.: YD610025S

ORDER AND OPINION DENYING PETITION FOR ADMINISTRATIVE REVIEW

On August 21, 2010, the above-named petitioner-owner filed a Petition for Administrative Review (PAR) of an order the Rent Administrator issued on July 20, 2010 concerning the housing accommodations known as 1639 Nelson Avenue, Apt 3F, Bronx, NY, wherein the Administrator, based upon the entire record, directed the restoration of services and granted a rent reduction.

[...body text about owner's claims regarding rent regulation status...]

THEREFORE, in accordance with the relevant Rent Regulatory Laws and Regulations, it is ORDERED, that this petition be, and the same hereby is, denied, and that the Rent Administrator's order be, and the same hereby is, affirmed.

ISSUED: JAN 0 5 2011"""
    },
    {
        "role": "assistant",
        "content": json.dumps({
            "petitioner": None,
            "petitioner_type": "Owner",
            "other_party": None,
            "adm_review_docket": "YH610037RO",
            "ra_docket": "YD610025S",
            "address": "1639 Nelson Avenue, Bronx, NY",
            "apartment": "3F",
            "determination": "Denied",
            "ra_determination": "Granted",
            "par_filed_date": "2010-08-21",
            "ra_order_issued": "2010-07-20",
            "ra_case_filed": None,
            "issue_date": "2011-01-05"
        }, indent=2)
    }
]

ANTHROPIC_MODELS = {
    "claude-haiku-4-5-20251001",
    "claude-sonnet-4-20250514",
}


def _is_anthropic(model: str) -> bool:
    return model.startswith("claude-") or model in ANTHROPIC_MODELS


def _make_client(model: str, api_key: str):
    """Create a reusable API client for the given provider."""
    if _is_anthropic(model):
        from anthropic import AsyncAnthropic
        return AsyncAnthropic(api_key=api_key)
    else:
        from openai import AsyncOpenAI
        return AsyncOpenAI(api_key=api_key)


async def _llm_call(
    messages: list[dict],
    *,
    model: str,
    api_key: str,
    client=None,
) -> str:
    """Call OpenAI or Anthropic, return response text.

    Pass a pre-built client to reuse connections across calls.
    Anthropic calls use prompt caching on the system prompt
    and few-shot examples (cache_control on last few-shot msg).
    """
    if client is None:
        client = _make_client(model, api_key)

    if _is_anthropic(model):
        # Separate system from conversation and add cache
        system = []
        conv = []
        for m in messages:
            if m["role"] == "system":
                system.append({
                    "type": "text",
                    "text": m["content"],
                    "cache_control": {"type": "ephemeral"},
                })
            else:
                conv.append(m)
        # Mark last few-shot message for caching (the
        # boundary between static examples and new input)
        for i in range(len(conv) - 2, -1, -1):
            if conv[i]["role"] == "assistant":
                conv[i] = {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "text",
                            "text": conv[i]["content"],
                            "cache_control": {
                                "type": "ephemeral",
                            },
                        }
                    ],
                }
                break
        resp = await client.messages.create(
            model=model,
            max_tokens=2048,
            temperature=0,
            system=system,
            messages=conv,
        )
        return resp.content[0].text
    else:
        # OpenAI: automatic prompt caching for gpt-4.1
        resp = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

DOCKET_RE = re.compile(r'^[A-Z]{2}\d{6}[A-Z]{1,3}$')
PAR_SUFFIX_RE = re.compile(r'^[A-Z]{2}\d{6}(RO|RT|RK|RP)$')
DOCKET_SUFFIX_RE = re.compile(r'^[A-Z]{2}\d{6}([A-Z]+)$')

VALID_DETERMINATIONS = {
    'Denied', 'Granted', 'Granted in Part', 'Dismissed',
    'Revoked', 'Modified', 'Rescinded', 'Remanded', 'Terminated',
}

VALID_RA_DETERMINATIONS = {
    'Granted', 'Denied', 'Granted in Part', 'Terminated', None,
}


def derive_case_type(ra_docket: str | None) -> str | None:
    if not ra_docket:
        return None
    first = ra_docket.split(',')[0].strip()
    m = DOCKET_SUFFIX_RE.match(first)
    return m.group(1) if m else None


def validate_docket(docket: str | None, is_par: bool = False) -> str | None:
    if docket is None:
        return "docket is null"
    if is_par:
        if not PAR_SUFFIX_RE.match(docket):
            return (
                f"'{docket}' doesn't match PAR docket format "
                f"(expected: 2 letters + 6 digits + RO/RT/RK/RP)"
            )
    else:
        if not DOCKET_RE.match(docket):
            return (
                f"'{docket}' doesn't match docket format "
                f"(expected: 2 letters + 6 digits + 1-3 letter suffix)"
            )
        if PAR_SUFFIX_RE.match(docket):
            return (
                f"'{docket}' ends in RO/RT/RK/RP — that's a PAR docket, "
                f"not an RA docket. Find the Rent Administrator's docket instead."
            )
    return None


def validate_date(date_str: str | None, field_name: str) -> str | None:
    if date_str is None:
        return None
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return None
    except ValueError:
        return f"'{date_str}' for {field_name} is not valid YYYY-MM-DD"


def docket_year(docket: str | None) -> int | None:
    if not docket or len(docket) < 2:
        return None
    year_code = docket[0].upper()
    month_code = docket[1].upper()
    if not year_code.isalpha() or not month_code.isalpha():
        return None
    base_year = ord(year_code) - ord('A')
    if month_code >= 'M':
        return 2012 + base_year
    else:
        return 1986 + base_year


def validate_result(result: dict) -> list[dict]:
    errors = []

    err = validate_docket(result.get('adm_review_docket'), is_par=True)
    if err:
        errors.append({'field': 'adm_review_docket', 'value': result.get('adm_review_docket'), 'error': err})

    ra_docket_val = result.get('ra_docket')
    if ra_docket_val:
        for d in ra_docket_val.split(','):
            d = d.strip()
            if d:
                err = validate_docket(d, is_par=False)
                if err:
                    errors.append({'field': 'ra_docket', 'value': ra_docket_val, 'error': err})
                    break
    else:
        errors.append({'field': 'ra_docket', 'value': ra_docket_val, 'error': 'docket is null'})

    det = result.get('determination')
    if det and det not in VALID_DETERMINATIONS:
        errors.append({'field': 'determination', 'value': det, 'error': f"'{det}' is not a valid determination"})

    ra_det = result.get('ra_determination')
    if ra_det not in VALID_RA_DETERMINATIONS:
        errors.append({'field': 'ra_determination', 'value': ra_det, 'error': f"'{ra_det}' is not a valid RA determination"})

    for date_field in ['par_filed_date', 'ra_order_issued', 'ra_case_filed', 'issue_date']:
        err = validate_date(result.get(date_field), date_field)
        if err:
            errors.append({'field': date_field, 'value': result.get(date_field), 'error': err})

    issue_date = result.get('issue_date')
    if issue_date:
        try:
            year = int(issue_date[:4])
            if year < 2005 or year > 2026:
                errors.append({'field': 'issue_date', 'value': issue_date,
                               'error': f"year {year} is outside plausible range (2005-2026)"})
        except (ValueError, TypeError):
            pass

    par_filed = result.get('par_filed_date')
    if issue_date and par_filed:
        try:
            issue_dt = datetime.strptime(issue_date, '%Y-%m-%d')
            par_dt = datetime.strptime(par_filed, '%Y-%m-%d')
            if issue_dt < par_dt:
                errors.append({'field': 'issue_date', 'value': issue_date,
                               'error': f"issue_date {issue_date} is before par_filed_date {par_filed}"})
        except ValueError:
            pass

    par_docket = result.get('adm_review_docket')
    par_yr = docket_year(par_docket)
    if issue_date and par_yr:
        try:
            issue_yr = int(issue_date[:4])
            if issue_yr < par_yr or issue_yr > par_yr + 7:
                errors.append({'field': 'issue_date', 'value': issue_date,
                               'error': f"issue_date year {issue_yr} is implausible for "
                               f"PAR docket {par_docket} (filed ~{par_yr}, "
                               f"expected {par_yr}-{par_yr+7}). "
                               f"OCR may have garbled the year — if unreadable, return null."})
        except (ValueError, TypeError):
            pass

    return errors


# ---------------------------------------------------------------------------
# Extraction pipeline
# ---------------------------------------------------------------------------

def _build_extract_messages(text: str) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        *FEW_SHOT_EXAMPLES,
        {"role": "user", "content": f"Extract fields from this PAR decision:\n\n{text}"},
    ]


def _build_requery_messages(text: str, errors: list[dict]) -> list[dict]:
    error_descriptions = [
        f"- {e['field']}: you returned {json.dumps(e['value'])}. Problem: {e['error']}"
        for e in errors
    ]
    correction_prompt = f"""Some extracted fields failed validation. Here are the problems:

{chr(10).join(error_descriptions)}

Hints:
- Docket format: 2 letters + 6 digits + 1-3 letter suffix. OCR can cause digit/letter confusion (0↔O, 1↔I, 5↔S, 8↔B). Only fix the original value — do not substitute a different docket from the document.
- The RA docket is labeled "RENT ADMINISTRATOR'S DOCKET NO." in the header. It is NOT the same as the PAR/Administrative Review docket.
- If a date's year seems implausible given other dates in the document, return null rather than guessing.

Re-read the document and return corrected values as JSON (only the fields that need correction):

{text}"""
    return [
        {"role": "system", "content": "You are correcting specific extracted fields from a DHCR PAR decision. Return ONLY a JSON object with the corrected field values."},
        {"role": "user", "content": correction_prompt},
    ]


async def async_process_file(
    pdf_path: str,
    *,
    model: str = "gpt-4.1",
    api_key: str | None = None,
    on_progress=None,
) -> dict:
    """Extract structured PAR data with OCR, validation, and re-query.

    on_progress: optional async callable(step: str) called at each stage.
    """
    async def _emit(step: str):
        if on_progress:
            await on_progress(step)

    # Step 1: text extraction (with smart OCR fallback)
    await _emit("OCR")
    text, used_ocr = await asyncio.to_thread(extract_text, pdf_path)

    # Reuse one client for all LLM calls (connection pooling
    # + enables Anthropic prompt caching across calls)
    client = _make_client(model, api_key)
    llm_kw = dict(model=model, api_key=api_key, client=client)

    async def _extract(txt: str) -> dict:
        raw = await _llm_call(
            _build_extract_messages(txt), **llm_kw,
        )
        res = json.loads(raw)
        for k in list(res.keys()):
            if isinstance(res[k], list):
                res[k] = ", ".join(str(v) for v in res[k])
        return res

    # Step 2: LLM extraction
    await _emit("Extracting")
    result = await _extract(text)

    # Step 2b: If most key fields are null, text was probably bad.
    # Force OCR and retry.
    key_fields = [
        'petitioner', 'adm_review_docket',
        'ra_docket', 'address', 'determination',
    ]
    null_count = sum(
        1 for f in key_fields if not result.get(f)
    )
    if null_count >= 3 and not used_ocr:
        await _emit("OCR (retry)")
        ocr_text = await asyncio.to_thread(
            _ocr_pdf, pdf_path, force=True,
        )
        ocr_text = _clean_text(ocr_text)
        if len(ocr_text) > len(text):
            text = ocr_text
            await _emit("Re-extracting")
            result = await _extract(text)

    # Step 3: Validate and re-query if needed
    errors = validate_result(result)
    if errors:
        await _emit("Validating")
        error_fields = [e['field'] for e in errors]
        raw = await _llm_call(
            _build_requery_messages(text, errors), **llm_kw,
        )
        corrections = json.loads(raw)
        for field, value in corrections.items():
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            if field in error_fields:
                result[field] = value

        remaining = validate_result(result)
        if remaining:
            result['_validation_warnings'] = [
                f"{e['field']}: {e['error']}" for e in remaining
            ]

    # Add derived fields
    result['ra_case_type'] = derive_case_type(result.get('ra_docket'))
    result['_source_file'] = Path(pdf_path).name
    result['_text_length'] = len(text)

    await _emit("Done")
    return result
