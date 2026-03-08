"""
Compare extraction results across models with bare vs engineered prompts.
Usage: python compare_models.py [--files N] [--bare]
"""
import json
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from openai import OpenAI
import anthropic

from par_rag_extract import (
    _build_extract_messages, extract_text, LLM_PARAMS,
)

COMPARE_FIELDS = [
    "adm_review_docket", "ra_docket", "petitioner", "petitioner_type",
    "other_party", "address", "apartment", "determination",
    "ra_determination", "par_filed_date", "ra_order_issued",
    "ra_case_filed", "issue_date",
]

BARE_PROMPT = """Extract the following fields from this document. Return ONLY a raw JSON object, no markdown, no explanation. Use null for missing values. Dates as YYYY-MM-DD.

Fields:
- petitioner: name of the petitioner
- petitioner_type: "Owner" or "Tenant"
- other_party: the opposing party's name, or null
- adm_review_docket: the administrative review / PAR docket number
- ra_docket: the Rent Administrator's docket number(s)
- address: street address including borough/city
- apartment: apartment number or null
- determination: the PAR outcome (one word: Denied, Granted, Remanded, etc.)
- ra_determination: what the RA originally decided (one word: Granted, Denied, Terminated, etc.)
- par_filed_date: date the PAR was filed
- ra_order_issued: date the RA order was issued
- ra_case_filed: date the original RA complaint was filed
- issue_date: date from the ISSUED stamp

Document:

"""


def _bare_messages(text):
    return [
        {"role": "system", "content": "Return ONLY a raw JSON object. No markdown fences, no explanation."},
        {"role": "user", "content": BARE_PROMPT + text},
    ]


def call_openai(text, messages_fn, model="gpt-4.1-mini"):
    client = OpenAI()
    response = client.chat.completions.create(
        model=model, messages=messages_fn(text), **LLM_PARAMS,
    )
    return json.loads(response.choices[0].message.content)


def call_anthropic(text, messages_fn, model="claude-sonnet-4-20250514"):
    client = anthropic.Anthropic()
    messages = messages_fn(text)
    system = messages[0]["content"]
    chat_messages = messages[1:]
    response = client.messages.create(
        model=model, max_tokens=1024,
        system=system, messages=chat_messages, temperature=0,
    )
    return json.loads(response.content[0].text)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=5)
    parser.add_argument("--bare", action="store_true",
                        help="Use bare prompt instead of engineered prompt")
    args = parser.parse_args()

    messages_fn = _bare_messages if args.bare else _build_extract_messages
    prompt_label = "BARE" if args.bare else "ENGINEERED"

    providers = {
        "gpt-4.1-mini": lambda text: call_openai(text, messages_fn),
        "claude-sonnet": lambda text: call_anthropic(text, messages_fn),
    }

    par_dir = Path(__file__).parent / "PAR_files"
    files = sorted(par_dir.glob("*.pdf"))[:args.files]

    print(f"Prompt: {prompt_label}")
    print(f"Comparing {len(files)} files across {len(providers)} models\n")

    all_results = []
    provider_names = list(providers.keys())

    for pdf_path in files:
        print(f"{'='*70}")
        print(f"FILE: {pdf_path.name}")
        print(f"{'='*70}")

        text = extract_text(str(pdf_path))
        file_results = {}

        for name, call_fn in providers.items():
            try:
                file_results[name] = call_fn(text)
                print(f"  {name}: OK")
            except Exception as e:
                file_results[name] = {"_error": str(e)}
                print(f"  {name}: ERROR - {e}")

        header = f"\n  {'Field':<22} " + " ".join(
            f"{p:<30}" for p in provider_names)
        print(header)
        print(f"  {'-' * (22 + 31 * len(provider_names))}")

        for field in COMPARE_FIELDS:
            vals = []
            for p in provider_names:
                v = file_results.get(p, {}).get(field)
                s = str(v) if v is not None else "null"
                if len(s) > 28:
                    s = s[:25] + "..."
                vals.append(s)

            unique = set(v for v in vals if v != "null")
            marker = " " if len(unique) <= 1 else "*"
            row = " ".join(f"{v:<30}" for v in vals)
            print(f" {marker}{field:<22} {row}")

        all_results.append({"file": pdf_path.name, "results": file_results})
        print()

    # Summary
    print(f"\n{'='*70}")
    print(f"AGREEMENT SUMMARY ({prompt_label} prompt)")
    print(f"{'='*70}")
    n = len(all_results)
    print(f"{'Field':<25} {'Agree':>6} {'Differ':>8} {'of':>4} {n}")
    print("-" * 50)

    for field in COMPARE_FIELDS:
        agree = differ = 0
        for entry in all_results:
            vals = []
            for p in provider_names:
                r = entry["results"].get(p, {})
                if "_error" in r:
                    continue
                v = r.get(field)
                vals.append(str(v).strip().lower() if v else "null")
            if len(vals) == 2:
                if vals[0] == vals[1]:
                    agree += 1
                else:
                    differ += 1
        print(f"{field:<25} {agree:>6} {differ:>8}")


if __name__ == "__main__":
    main()
