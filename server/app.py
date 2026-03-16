"""
Web interface for PDF field extraction.
"""
import asyncio
import json
import tempfile
from datetime import datetime
from pathlib import Path

import yaml
from fastapi import FastAPI, UploadFile, Form, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from server.auth import FirebaseAuthMiddleware, get_uid
from server.extract import (
    async_extract, async_extract_pages, load_schema,
    list_schemas, SCHEMAS_DIR, _build_model,
    extract_text, check_text_length,
)
from server.par_extract import async_process_file as par_process_file, extract_text as par_extract_text
from server.settings import (
    get_settings, update_settings, mask_key, get_provider, MODELS,
)
from server.validate_keys import (
    validate_openai_key, validate_anthropic_key,
)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"

app = FastAPI()
app.add_middleware(FirebaseAuthMiddleware)

_schema_cache: dict[str, type] = {}


def get_model(schema_file: str):
    if schema_file not in _schema_cache:
        model, _ = load_schema(SCHEMAS_DIR / schema_file)
        _schema_cache[schema_file] = model
    return _schema_cache[schema_file]


def _load_template(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text()


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/schemas")
async def schemas():
    return list_schemas()


@app.get("/schemas/{schema_file}")
async def get_schema(schema_file: str):
    # Sanitize filename to prevent path traversal
    safe_name = Path(schema_file).name
    path = SCHEMAS_DIR / safe_name
    if not path.exists():
        return JSONResponse({"error": "not found"}, 404)
    with open(path) as f:
        return yaml.safe_load(f)


@app.post("/schemas")
async def save_schema(request: Request):
    spec = await request.json()
    filename = (
        spec.get("name", "schema").lower().replace(" ", "_") + ".yaml"
    )
    # Sanitize filename
    filename = Path(filename).name
    path = SCHEMAS_DIR / filename
    with open(path, "w") as f:
        yaml.dump(spec, f, default_flow_style=False, sort_keys=False)
    _schema_cache.pop(filename, None)
    return {"file": filename, "name": spec.get("name")}


@app.post("/extract")
async def extract_endpoint(
    request: Request,
    file: UploadFile,
    schema_file: str = Form(None),
    schema_spec: str = Form(None),
    instructions: str = Form(""),
    parser: str = Form("pymupdf"),
    ocr_fallback: bool = Form(False),
    uid: str = Depends(get_uid),
):
    # Check that the user has the required API key before processing
    settings = get_settings(uid)
    provider = get_provider(settings["model"])
    if provider == "anthropic" and not settings.get("anthropic_api_key"):
        return JSONResponse(
            {"error": "No Anthropic API key configured. Add one in Settings before extracting."},
            status_code=400,
        )
    if provider == "openai" and not settings.get("openai_api_key"):
        return JSONResponse(
            {"error": "No OpenAI API key configured. Add one in Settings before extracting."},
            status_code=400,
        )

    if schema_spec:
        spec = json.loads(schema_spec)
        response_model = _build_model(spec)
    elif schema_file:
        response_model = get_model(schema_file)
        with open(SCHEMAS_DIR / schema_file) as f:
            spec = yaml.safe_load(f)
    else:
        return JSONResponse({"error": "No schema provided"}, 400)

    with tempfile.NamedTemporaryFile(
        suffix=".pdf", delete=False
    ) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        is_array = spec.get("record_type") == "array"
        if is_array:
            records = await async_extract_pages(
                tmp_path, response_model,
                uid=uid, instructions=instructions,
                parser=spec.get("parser", "tables"),
                header_pages=spec.get("header_pages", 0),
                page_range=spec.get("pages") or None,
            )
            rows = []
            for chunk in records:
                if not chunk.get("_error") and "items" in chunk:
                    rows.extend(chunk["items"])
            data = {"_source_file": file.filename, "records": rows}
        else:
            text, info = extract_text(tmp_path)
            warning = check_text_length(text)
            result = await async_extract(
                tmp_path, response_model,
                uid=uid, instructions=instructions,
                parser=parser, ocr_fallback=ocr_fallback,
                text=text if info else None,
            )
            data = result.model_dump()
            data["_source_file"] = file.filename
            if warning:
                data["_warning"] = warning
            if info:
                data["_info"] = info
    except Exception as e:
        data = {
            "_source_file": file.filename,
            "_error": str(e),
        }
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    return data


@app.post("/extract/stream")
async def extract_stream_endpoint(
    request: Request,
    file: UploadFile,
    schema_spec: str = Form(None),
    instructions: str = Form(""),
    uid: str = Depends(get_uid),
):
    """Streaming array extraction — emits NDJSON page progress then result."""
    settings = get_settings(uid)
    provider = get_provider(settings["model"])
    if provider == "anthropic" and not settings.get("anthropic_api_key"):
        return JSONResponse({"error": "No Anthropic API key configured."}, 400)
    if provider == "openai" and not settings.get("openai_api_key"):
        return JSONResponse({"error": "No OpenAI API key configured."}, 400)

    if not schema_spec:
        return JSONResponse({"error": "No schema provided"}, 400)
    spec = json.loads(schema_spec)
    response_model = _build_model(spec)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    filename = file.filename

    async def _stream():
        queue = asyncio.Queue()

        def on_result(label, data):
            queue.put_nowait(json.dumps({"type": "progress", "page": label}) + "\n")

        async def run():
            try:
                records = await async_extract_pages(
                    tmp_path, response_model,
                    uid=uid, instructions=instructions,
                    parser=spec.get("parser", "tables"),
                    header_pages=spec.get("header_pages", 0),
                    page_range=spec.get("pages") or None,
                    on_result=on_result,
                )
                rows = []
                for chunk in records:
                    if not chunk.get("_error") and "items" in chunk:
                        rows.extend(chunk["items"])
                result = {"_source_file": filename, "records": rows}
            except Exception as e:
                result = {"_source_file": filename, "_error": str(e)}
            finally:
                Path(tmp_path).unlink(missing_ok=True)
            queue.put_nowait(json.dumps({"type": "result", "data": result}) + "\n")

        task = asyncio.create_task(run())
        done = False
        while not done:
            msg = await queue.get()
            yield msg
            if json.loads(msg)["type"] == "result":
                done = True
        await task

    return StreamingResponse(_stream(), media_type="application/x-ndjson")


@app.post("/par/extract")
async def par_extract_endpoint(
    request: Request,
    files: list[UploadFile],
    uid: str = Depends(get_uid),
):
    """Bespoke PAR decision extractor with streaming progress.
    Streams newline-delimited JSON: progress events then results."""
    settings = get_settings(uid)
    provider = get_provider(settings["model"])
    if provider == "anthropic":
        api_key = settings.get("anthropic_api_key")
        model_id = settings["model"]
    else:
        api_key = settings.get("openai_api_key")
        # PAR extractor needs gpt-4.1 (mini is too weak)
        model_id = "gpt-4.1"
    if not api_key:
        return JSONResponse(
            {"error": "No API key configured. Add one in Settings."},
            status_code=400,
        )

    pdf_files = [
        f for f in files
        if f.filename and f.filename.lower().endswith(".pdf")
    ]
    if not pdf_files:
        return JSONResponse({"error": "No PDF files found."}, 400)

    # Save all uploads to temp files upfront (can't read UploadFile
    # inside the streaming generator after the request body is consumed)
    temp_paths = []
    for f in pdf_files:
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(await f.read())
        tmp.close()
        temp_paths.append((f.filename, tmp.name))

    total = len(temp_paths)

    async def _stream():
        queue = asyncio.Queue()
        sem = asyncio.Semaphore(5)

        async def _process(idx, filename, tmp_path):
            async def on_progress(step):
                await queue.put(json.dumps({
                    "type": "progress",
                    "file": filename,
                    "fileIndex": idx,
                    "totalFiles": total,
                    "step": step,
                }) + "\n")

            async with sem:
                try:
                    data = await par_process_file(
                        tmp_path, model=model_id,
                        api_key=api_key, on_progress=on_progress,
                    )
                    data["_source_file"] = filename
                except Exception as e:
                    data = {
                        "_source_file": filename, "_error": str(e),
                    }
                finally:
                    Path(tmp_path).unlink(missing_ok=True)

                await queue.put(json.dumps({
                    "type": "result", "data": data,
                }) + "\n")

        tasks = [
            asyncio.create_task(_process(i, fn, tp))
            for i, (fn, tp) in enumerate(temp_paths)
        ]

        done_count = 0
        while done_count < total:
            msg = await queue.get()
            yield msg
            parsed = json.loads(msg)
            if parsed["type"] == "result":
                done_count += 1

        await asyncio.gather(*tasks)

        yield json.dumps({
            "type": "done", "totalFiles": total,
        }) + "\n"

    return StreamingResponse(
        _stream(), media_type="application/x-ndjson",
    )


@app.get("/par/debug", response_class=HTMLResponse)
async def par_debug_page():
    return _load_template("par_debug.html")


@app.post("/par/debug-text")
async def par_debug_text(file: UploadFile):
    """Return extracted + cleaned text that would be sent to the LLM."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        text, used_ocr = par_extract_text(tmp_path)
        return {
            "filename": file.filename,
            "text_length": len(text),
            "used_ocr": used_ocr,
            "text": text,
        }
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@app.post("/results/init")
async def results_init():
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results_{ts}.jsonl"
    path = OUTPUT_DIR / filename
    path.touch()
    return {"file": filename, "path": str(path)}


@app.post("/results/append")
async def results_append(request: Request):
    body = await request.json()
    filename = body["file"]
    path = OUTPUT_DIR / filename
    if not path.exists():
        return JSONResponse(
            {"error": "results file not found"}, 404
        )
    with open(path, "a") as f:
        f.write(json.dumps(body["data"]) + "\n")
    return {"ok": True}


@app.post("/parse-yaml")
async def parse_yaml(request: Request):
    body = await request.json()
    return yaml.safe_load(body["yaml"])


# ---------------------------------------------------------------------------
# Settings endpoints (per-user)
# ---------------------------------------------------------------------------

@app.get("/settings", response_class=JSONResponse)
async def get_settings_endpoint(
    uid: str = Depends(get_uid),
):
    settings = get_settings(uid)
    return {
        "model": settings["model"],
        "openai_api_key": mask_key(
            settings.get("openai_api_key", "")
        ),
        "anthropic_api_key": mask_key(
            settings.get("anthropic_api_key", "")
        ),
        "mistral_api_key": mask_key(
            settings.get("mistral_api_key", "")
        ),
        "models": MODELS,
    }


@app.post("/settings")
async def save_settings(
    request: Request,
    uid: str = Depends(get_uid),
):
    body = await request.json()
    updates = {}
    if "model" in body:
        updates["model"] = body["model"]
    if (
        "openai_api_key" in body
        and "..." not in body["openai_api_key"]
    ):
        updates["openai_api_key"] = body["openai_api_key"]
    if (
        "anthropic_api_key" in body
        and "..." not in body["anthropic_api_key"]
    ):
        updates["anthropic_api_key"] = body["anthropic_api_key"]
    if (
        "mistral_api_key" in body
        and "..." not in body["mistral_api_key"]
    ):
        updates["mistral_api_key"] = body["mistral_api_key"]
    settings = update_settings(uid, updates)
    return {
        "model": settings["model"],
        "openai_api_key": mask_key(
            settings.get("openai_api_key", "")
        ),
        "anthropic_api_key": mask_key(
            settings.get("anthropic_api_key", "")
        ),
        "mistral_api_key": mask_key(
            settings.get("mistral_api_key", "")
        ),
    }


@app.post("/validate-key")
async def validate_key_endpoint(
    request: Request,
    uid: str = Depends(get_uid),
):
    body = await request.json()
    provider = body.get("provider")
    key = body.get("key", "")

    if not key or "..." in key:
        return JSONResponse(
            {"error": "Provide a full API key to validate"},
            400,
        )

    if provider == "openai":
        valid, message = await validate_openai_key(key)
    elif provider == "anthropic":
        valid, message = await validate_anthropic_key(key)
    else:
        return JSONResponse(
            {"error": "Unknown provider"}, 400
        )

    return {"valid": valid, "message": message}


# ---------------------------------------------------------------------------
# Firebase config (served to frontend)
# ---------------------------------------------------------------------------

@app.get("/firebase-config")
async def firebase_config():
    """Serve Firebase client config from environment variables."""
    import os
    disabled = os.getenv("FIREBASE_AUTH_DISABLED", "").strip() in ("1", "true")
    return {
        "apiKey": os.environ.get("FIREBASE_API_KEY", ""),
        "authDomain": os.environ.get(
            "FIREBASE_AUTH_DOMAIN", ""
        ),
        "projectId": os.environ.get(
            "FIREBASE_PROJECT_ID", ""
        ),
        "authDisabled": disabled,
    }


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def builder_page():
    return _load_template("builder.html")


@app.get("/settings/page", response_class=HTMLResponse)
async def settings_page():
    return _load_template("settings.html")


@app.get("/template-builder", response_class=HTMLResponse)
async def template_builder_page():
    return _load_template("template_builder.html")


@app.get("/about", response_class=HTMLResponse)
async def about_page():
    return _load_template("about.html")


@app.get("/par", response_class=HTMLResponse)
async def par_page():
    return _load_template("par.html")
