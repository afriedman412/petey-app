"""
Web interface for PDF field extraction.
"""
import json
import tempfile
from datetime import datetime
from pathlib import Path

import yaml
from fastapi import FastAPI, UploadFile, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse

from server.extract import (
    async_extract, load_schema,
    list_schemas, SCHEMAS_DIR, _build_model,
    extract_text, check_text_length,
)
from server.settings import (
    get_settings, update_settings, mask_key, MODELS,
)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"

app = FastAPI()

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

@app.get("/schemas")
async def schemas():
    return list_schemas()


@app.get("/schemas/{schema_file}")
async def get_schema(schema_file: str):
    path = SCHEMAS_DIR / schema_file
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
    path = SCHEMAS_DIR / filename
    with open(path, "w") as f:
        yaml.dump(spec, f, default_flow_style=False, sort_keys=False)
    _schema_cache.pop(filename, None)
    return {"file": filename, "name": spec.get("name")}


@app.post("/extract")
async def extract_endpoint(
    file: UploadFile,
    schema_file: str = Form(None),
    schema_spec: str = Form(None),
    instructions: str = Form(""),
):
    if schema_spec:
        spec = json.loads(schema_spec)
        response_model = _build_model(spec)
    elif schema_file:
        response_model = get_model(schema_file)
        with open(SCHEMAS_DIR / schema_file) as f:
            spec = yaml.safe_load(f)
    else:
        return JSONResponse({"error": "No schema provided"}, 400)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name
    try:
        text = extract_text(tmp_path)
        warning = check_text_length(text)
        result = await async_extract(
            tmp_path, response_model, instructions=instructions,
        )
        data = result.model_dump()
        if spec.get("record_type") == "array" and "items" in data:
            data = {
                "_source_file": file.filename,
                "records": data["items"],
            }
        else:
            data["_source_file"] = file.filename
        if warning:
            data["_warning"] = warning
    except Exception as e:
        data = {"_source_file": file.filename, "_error": str(e)}
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    return data


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
        return JSONResponse({"error": "results file not found"}, 404)
    with open(path, "a") as f:
        f.write(json.dumps(body["data"]) + "\n")
    return {"ok": True}


@app.post("/parse-yaml")
async def parse_yaml(request: Request):
    body = await request.json()
    return yaml.safe_load(body["yaml"])


@app.get("/settings", response_class=JSONResponse)
async def get_settings_endpoint():
    settings = get_settings()
    return {
        "model": settings["model"],
        "openai_api_key": mask_key(settings.get("openai_api_key", "")),
        "anthropic_api_key": mask_key(settings.get("anthropic_api_key", "")),
        "models": MODELS,
    }


@app.post("/settings")
async def save_settings(request: Request):
    body = await request.json()
    updates = {}
    if "model" in body:
        updates["model"] = body["model"]
    if "openai_api_key" in body and "..." not in body["openai_api_key"]:
        updates["openai_api_key"] = body["openai_api_key"]
    if "anthropic_api_key" in body and "..." not in body["anthropic_api_key"]:
        updates["anthropic_api_key"] = body["anthropic_api_key"]
    settings = update_settings(updates)
    return {
        "model": settings["model"],
        "openai_api_key": mask_key(settings.get("openai_api_key", "")),
        "anthropic_api_key": mask_key(settings.get("anthropic_api_key", "")),
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
