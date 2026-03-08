"""
Server-side settings management.
Stores model and API key config in a local JSON file.
"""
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = BASE_DIR / "settings.json"

DEFAULTS = {
    "model": "gpt-4.1-mini",
    "openai_api_key": "",
    "anthropic_api_key": "",
}

MODELS = [
    {"id": "gpt-4.1-mini", "name": "GPT-4.1 Mini", "provider": "openai"},
    {"id": "gpt-4.1", "name": "GPT-4.1", "provider": "openai"},
    {"id": "gpt-4.1-nano", "name": "GPT-4.1 Nano", "provider": "openai"},
    {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai"},
    {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "provider": "openai"},
    {"id": "claude-haiku-4-5-20251001", "name": "Claude Haiku 4.5", "provider": "anthropic"},
    {"id": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4", "provider": "anthropic"},
]


def _load() -> dict:
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH) as f:
            stored = json.load(f)
        return {**DEFAULTS, **stored}
    return dict(DEFAULTS)


def _save(settings: dict):
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def get_settings() -> dict:
    return _load()


def update_settings(updates: dict) -> dict:
    settings = _load()
    for key in DEFAULTS:
        if key in updates and updates[key] is not None:
            settings[key] = updates[key]
    _save(settings)
    return settings


def get_provider(model_id: str) -> str:
    for m in MODELS:
        if m["id"] == model_id:
            return m["provider"]
    return "openai"


def mask_key(key: str) -> str:
    if not key or len(key) < 8:
        return ""
    return key[:3] + "..." + key[-4:]
