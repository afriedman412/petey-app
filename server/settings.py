"""
Per-user settings stored in Firestore.

Firestore collection: "user_settings"
Each document is keyed by the user's Firebase UID and contains:
  - model: selected model ID
  - openai_api_key: encrypted-at-rest by Firestore
  - anthropic_api_key: encrypted-at-rest by Firestore

Falls back to local JSON file when FIREBASE_AUTH_DISABLED=1 (local dev).
"""
import json
import os
from pathlib import Path

try:
    from google.cloud import firestore
except ImportError:
    firestore = None

BASE_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = BASE_DIR / "settings.json"
COLLECTION = "user_settings"

DEFAULTS = {
    "model": "gpt-4.1-mini",
    "openai_api_key": "",
    "anthropic_api_key": "",
    "datalab_api_key": "",
    "concurrency": 10,
}

MODELS = [
    {"id": "none", "name": "Text only (no LLM)", "provider": "none"},
    {"id": "gpt-4.1-mini", "name": "GPT-4.1 Mini", "provider": "openai"},
    {"id": "gpt-4.1", "name": "GPT-4.1", "provider": "openai"},
    {"id": "gpt-4.1-nano", "name": "GPT-4.1 Nano", "provider": "openai"},
    {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai"},
    {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "provider": "openai"},
    {"id": "claude-haiku-4-5-20251001", "name": "Claude Haiku 4.5", "provider": "anthropic"},
    {"id": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4", "provider": "anthropic"},
]


def _use_local() -> bool:
    return os.getenv("FIREBASE_AUTH_DISABLED", "1").strip() in ("1", "true")


# ---------------------------------------------------------------------------
# Firestore backend
# ---------------------------------------------------------------------------

_db = None


def _get_db():
    global _db
    if _db is None:
        _db = firestore.Client(database="petey-fb-db")
    return _db


def _firestore_get(uid: str) -> dict:
    doc = _get_db().collection(COLLECTION).document(uid).get()
    if doc.exists:
        return {**DEFAULTS, **doc.to_dict()}
    return dict(DEFAULTS)


def _firestore_set(uid: str, settings: dict):
    _get_db().collection(COLLECTION).document(uid).set(settings)


# ---------------------------------------------------------------------------
# Local file backend (dev fallback)
# ---------------------------------------------------------------------------

def _local_get() -> dict:
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH) as f:
            stored = json.load(f)
        return {**DEFAULTS, **stored}
    return dict(DEFAULTS)


def _local_set(settings: dict):
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_settings(uid: str) -> dict:
    if _use_local():
        return _local_get()
    return _firestore_get(uid)


def update_settings(uid: str, updates: dict) -> dict:
    settings = get_settings(uid)
    for key in DEFAULTS:
        if key in updates and updates[key] is not None:
            settings[key] = updates[key]
    if _use_local():
        _local_set(settings)
    else:
        _firestore_set(uid, settings)
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
