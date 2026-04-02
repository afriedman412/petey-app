"""
Run history stored in Firestore (metadata only).
Actual extraction results are cached client-side in IndexedDB.

Firestore path: user_settings/{uid}/runs/{run_id}
"""
import os
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

RUNS_SUBCOLLECTION = "runs"
LOCAL_RUNS_PATH = Path(__file__).resolve().parent.parent / "runs.json"


def _use_local() -> bool:
    return os.getenv("FIREBASE_AUTH_DISABLED", "1").strip() in ("1", "true")


def _get_db():
    from google.cloud import firestore
    from server.settings import _get_db as get_db
    return get_db()


def _runs_ref(uid: str):
    return _get_db().collection("user_settings").document(uid).collection(RUNS_SUBCOLLECTION)


# ---------------------------------------------------------------------------
# Local file backend (dev fallback)
# ---------------------------------------------------------------------------

def _local_load() -> list[dict]:
    if LOCAL_RUNS_PATH.exists():
        with open(LOCAL_RUNS_PATH) as f:
            return json.load(f)
    return []


def _local_save(runs: list[dict]):
    with open(LOCAL_RUNS_PATH, "w") as f:
        json.dump(runs, f, indent=2)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_run(uid: str, data: dict) -> dict:
    """Create a new run record. Returns the run with its ID."""
    run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()
    run = {
        "id": run_id,
        "filename": data.get("filename", ""),
        "schema_spec": data.get("schema_spec"),
        "instructions": data.get("instructions", ""),
        "model": data.get("model", ""),
        "parser": data.get("parser", "pymupdf"),
        "mode": data.get("mode", "query"),
        "concurrency": data.get("concurrency", 10),
        "total_pages": data.get("total_pages", 0),
        "pages_parsed": 0,
        "pages_extracted": 0,
        "status": "running",
        "started_at": now,
        "finished_at": None,
    }

    if _use_local():
        runs = _local_load()
        runs.insert(0, run)
        _local_save(runs)
    else:
        _runs_ref(uid).document(run_id).set(run)

    return run


def update_run(uid: str, run_id: str, updates: dict) -> dict:
    """Update fields on an existing run."""
    if _use_local():
        runs = _local_load()
        for r in runs:
            if r["id"] == run_id:
                r.update(updates)
                _local_save(runs)
                return r
        return {}
    else:
        ref = _runs_ref(uid).document(run_id)
        ref.update(updates)
        return {**ref.get().to_dict(), "id": run_id}


def finish_run(uid: str, run_id: str, status: str = "completed",
               pages_parsed: int = 0, pages_extracted: int = 0):
    """Mark a run as finished."""
    now = datetime.now(timezone.utc).isoformat()
    return update_run(uid, run_id, {
        "status": status,
        "finished_at": now,
        "pages_parsed": pages_parsed,
        "pages_extracted": pages_extracted,
    })


def list_runs(uid: str, limit: int = 50) -> list[dict]:
    """List recent runs, newest first."""
    if _use_local():
        runs = _local_load()
        return runs[:limit]
    else:
        docs = (
            _runs_ref(uid)
            .order_by("started_at", direction="DESCENDING")
            .limit(limit)
            .stream()
        )
        return [{**doc.to_dict(), "id": doc.id} for doc in docs]


def get_run(uid: str, run_id: str) -> dict | None:
    """Get a single run by ID."""
    if _use_local():
        runs = _local_load()
        for r in runs:
            if r["id"] == run_id:
                return r
        return None
    else:
        doc = _runs_ref(uid).document(run_id).get()
        if doc.exists:
            return {**doc.to_dict(), "id": doc.id}
        return None


def delete_run(uid: str, run_id: str):
    """Delete a run."""
    if _use_local():
        runs = _local_load()
        runs = [r for r in runs if r["id"] != run_id]
        _local_save(runs)
    else:
        _runs_ref(uid).document(run_id).delete()
