"""
Firebase Authentication middleware.

Verifies Firebase ID tokens from the Authorization header and injects
the authenticated user's UID into the request state.

Set FIREBASE_AUTH_DISABLED=1 to skip verification (local dev only).
"""
import os

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# Paths that don't require authentication (POST endpoints
# that are safe without auth, e.g. debug tools)
PUBLIC_POST_PATHS = {"/parse-yaml", "/par/debug-text"}

# Initialize Firebase Admin SDK once (skip when auth is disabled for local dev).
# On Cloud Run, calling initialize_app() with no args uses the
# default service account automatically (ADC).
# Import is conditional so the desktop app can run without firebase_admin.
firebase_admin = None
firebase_auth = None
if os.getenv("FIREBASE_AUTH_DISABLED", "").strip() not in ("1", "true"):
    import firebase_admin as _fa
    from firebase_admin import auth as _fa_auth
    firebase_admin = _fa
    firebase_auth = _fa_auth
    if not firebase_admin._apps:
        firebase_admin.initialize_app()


def _is_public(request: Request) -> bool:
    """Check if a request is public (no auth required).

    All GET requests are public (they serve pages or config).
    POST endpoints require auth unless explicitly exempted.
    """
    if request.method == "GET":
        return True
    return request.url.path in PUBLIC_POST_PATHS


def _auth_disabled() -> bool:
    return os.getenv("FIREBASE_AUTH_DISABLED", "").strip() in ("1", "true")


class FirebaseAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware that verifies Firebase ID tokens.

    - Public paths (HTML pages, health check) are allowed through.
    - All API endpoints require a valid Bearer token.
    - The decoded user UID is stored in request.state.uid.
    """

    async def dispatch(self, request: Request, call_next):
        if _auth_disabled():
            request.state.uid = "local-dev"
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header.removeprefix("Bearer ").strip()
            try:
                decoded = firebase_auth.verify_id_token(token)
                request.state.uid = decoded["uid"]
            except Exception:
                return JSONResponse(
                    {"error": "Invalid or expired token"},
                    status_code=401,
                )
        elif not _is_public(request):
            return JSONResponse(
                {"error": "Missing or invalid Authorization header"},
                status_code=401,
            )

        return await call_next(request)


def get_uid(request: Request) -> str:
    """Extract authenticated user UID from request."""
    uid = getattr(request.state, "uid", None)
    if not uid:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return uid
