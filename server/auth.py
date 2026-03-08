"""
Firebase Authentication middleware.

Verifies Firebase ID tokens from the Authorization header and injects
the authenticated user's UID into the request state.

Set FIREBASE_AUTH_DISABLED=1 to skip verification (local dev only).
"""
import os

import firebase_admin
from firebase_admin import auth as firebase_auth
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

# Paths that don't require authentication
PUBLIC_PATHS = {
    "/", "/settings/page", "/health", "/firebase-config",
}

# Initialize Firebase Admin SDK once.
# On Cloud Run, calling initialize_app() with no args uses the
# default service account automatically (ADC).
if not firebase_admin._apps:
    firebase_admin.initialize_app()


def _is_public(path: str) -> bool:
    """Check if a path is public (no auth required)."""
    return path in PUBLIC_PATHS


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
        if _is_public(request.url.path):
            return await call_next(request)

        if _auth_disabled():
            request.state.uid = "local-dev"
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"error": "Missing or invalid Authorization header"},
                status_code=401,
            )

        token = auth_header.removeprefix("Bearer ").strip()
        try:
            decoded = firebase_auth.verify_id_token(token)
            request.state.uid = decoded["uid"]
        except Exception:
            return JSONResponse(
                {"error": "Invalid or expired token"},
                status_code=401,
            )

        return await call_next(request)


def get_uid(request: Request) -> str:
    """Extract authenticated user UID from request."""
    uid = getattr(request.state, "uid", None)
    if not uid:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return uid
