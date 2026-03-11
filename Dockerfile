FROM us-east1-docker.pkg.dev/petey-dev/petey/base:latest

WORKDIR /app

COPY pyproject.toml .
COPY server/ server/
COPY templates/ templates/
COPY schemas/ schemas/

RUN pip install --no-cache-dir .

# Firebase config is passed via env vars at deploy time:
#   FIREBASE_API_KEY, FIREBASE_AUTH_DOMAIN, FIREBASE_PROJECT_ID
# GCP credentials are auto-detected on Cloud Run (no key file needed).

CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "8080"]
