VENV = venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

BASE_IMAGE = us-east1-docker.pkg.dev/petey-dev/petey/base:latest

.PHONY: venv install run deploy build-base clean

venv:
	python3 -m venv $(VENV)
	$(PIP) install -e .

install: venv

run: venv
ifdef LOCAL
ifeq ($(LOCAL),1)
	$(PIP) install -e ../petey
else
	$(PIP) install -e $(LOCAL)
endif
endif
ifdef GIT
	$(PIP) install git+https://github.com/afriedman412/petey.git
endif
	export FIREBASE_AUTH_DISABLED=1 && $(VENV)/bin/uvicorn server.app:app --reload

build-base:
	gcloud config set project petey-dev
	gcloud artifacts repositories create petey --repository-format=docker --location=us-east1 || true
	docker buildx build --platform linux/amd64 -f Dockerfile.base -t $(BASE_IMAGE) --push .

docker-build:
	docker buildx build --platform linux/amd64 -f Dockerfile.base -t $(BASE_IMAGE) --load .
	docker buildx build --platform linux/amd64 -t petey-web --load .

docker-build-local:
	docker build -f Dockerfile.base -t $(BASE_IMAGE) .
	docker build -t petey-web .

docker-run:
	docker run --rm -p 8080:8080 \
		-e FIREBASE_AUTH_DISABLED=1 \
		petey-web

deploy:
	gcloud config set project petey-dev
	gcloud run deploy petey --source . --region=us-east1 --allow-unauthenticated --memory=4Gi

clean:
	rm -rf $(VENV)
