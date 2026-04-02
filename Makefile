VENV = venv
SYSTEM_PYTHON ?= /Library/Frameworks/Python.framework/Versions/3.13/bin/python3
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
PARSER_URL ?= https://petey-parser-425941924538.us-east1.run.app

BASE_IMAGE = us-east1-docker.pkg.dev/petey-dev/petey/base:latest
WEB_IMAGE = us-east1-docker.pkg.dev/petey-dev/petey/web:latest

.PHONY: venv install run deploy deploy-web deploy-parser build-base clean

venv:
	@if [ ! -d "$(VENV)" ]; then $(SYSTEM_PYTHON) -m venv $(VENV) && $(PIP) install -e .; fi

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
	FIREBASE_AUTH_DISABLED=1 $(VENV)/bin/uvicorn server.app:app --reload

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



-include .env
-include ../.env
export

FIREBASE_AUTH_DOMAIN ?= petey-dev.firebaseapp.com
FIREBASE_PROJECT_ID ?= petey-dev

deploy-web:
	gcloud config set project petey-dev
	docker buildx build --platform linux/amd64 -t $(WEB_IMAGE) --push \
		$(if $(PETEY),--build-arg PETEY_BUST_CACHE=$$(date +%s),) .
	gcloud run deploy petey --image=$(WEB_IMAGE) --region=us-east1 --allow-unauthenticated --memory=4Gi --cpu=4 --timeout=3600 \
		--set-env-vars=FIREBASE_AUTH_DISABLED=0,PARSER_URL=$(PARSER_URL),FIREBASE_API_KEY=$(FIREBASE_API_KEY),FIREBASE_AUTH_DOMAIN=$(FIREBASE_AUTH_DOMAIN),FIREBASE_PROJECT_ID=$(FIREBASE_PROJECT_ID)

deploy-parser:
	gcloud config set project petey-dev
	cd parser && gcloud run deploy petey-parser --source . --region=us-east1 --port=8081 --cpu=4 --memory=2Gi --min-instances=0 --max-instances=10 --allow-unauthenticated

deploy:
ifdef SERVICE
ifeq ($(SERVICE),w)
	$(MAKE) deploy-web
else ifeq ($(SERVICE),p)
	$(MAKE) deploy-parser
endif
else
	$(MAKE) deploy-web
	$(MAKE) deploy-parser
endif

clean:
	rm -rf $(VENV)
