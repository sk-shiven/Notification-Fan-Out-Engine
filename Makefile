.PHONY: help install start-docker test lint format clean up down logs

VENV := .venv
PYTHON := $(VENV)/bin/python
UV := $(shell which uv 2>/dev/null || echo "$$HOME/.local/bin/uv")

help:
	@echo "Available commands:"
	@echo "  make install       - Create .venv and install dependencies in editable mode via uv"
	@echo "  make start-docker  - Start and wait for Docker daemon"
	@echo "  make test          - Run pytest test suite"
	@echo "  make lint          - Run ruff linter"
	@echo "  make format        - Format code with ruff"
	@echo "  make up            - Start Docker Compose infrastructure"
	@echo "  make down          - Stop Docker Compose infrastructure"
	@echo "  make logs          - Tail Docker Compose logs"
	@echo "  make clean         - Clean bytecode and build caches"

install:
	$(UV) venv --python 3.13 $(VENV)
	$(UV) pip install -e ".[dev]"

start-docker:
	./scripts/start_docker.sh

test:
	$(PYTHON) -m pytest tests/

lint:
	$(VENV)/bin/ruff check .

format:
	$(VENV)/bin/ruff format .

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .ruff_cache build dist *.egg-info
