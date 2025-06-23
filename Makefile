COMPOSE_FILE ?= docker-compose.yml
HOST_ENV = PGUSER=pgquser PGDATABASE=pgqdb PGPASSWORD=pgqpw PGHOST=localhost PGPORT=5432

.PHONY: help build up db down clean pytest benchmark lint typecheck sync check

help:
	@echo "PGQueuer Makefile"
	@echo "Available commands:"
	@echo "  build       Build docker images"
	@echo "  up          Start database container"
	@echo "  db          Start database and populate tables"
	@echo "  pytest      Run pytest"
	@echo "  benchmark   Run benchmark"
	@echo "  lint        Run ruff linter"
	@echo "  typecheck   Run mypy type checks"
	@echo "  sync        Install dependencies via uv"
	@echo "  check       Run lint, typecheck, sync and pytest"
	@echo "  down        Stop containers"
	@echo "  clean       Remove containers, networks, volumes and images"

build:
	docker compose -f $(COMPOSE_FILE) build

up:
	docker compose -f $(COMPOSE_FILE) up -d db

db: up
	docker compose -f $(COMPOSE_FILE) run --rm populate

pytest:
	$(HOST_ENV) uv run pytest $(ARGS)

benchmark:
	docker compose -f $(COMPOSE_FILE) run --rm benchmark

lint:
	$(HOST_ENV) uv run ruff check .

typecheck:
	$(HOST_ENV) uv run mypy .

sync:
	$(HOST_ENV) uv sync --all-extras --frozen

check: lint typecheck sync pytest

down:
	docker compose -f $(COMPOSE_FILE) down --remove-orphans -v

clean:
	docker compose -f $(COMPOSE_FILE) down --rmi all --volumes --remove-orphans
