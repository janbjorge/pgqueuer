
.PHONY: help pytest benchmark lint typecheck sync check

help:
	@echo "PGQueuer Makefile"
	@echo "Available commands:"
	@echo "  pytest      Run pytest"
	@echo "  benchmark   Run benchmark"
	@echo "  lint        Run ruff linter"
	@echo "  typecheck   Run mypy type checks"
	@echo "  sync        Install dependencies via uv"
	@echo "  check       Run lint, typecheck, sync and pytest"

pytest:
	$(HOST_ENV) uv run pytest $(ARGS)

benchmark:
	docker compose run --rm benchmark

lint:
	 uv run ruff check .

typecheck:
	 uv run mypy .

sync:
	 uv sync --all-extras --frozen

check: sync lint typecheck pytest
