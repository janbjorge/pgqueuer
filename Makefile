
.PHONY: help pytest benchmark lint import-lint typecheck sync check

help:
	@echo "PGQueuer Makefile"
	@echo "Available commands:"
	@echo "  pytest       Run pytest"
	@echo "  benchmark    Run benchmark"
	@echo "  lint         Run ruff linter"
	@echo "  import-lint  Run import linter (hex architecture validation)"
	@echo "  typecheck    Run mypy type checks"
	@echo "  sync         Install dependencies via uv"
	@echo "  check        Run sync, lint, import-lint, typecheck and pytest"

pytest:
	$(HOST_ENV) uv run pytest $(ARGS)

benchmark:
	docker compose run --rm benchmark

lint:
	 uv run ruff check .

import-lint:
	 uv run lint-imports

typecheck:
	 uv run mypy .

sync:
	 uv sync --all-extras --frozen

check: sync lint import-lint typecheck pytest
