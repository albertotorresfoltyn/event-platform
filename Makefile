.PHONY: install lint format typecheck test test-unit test-integration run check

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest --cov --cov-report=term-missing

test-unit:
	uv run pytest tests/unit

test-integration:
	uv run pytest -m integration tests/integration

run:
	uv run uvicorn event_platform.main:app --reload

check: lint typecheck test
