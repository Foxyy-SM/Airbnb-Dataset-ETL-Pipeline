.PHONY: test lint sql up

test:
	python -m pytest

lint:
	python -m ruff check src tests

sql:
	python -m airbnb_etl.ddl

up:
	docker compose up --build
