.PHONY: up down test lint logs

up:
	docker compose up -d

down:
	docker compose down -v

test:
	pytest tests/

lint:
	ruff check .
	mypy .

logs:
	docker compose logs -f
