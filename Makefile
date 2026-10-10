.PHONY: up down test lint logs demo eval

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

demo:
	python -m bench.demo

eval:
	python -m bench.evaluate_detectors
