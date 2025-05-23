.PHONY: build up down logs clean test analyze

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	rm -rf data/input/* data/output/*
	find . -type d -name "__pycache__" -exec rm -r {} +

test:
	python -m pytest tests/

analyze:
	python app/analyze.py

setup-minio:
	@echo "Creating MinIO bucket..."
	@curl -X POST http://localhost:9000/api/v1/buckets \
		-H "Content-Type: application/json" \
		-u minioadmin:minioadmin \
		-d '{"name": "datapipeline"}' 