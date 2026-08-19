.PHONY: postgres-up postgres-down redis-up api worker test fmt vet check observability-up observability-down # Tells make that these names are commands, not files.

postgres-up:
	docker compose up -d postgres

postgres-down:
	docker compose down

api:
	go run ./cmd/api

worker:
	go run ./cmd/worker

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: fmt vet test

redis-up:
	docker compose up -d redis

observability-up:
	docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d prometheus

observability-down:
	docker compose -f docker-compose.yml -f docker-compose.observability.yml down prometheus
