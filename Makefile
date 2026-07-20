.PHONY: postgres-up postgres-down api worker test fmt vet check # Tells make that these names are commands, not files.

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