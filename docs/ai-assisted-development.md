# AI-Assisted Development Log

## Purpose

AI is used to support design review, implementation, testing and documentation.

All architectural decisions, code changes and reliability claims are reviewed and validated manually.

## Day 1

### AI-assisted work

- Reviewed possible Go project structures
- Explained unfamiliar Go syntax and standard-library behaviour
- Helped identify local PostgreSQL and port conflicts
- Suggested health and readiness endpoints
- Reviewed configuration, database connection and graceful shutdown code

### Decisions retained

- Separate API and worker entry points
- PostgreSQL connection pool using `pgx`
- Chi router for HTTP endpoints
- Environment-based configuration
- Graceful shutdown using OS signals
- Docker Compose for local PostgreSQL

### Decisions modified

- PostgreSQL was moved from port `5432` to `5433` because a local PostgreSQL instance was already using port `5432`
- The API port was changed from `8080` because another process was using it

### Validation performed

- Ran `docker compose config`
- Confirmed PostgreSQL container health
- Ran `gofmt`
- Ran `go vet`
- Ran `go test ./...`
- Verified database connectivity during API startup
