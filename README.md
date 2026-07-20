# GoFlow

GoFlow is a distributed workflow orchestration engine written in Go.

It is designed to execute dependency-based tasks across multiple workers while
maintaining durable workflow state and supporting reliable failure recovery.

## Current Capabilities

- Separate API and worker applications
- Environment-based configuration
- PostgreSQL integration using `pgx`
- Local PostgreSQL setup with Docker Compose
- Health and readiness endpoints
- Graceful shutdown on `Ctrl+C` and `SIGTERM`
- Standardised development commands through a `Makefile`

## Architecture

```text
Client
  |
  v
GoFlow API
  |
  +--- PostgreSQL
  |
  +--- Redis Streams
           |
           v
       Worker Pool
```

PostgreSQL is the source of truth for workflow and task state.

Redis Streams will be used to distribute executable tasks across workers.

## Project Structure

```text
cmd/
  api/
  worker/

internal/
  config/
  database/
  httpserver/
  workflow/
  task/
  worker/

docs/
migrations/
tests/
```

## Local Development

Create the local environment file:

```sh
cp .env.example .env
```

Start PostgreSQL:

```sh
make postgres-up
```

Start the API:

```sh
make api
```

Start the worker:

```sh
make worker
```

Run formatting, static analysis, and tests:

```sh
make check
```

## Configuration

The API loads configuration from the environment. During local development,
values can be provided through `.env`.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `APP_ENV` | No | `development` | Runtime environment name. |
| `HTTP_PORT` | No | `8080` | API listen port. |
| `DATABASE_URL` | Yes | None | PostgreSQL connection string. |

The provided `.env.example` points at the local Docker Compose PostgreSQL
instance on port `5433`.

## API Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Returns basic API liveness information. |
| `GET` | `/ready` | Checks whether the API can reach PostgreSQL. |

## Documentation

- [Architecture](docs/architecture.md)
- [Failure model](docs/failure-model.md)
- [Test strategy](docs/test-strategy.md)
- [AI-assisted development](docs/ai-assisted-development.md)

## Planned Features

- Workflow definitions and execution runs
- DAG validation and dependency scheduling
- Redis Streams task distribution
- Parallel worker execution
- Idempotent workflow and task processing
- Retry with exponential backoff
- Dead-letter handling
- Transactional outbox publishing
- Worker heartbeat and lease recovery
- Prometheus metrics
- Grafana dashboards
- Load testing and failure injection

## Development Principles

- PostgreSQL is the authoritative source of workflow state.
- Task delivery will use at-least-once semantics.
- Duplicate delivery must not create duplicate logical side effects.
- Failure handling must be observable and testable.
- Architectural decisions should be documented through ADRs.
