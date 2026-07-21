# GoFlow Test Strategy

## Purpose

GoFlow tests should prove durable workflow behavior against the same boundaries
the production system uses: HTTP handlers, service logic, PostgreSQL constraints
and later Redis-backed worker execution.

## Current Test Layers

### Unit and handler tests

Handler tests cover API behavior that does not require a real database, such as
health checks, readiness checks, JSON decoding, request IDs and structured
logging.

### PostgreSQL integration tests

Database tests run against real PostgreSQL. They prove schema constraints such
as unique task names within a workflow, duplicate dependency prevention,
same-workflow foreign keys, timestamp checks and attempt-number checks.

Workflow API integration tests also run against PostgreSQL. They verify that the
HTTP API, workflow service and repository work together correctly.

## Current Coverage

- `GET /health`
- `GET /ready`
- `POST /workflows`
- `POST /workflows/{workflowID}/tasks`
- `POST /workflows/{workflowID}/dependencies`
- `POST /workflows/{workflowID}/runs`
- Invalid JSON bodies
- Missing required fields
- Invalid UUID path parameters
- Unknown workflows
- Invalid task references
- Duplicate task names
- Duplicate dependencies
- Empty workflow runs
- Request ID response headers and error bodies
- Structured request logging
- Transactional workflow-run creation with pending task runs

## Validation Commands

Run the full test suite:

```sh
go test ./...
```

Run the HTTP API tests:

```sh
go test ./internal/httpserver -v
```

Run the database constraint tests:

```sh
go test ./internal/database -v
```

Some integration tests require local PostgreSQL. Start it with:

```sh
make postgres-up
```

## Testing Principles

- Prefer real PostgreSQL for schema and repository behavior.
- Keep HTTP tests focused on observable API contracts.
- Use transactions, cleanup helpers or isolated test data to avoid test
  interference.
- Prove failure cases, not only successful paths.
- Add broader integration tests when a change crosses handler, service and
  repository boundaries.
