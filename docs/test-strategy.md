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

Workflow unit tests cover pure graph behavior without PostgreSQL. They prove
adjacency-list construction, in-degree counts, deterministic roots and leaves,
valid single-task, linear, fan-out, fan-in, diamond and disconnected graphs, and
cycle rejection.

### PostgreSQL integration tests

Database tests run against real PostgreSQL. They prove schema constraints such
as unique task names within a workflow, duplicate dependency prevention,
same-workflow foreign keys, timestamp checks and attempt-number checks.

Workflow API integration tests also run against PostgreSQL. They verify that the
HTTP API, workflow service and repository work together correctly.

### Redis queue tests

Queue unit tests cover configuration validation, task message validation and
deterministic message field serialization.

Redis Streams integration tests run against real Redis when it is available at
the configured local address. They publish task messages through the
`RedisStreamPublisher`, read the stream back, and verify:

- a Redis stream message ID is returned
- each message is appended instead of overwritten
- stream payload fields match the task message schema
- tests skip clearly when Redis is not running

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
- Workflow graph construction
- Deterministic graph root and leaf ordering
- Dependency cycle rejection
- Disconnected workflow graph components
- Dependency cycle API error mapping
- Empty workflow runs
- Request ID response headers and error bodies
- Structured request logging
- Transactional workflow-run creation with pending task runs
- Transactional dependency creation with graph validation before insert
- Queue configuration defaults and validation
- Task queue message schema validation
- Redis Streams publisher integration with real Redis when available

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

Run the workflow graph and state-machine tests:

```sh
go test ./internal/workflow -v
```

Run the queue tests:

```sh
go test ./internal/queue -v
```

Some integration tests require local PostgreSQL. Start it with:

```sh
make postgres-up
```

Redis queue integration tests run when Redis is available. Start it with:

```sh
make redis-up
```

If Redis is not running, the Redis integration test skips with a clear message.

## Testing Principles

- Prefer real PostgreSQL for schema and repository behavior.
- Keep HTTP tests focused on observable API contracts.
- Use transactions, cleanup helpers or isolated test data to avoid test
  interference.
- Prove failure cases, not only successful paths.
- Add broader integration tests when a change crosses handler, service and
  repository boundaries.
- Keep pure graph tests independent of PostgreSQL so scheduling logic can evolve
  without database setup.
- Use PostgreSQL-backed API integration tests for dependency behavior that
  depends on transactions, locks or database constraints.
- Keep queue message serialization tests independent of Redis.
- Use real Redis for publisher behavior so the tests prove the `XADD` boundary.
