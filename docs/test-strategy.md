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

### Redis queue and worker tests

Queue unit tests cover configuration validation, task message validation and
deterministic message field serialization. Consumer-side tests also cover
parsing Redis field maps into `TaskMessage`, unsupported schema versions,
missing required fields, generated consumer names and no-message behavior.

Redis Streams integration tests run against real Redis when it is available at
the configured local address. They publish task messages through the
`RedisStreamPublisher`, read the stream back, create consumer groups, consume
messages through `RedisStreamConsumer`, and verify:

- a Redis stream message ID is returned
- each message is appended instead of overwritten
- stream payload fields match the task message schema
- consumer group creation is idempotent
- one new message is delivered to one consumer in a group
- acknowledged messages leave the Redis pending entries list
- tests skip clearly when Redis is not running

Worker service tests use fakes for Redis and PostgreSQL boundaries. They prove
that one worker step receives a message, claims the referenced task run, loads
execution data, creates a task attempt, runs the executor, completes the
attempt and task run, and acknowledges only after completion is persisted.

Worker executor tests cover the built-in `sleep`, `log` and `random_fail`
executors. Worker service integration tests use PostgreSQL with fake queue
messages to verify successful and failed execution paths end in durable task
state.

Scheduler tests use fake publishers for publish coordination and PostgreSQL
integration tests for runnable-task selection. A worker/scheduler integration
test runs an `A -> B, C -> D` workflow with real PostgreSQL and an in-memory
queue to prove fan-out and fan-in progress without requiring Redis.

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
- Redis Streams consumer group setup and acknowledgement behavior
- Conditional task-run claiming from `queued` to `running`
- Task-run execution loading
- Task-attempt creation and completion
- Built-in worker executors
- Worker receive, claim, execute, complete and acknowledgement coordination
- Scheduler queueing for runnable task runs
- Fan-out and fan-in DAG scheduling
- Duplicate scheduler calls do not queue the same task run twice

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

Run the worker service tests:

```sh
go test ./internal/worker -v
```

Some integration tests require local PostgreSQL. Start it with:

```sh
make postgres-up
```

Redis queue integration tests run when Redis is available. Start it with:

```sh
make redis-up
```

If Redis is not running, Redis integration tests skip with a clear message.

Manual Redis pending-entry validation can be run against local Docker services:

```sh
make postgres-up
make redis-up
```

Use a dedicated stream such as `goflow:tasks:validation`, start `cmd/worker`
with `QUEUE_STREAM_NAME` pointing to that stream, publish one executable
message and one message whose `task_run_id` is not claimable, then inspect:

```sh
docker compose exec redis redis-cli XINFO GROUPS goflow:tasks:validation
docker compose exec redis redis-cli XPENDING goflow:tasks:validation goflow-workers
```

The executable message should be acknowledged after the task run reaches a
terminal state; the failed-claim message should remain pending.

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
- Use real Redis for consumer-group behavior so the tests prove `XREADGROUP`
  and `XACK` semantics.
- Keep worker service tests independent of Redis and PostgreSQL by testing the
  coordination contract with fakes.
