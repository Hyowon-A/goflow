# GoFlow

GoFlow is a distributed workflow orchestration engine written in Go.

It is designed to execute dependency-based tasks across multiple workers while
maintaining durable workflow state and supporting reliable failure recovery.

## Current Capabilities

- Separate API and worker applications
- Environment-based configuration
- PostgreSQL integration using `pgx`
- Local PostgreSQL and Redis setup with Docker Compose
- Redis Streams task publisher
- Redis Streams worker consumer groups
- Health and readiness endpoints
- Workflow definition API endpoints
- Task definition and dependency API endpoints
- Workflow run creation with transactional task-run initialization
- DAG scheduler queueing for root tasks and ready successors
- Worker task-run claiming from `queued` to `running`
- Worker task execution with task-attempt creation and completion
- Idempotent workflow-run submission with `Idempotency-Key`
- Duplicate queue-message acknowledgement for already-owned or terminal task runs
- Built-in `sleep`, `log` and `random_fail` task executors
- Workflow DAG validation with dependency cycle rejection
- Workflow, task-run and task-attempt state transition validation
- Consistent JSON error responses with request IDs
- Structured request, idempotency, scheduler and duplicate-message logging
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

Redis Streams is the task-delivery boundary. The current implementation can
publish task messages to a configured stream, consume them with Redis consumer
groups, queue runnable task runs from DAG state, claim queued task runs in
PostgreSQL, execute the task, persist a task attempt, and acknowledge Redis
messages only after completion is persisted.

```text
Redis XADD
  |
  v
Redis stream message
  |
  v
Worker XREADGROUP
  |
  v
PostgreSQL claim: task_run queued -> running
  |
  v
Load task definition and input
  |
  v
Create task_attempt running
  |
  v
Execute task
  |
  v
Complete task_attempt and task_run
  |
  +-- outcome persisted --> Redis XACK --> message removed from pending
  |
  +-- persistence error -> no XACK ----> message remains pending
```

Redis says which task run a worker should try. PostgreSQL decides whether that
worker is allowed to own it.

## Database Schema

The initial PostgreSQL schema separates reusable workflow definitions from
runtime execution state.

![GoFlow database schema](docs/images/database-schema.png)

Definition tables:

- `workflows`
- `tasks`
- `task_dependencies`

Execution tables:

- `workflow_runs`
- `task_runs`
- `task_attempts`

`workflow_runs` and `task_runs` store runtime inputs, outputs, statuses and
timestamps. `task_attempts` stores individual retry attempts so failures are not
overwritten by later retries.

The schema uses named primary keys, unique constraints, check constraints and
composite foreign keys to prevent invalid cross-workflow dependencies and
mismatched task runs.

## Workflow DAG Validation

Workflow dependencies form a directed acyclic graph. Dependency edges point from
predecessor task to successor task.

```text
extract -> transform -> load
```

GoFlow validates dependency graphs before accepting new dependency edges. The
graph builder tracks all task IDs, outgoing edges, in-degree counts, root tasks
and leaf tasks. Roots, leaves and successor lists are sorted for deterministic
output.

Cycle detection uses Kahn's algorithm. When creating a dependency, the
PostgreSQL repository locks the workflow row, loads tasks and existing
dependencies inside the same transaction, validates the proposed graph and only
then inserts the edge. Cyclic dependency requests return a stable
`dependency_cycle` API error.

Multiple roots, multiple leaves and disconnected components are valid.

## State Transition Automata

GoFlow keeps workflow-run, task-run and task-attempt lifecycles separate. The
state-machine implementation is in `internal/workflow/state_machine.go`.

Workflow run:

![Workflow run state automaton](docs/images/workflow-run-automaton.svg)

Task run:

![Task run state automaton](docs/images/task-run-automaton.svg)

Task attempt:

![Task attempt state automaton](docs/images/task-attempt-automaton.svg)

Terminal states have no outgoing transitions. Same-state transitions and
unknown states are rejected by the workflow package before persistence or worker
logic can depend on them.

Schema files:

- [Initial up migration](migrations/001_initial_schema.up.sql)
- [Initial down migration](migrations/001_initial_schema.down.sql)
- [Schema constraint tests](internal/database/schema_constraints_test.go)
- [ADR-001: Separate Workflow Definitions from Execution State](docs/adr/ADR-001-workflow-definitions-and-execution-runs.md)

## Project Structure

```text
cmd/
  api/
  worker/

internal/
  config/
  database/
  httpserver/
  queue/
  workflow/

docs/
migrations/
tests/
```

## Local Development

Create the local environment file:

```sh
cp .env.example .env
```

Start local infrastructure:

```sh
make postgres-up
make redis-up
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
| `REDIS_ADDR` | No | `localhost:6379` | Redis address used by the queue publisher and worker consumer. |
| `QUEUE_STREAM_NAME` | No | `goflow:tasks` | Redis stream used for task messages. |
| `WORKER_ID` | No | Generated | Worker consumer name used for Redis consumer groups and task-run claims. |
| `QUEUE_CONSUMER_GROUP` | No | `goflow-workers` | Redis consumer group used by workers. |
| `QUEUE_BLOCK_TIMEOUT` | No | `1s` | Blocking read timeout for worker Redis reads. |
| `QUEUE_READ_COUNT` | No | `1` | Maximum messages read per worker Redis read. |

The provided `.env.example` points at the local Docker Compose PostgreSQL
instance on port `5433` and Redis on port `6379`.

## API Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Returns basic API liveness information. |
| `GET` | `/ready` | Checks whether the API can reach PostgreSQL. |
| `POST` | `/workflows` | Creates a reusable workflow definition. |
| `POST` | `/workflows/{workflowID}/tasks` | Creates a task definition inside a workflow. |
| `POST` | `/workflows/{workflowID}/dependencies` | Creates a dependency between two tasks in a workflow. |
| `POST` | `/workflows/{workflowID}/runs` | Creates a workflow run and one pending task run per task. |

API errors use a consistent JSON shape:

```json
{
  "error": "validation_error",
  "message": "request failed validation",
  "request_id": "request-id"
}
```

The API echoes `X-Request-ID` when provided and generates one when it is
missing. Request logs include method, path, status, duration and request ID.

## Documentation

- [Architecture](docs/architecture.md)
- [Failure model](docs/failure-model.md)
- [Test strategy](docs/test-strategy.md)
- [AI-assisted development](docs/ai-assisted-development.md)
- [ADR-002: Use Redis Streams for the Initial Task Queue](docs/adr/ADR-002-redis-streams-task-queue.md)

## Planned Features

- Broader idempotency cleanup and retention policy
- Retry with exponential backoff
- Dead-letter handling
- Transactional outbox publishing
- Worker heartbeat and lease recovery
- Prometheus metrics
- Grafana dashboards
- Load testing and failure injection

## Development Principles

- PostgreSQL is the authoritative source of workflow state.
- Redis Streams carries task-delivery messages, not authoritative workflow state.
- Task delivery uses at-least-once semantics.
- Duplicate delivery must not create duplicate logical side effects.
- Failure handling must be observable and testable.
- Architectural decisions should be documented through ADRs.
