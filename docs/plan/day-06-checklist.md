# Day 6 Checklist: Queue

Goal: add the queue boundary and Redis Streams publisher so scheduler and worker logic can later exchange durable task messages.

## Scope

- [x] Keep Day 6 focused on queue publishing only.
  - [x] Do not implement worker consumer loops yet.
  - [x] Do not implement scheduler dependency release yet.
  - [x] Do not implement executor logic yet.
  - [x] Do not implement retries, leases, claiming or dead-letter handling yet.
- [x] Use Redis Streams as the initial queue backend.
  - [x] Add Redis to local Docker Compose if it is not already present.
  - [x] Keep PostgreSQL as the source of truth for workflow and task state.
  - [x] Treat Redis Streams as task delivery infrastructure, not durable state.
- [x] Define Day 6 delivery semantics before coding.
  - [x] Publishing a task message means a task run is eligible for future worker processing.
  - [x] The queue may deliver messages at least once.
  - [x] Consumers must eventually be idempotent, but full idempotent execution belongs to a later day.
  - [x] Message acknowledgement belongs to worker consumption, not Day 6 publishing.

## Queue Interface

- [x] Add a queue package or workflow-adjacent queue abstraction.
  - [x] Define a small `Queue` interface for publishing task messages.
  - [x] Accept `context.Context` in queue methods.
  - [x] Return stable Go errors or wrapped infrastructure errors.
  - [x] Avoid leaking Redis client types through service interfaces.
- [x] Decide the queue method shape.
  - [x] Prefer a method such as `PublishTask(ctx, message TaskMessage) error`.
  - [x] Keep batch publishing out of Day 6 unless needed by tests.
  - [x] Keep acknowledgement and claiming methods out of Day 6.
- [x] Define queue configuration.
  - [x] Add Redis address configuration.
  - [x] Add stream name configuration.
  - [x] Add clear defaults for local development.
  - [x] Document required environment variables in README.

## Task Message Schema

- [x] Define a task message type.
  - [x] Include `workflow_id`.
  - [x] Include `workflow_run_id`.
  - [x] Include `task_id`.
  - [x] Include `task_run_id`.
  - [ ] Include an attempt number only if Day 6 needs it for future compatibility.
  - [x] Include a message schema version.
- [x] Keep message fields stable and explicit.
  - [x] Use snake_case field names for Redis payload fields.
  - [x] Encode IDs as strings.
  - [x] Avoid embedding full task config unless worker execution needs it later.
  - [x] Avoid putting secrets or large input payloads in Redis messages.
- [x] Document message ownership.
  - [x] PostgreSQL owns task state and payload references.
  - [x] Redis message carries enough identifiers for a worker to load state.

## Redis Streams Publisher

- [x] Add a Redis client dependency if needed.
  - [ ] Keep `go.mod` and `go.sum` changes in a separate commit if possible.
  - [x] Prefer a mature Go Redis client with context support.
- [x] Implement a Redis Streams publisher.
  - [x] Connect using configured Redis address.
  - [x] Publish with `XADD`.
  - [x] Use the configured stream name.
  - [x] Convert task messages into Redis field/value pairs deterministically.
  - [x] Return the Redis stream message ID if useful.
- [x] Handle publisher lifecycle.
  - [x] Close Redis client resources on shutdown if the client requires it.
  - [x] Avoid creating a new Redis client per message.
  - [x] Add constructor validation for required config.
- [x] Keep Redis usage isolated.
  - [x] Do not call Redis directly from HTTP handlers.
  - [x] Do not call Redis directly from workflow graph code.
  - [x] Route future publishing through the queue abstraction.

## Docker and Local Development

- [x] Update `docker-compose.yml`.
  - [x] Add a Redis service.
  - [x] Expose a local Redis port.
  - [x] Add a Redis healthcheck if practical.
  - [x] Avoid changing PostgreSQL service behavior.
- [x] Update local environment examples.
  - [x] Add `REDIS_ADDR`.
  - [x] Add queue stream name if configurable.
  - [x] Keep existing database settings unchanged.
- [x] Update Makefile commands if useful.
  - [x] Add `redis-up` only if it improves workflow.
  - [x] Keep `postgres-up` behavior stable.
  - [ ] Consider a combined local infrastructure command only if it is clearly useful.

## Integration Points

- [x] Decide what Day 6 should publish.
  - [ ] If publishing root task runs on workflow-run creation, publish only after the database transaction commits.
  - [x] If deferring workflow-run publishing to Day 9 scheduler work, keep Day 6 limited to queue implementation and tests.
  - [x] Record the tradeoff in `docs/ai-assisted-development.md`.
- [x] Avoid dual-write claims unless implemented.
  - [x] If database update plus Redis publish is not atomic yet, document the failure window.
  - [x] Do not claim transactional outbox guarantees until Day 11 implements them.
  - [x] Prefer explicit comments or docs over hidden reliability assumptions.

## Tests

- [x] Add unit tests for message serialization.
  - [x] Required fields are present.
  - [x] Field names are stable.
  - [x] Empty required IDs are rejected.
  - [x] Schema version is included.
- [x] Add unit tests for queue configuration.
  - [x] Defaults are applied.
  - [x] Missing required values fail clearly.
- [x] Add Redis integration tests if Redis is available.
  - [x] Publisher writes a message to the configured stream.
  - [x] Published Redis fields match the task message.
  - [x] Multiple messages are appended without overwriting.
  - [x] Tests skip clearly when Redis is not running.
- [ ] Add service integration tests only if Day 6 publishes from service code.
  - [ ] Workflow run creation queues root task runs if that behavior is implemented.
  - [ ] Publish failures are handled according to the documented Day 6 tradeoff.

## Manual Validation Steps

- [ ] Start local infrastructure:

```sh
make postgres-up
docker compose up -d redis
```

- [x] Run focused queue tests:

```sh
go test ./internal/queue -v
```

- [x] Run workflow tests:

```sh
go test ./internal/workflow -v
```

- [x] Run API integration tests if publishing is wired through the API:

```sh
go test ./internal/httpserver -v
```

- [x] Run the full test suite:

```sh
go test ./...
```

- [x] Run the full project check:

```sh
make check
```

- [ ] Inspect the Redis stream manually if Redis integration is implemented:

```sh
docker compose exec redis redis-cli XINFO STREAM goflow:tasks
```

## Documentation

- [x] Write an ADR comparing Redis Streams, Redis Pub/Sub and RabbitMQ.
- [x] Document queue delivery semantics in `docs/architecture.md` or a dedicated queue doc.
- [x] Document the Day 6 dual-write limitation if publishing happens outside a transactional outbox.
- [x] Update `docs/test-strategy.md` with queue unit and Redis integration tests.
- [x] Update `README.md` with Redis local setup and current queue capability after validation.
- [x] Record AI-suggested queue designs and rejected ideas in `docs/ai-assisted-development.md`.

## Deliverable

- [x] Redis is available in local development.
- [x] Queue abstraction skeleton is implemented.
- [x] Task message schema is implemented and tested.
- [x] Redis Streams publisher is implemented.
- [x] Redis publisher tests pass locally or skip clearly when Redis is unavailable.
- [x] Queue delivery semantics are documented.
- [x] Day 6 avoids worker execution and scheduler logic.
