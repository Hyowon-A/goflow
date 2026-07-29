# ADR-002: Use Redis Streams for the Initial Task Queue

- **Status:** Accepted
- **Date:** 2026-07-27
- **Decision owners:** GoFlow project
- **Related components:** Queue package, scheduler, worker pool, local development environment

## Context

GoFlow needs a queue boundary so scheduler logic can later publish runnable
task runs and worker processes can consume them. PostgreSQL remains the source
of truth for workflow definitions, workflow runs, task runs, task attempts and
status transitions.

The queue should carry delivery messages, not authoritative workflow state. A
message should contain stable identifiers that let a worker load the current
state from PostgreSQL.

The initial implementation needs to support local development and automated
tests without adding more operational complexity than the project currently
needs.

## Decision

GoFlow will use Redis Streams as the initial queue backend.

The application will expose a small Go publishing interface:

```go
type TaskPublisher interface {
	PublishTask(ctx context.Context, message TaskMessage) (string, error)
}
```

The Redis implementation will append task messages with `XADD` to a configured
stream and return the Redis stream message ID.

Day 7 adds a separate consumer interface for worker delivery:

```go
type TaskConsumer interface {
	ReceiveTask(ctx context.Context) (ReceivedTaskMessage, error)
	AckTask(ctx context.Context, messageID string) error
	Close() error
}
```

The Redis consumer uses consumer groups with `XREADGROUP` and acknowledges with
`XACK` only after PostgreSQL task-run claiming succeeds.

Task messages will include:

- `schema_version`
- `workflow_id`
- `workflow_run_id`
- `task_id`
- `task_run_id`

Task messages will not include full task configuration, secrets or large
payloads. Workers will use IDs from the message to load authoritative state from
PostgreSQL.

## Options Considered

### Redis Streams

Redis Streams provides durable stream entries, blocking reads and consumer-group
support for worker pools. It is easy to run locally with Docker Compose and has
mature Go client support through `github.com/redis/go-redis/v9`.

Redis Streams still requires careful consumer implementation. At-least-once
delivery means workers must be idempotent and acknowledgements must be handled
explicitly.

### Redis Pub/Sub

Redis Pub/Sub is simple, but it does not retain messages for unavailable
consumers. That behavior does not fit workflow task delivery because workers may
restart or be temporarily offline.

### RabbitMQ

RabbitMQ provides mature queueing and acknowledgement semantics, but it adds
more infrastructure and operational surface than GoFlow needs for the first
queue milestone. It remains a possible future backend if routing or stronger
broker-level features become necessary.

### Asynq

Asynq is a useful Go task queue built on Redis, but it owns more of the task
execution model. GoFlow needs explicit workflow state transitions, dependency
scheduling and future outbox behavior, so direct Redis Streams keeps the queue
boundary smaller and clearer at this stage.

## Consequences

Positive consequences:

- Local development only needs PostgreSQL and Redis.
- Queue publishing can be tested against a real Redis instance.
- Worker consumers can use Redis consumer groups.
- The service interface is not coupled to Redis client types.

Tradeoffs:

- Delivery is at least once, not exactly once.
- Consumers must be idempotent.
- Publishing to Redis after updating PostgreSQL is not atomic without a
  transactional outbox.
- Operational Redis failures must be surfaced and retried by later scheduler
  code.

## Reliability Boundary

Day 6 implements queue publishing only. Day 7 adds worker consumption,
message acknowledgement after task-run claiming and a minimal worker loop. It
does not implement task execution, leases, retries, dead-letter handling,
scheduler dependency release or transactional outbox publishing.

When workflow-run creation is later wired to queue publishing, GoFlow must avoid
claiming atomic database update plus Redis publish guarantees until a
transactional outbox exists.
