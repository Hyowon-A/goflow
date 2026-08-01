# GoFlow Architecture

## Purpose

GoFlow is a general-purpose distributed workflow orchestration engine written in Go.

It will execute dependency-based tasks across multiple workers while maintaining durable workflow state and supporting failure recovery.

## Planned Architecture

```text
Client
  |
  v
GoFlow API
  |
  +--- PostgreSQL
  |      - workflow definitions
  |      - workflow runs
  |      - task runs
  |      - retry history
  |      - outbox events
  |
  +--- Redis Streams
           |
           v
       Worker Pool
```

## Database Schema

PostgreSQL stores both reusable workflow definitions and runtime execution
state. The schema keeps these concepts separate so workflow structure can be
reused across many runs without overwriting execution history.

![GoFlow database schema](images/database-schema.png)

Definition tables:

- `workflows`
- `tasks`
- `task_dependencies`

Execution tables:

- `workflow_runs`
- `task_runs`
- `task_attempts`

`workflow_runs` represents one execution of a workflow definition. `task_runs`
represents one logical execution of a task inside that workflow run.
`task_attempts` records each physical retry attempt for a task run, including
attempt number, status, timestamps and failure reason.

Runtime input and output values live on run tables. Definition-level schemas
and task executor configuration live on definition tables.

Composite foreign keys intentionally include `workflow_id` on dependency and
task-run relationships. This lets PostgreSQL reject cross-workflow dependencies
and task runs whose workflow run and task belong to different workflows without
using triggers.

## Workflow Graph Validation

Workflow definitions are modeled as directed acyclic graphs. Tasks are graph
nodes and `task_dependencies` rows are directed edges from predecessor task to
successor task.

```text
extract
  |
  v
transform
  |
  v
load
```

Fan-out, fan-in, multiple roots, multiple leaves and disconnected components are
valid. Disconnected components are treated as separate runnable branches of the
same workflow definition.

```text
extract        enrich
  |              |
  v              v
transform      publish

aggregate ----> notify
```

Graph invariants:

- Every dependency edge must reference tasks in the same workflow.
- A task cannot depend on itself.
- Duplicate dependency edges are invalid.
- A valid workflow graph must be acyclic.
- A non-empty workflow must have at least one root task.
- Every task in a valid graph is reachable from at least one root task in its
  component.

The workflow package builds graphs from task IDs and dependency edges using an
adjacency list plus in-degree counts. Roots and leaves are sorted so later
scheduler logic can consume deterministic output. Cycle detection uses Kahn's
algorithm: start from zero in-degree tasks, visit successors while decrementing
their in-degree and reject the graph if the visited count is smaller than the
task count.

Dependency creation validates the graph before inserting the new edge. The
PostgreSQL repository wraps workflow-row locking, graph reads, graph validation
and dependency insertion in one transaction. Locking the workflow row with
`FOR UPDATE` serializes dependency creation for a workflow, avoiding the race
where two concurrent requests each validate against a stale graph.

PostgreSQL remains the first line of defense for invariants expressible with
constraints, such as same-workflow references, self-dependencies and duplicate
edges. Application-level graph validation covers the cycle invariant because it
requires checking the transitive dependency graph.

## Execution State Automata

GoFlow models three separate execution lifecycles. They share the same generic
transition-checking helper internally, but each lifecycle has its own typed
status values and transition table.

Workflow runs represent the aggregate outcome of one workflow execution:

![Workflow run state automaton](images/workflow-run-automaton.svg)

Task runs represent logical task execution inside a workflow run. A task run may
fail, wait for retry, return to the queue, or move to dead-letter state after it
can no longer be retried.

![Task run state automaton](images/task-run-automaton.svg)

Task attempts represent one physical execution attempt. Attempts start when a
worker begins execution and then end once.

![Task attempt state automaton](images/task-attempt-automaton.svg)

Terminal states have no outgoing transitions:

- Workflow runs: `completed`, `failed`
- Task runs: `completed`, `dead_letter`
- Task attempts: `completed`, `failed`

The workflow package rejects unknown states, same-state transitions and invalid
state changes before repository or worker code relies on them. The Go constants
are tested against the PostgreSQL enum definitions in
`migrations/001_initial_schema.up.sql`.

## API Request Flow

The API layer keeps HTTP concerns separate from workflow business logic and
PostgreSQL persistence.

```text
HTTP client
  |
  v
chi router
  |
  +-- request ID middleware
  |     - reads or generates X-Request-ID
  |
  +-- logging middleware
  |     - logs method, path, status, duration and request_id
  |
  v
HTTP handler
  |
  +-- decode JSON request body
  +-- validate required fields and UUID path params
  +-- convert request DTO into workflow input
  |
  v
workflow service
  |
  +-- trim and validate application inputs
  +-- enforce workflow-level rules
  +-- call repository interface
  |
  v
PostgreSQL repository
  |
  +-- execute parameterised SQL with context.Context
  +-- use transactions where multiple records must be created together
  +-- translate PostgreSQL constraint errors into workflow errors
  |
  v
PostgreSQL
```

Example for `POST /workflows/{workflowID}/tasks`:

```text
createTask handler
  -> validate workflowID is a UUID
  -> decode createTaskRequest
  -> call workflow.Service.CreateTask
  -> call PostgresRepository.CreateTask
  -> INSERT INTO tasks
  -> return taskResponse with 201 Created
```

Handlers own transport details such as JSON, HTTP status codes, request IDs and
response DTOs. The workflow service owns application validation and use-case
coordination. The repository owns SQL and converts database constraint failures
such as duplicate task names or missing workflows into meaningful workflow
errors.

Implemented workflow API endpoints:

| Method | Path | Responsibility |
| --- | --- | --- |
| `POST` | `/workflows` | Create a reusable workflow definition. |
| `POST` | `/workflows/{workflowID}/tasks` | Create a task definition inside one workflow. |
| `POST` | `/workflows/{workflowID}/dependencies` | Create a dependency edge between two tasks. |
| `POST` | `/workflows/{workflowID}/runs` | Create a workflow run and pending task runs transactionally. |

Workflow-run creation accepts an optional `Idempotency-Key` header. The key is
scoped to the workflow ID for this endpoint. Repeating the same key with the
same request body returns the original workflow run; reusing the key with a
different body returns `409 Conflict`. Keys are retained indefinitely until a
cleanup policy exists.

Idempotency decisions are logged without request payloads or request hashes.
Successful replays emit `idempotency_key_reused` with request, workflow and
workflow-run IDs. Conflicts emit `idempotency_key_conflict` with request and
workflow IDs.

The API validates malformed JSON, missing required fields and invalid UUID path
parameters before calling the workflow service. Repository constraint mappings
turn database failures into stable API errors, including duplicate task names,
duplicate dependencies, missing workflows and invalid task references.
Dependency cycle validation returns a stable `dependency_cycle` error with
`400 Bad Request`.

## Task Queue

GoFlow uses Redis Streams as the initial task-delivery boundary. PostgreSQL
remains the authoritative store for workflow definitions, workflow runs, task
runs, task attempts, statuses, inputs and outputs. Redis messages are delivery
records that carry enough identifiers for worker code to load the authoritative
state from PostgreSQL.

The queue package exposes separate publishing and consuming boundaries:

```go
type TaskPublisher interface {
	PublishTask(ctx context.Context, message TaskMessage) (string, error)
}

type TaskConsumer interface {
	ReceiveTask(ctx context.Context) (ReceivedTaskMessage, error)
	AckTask(ctx context.Context, messageID string) error
	Close() error
}
```

The Redis implementation owns one Redis client per publisher and appends
messages with `XADD` to the configured stream. The returned value is the Redis
stream message ID.

Worker consumers use Redis consumer groups. A worker creates the configured
group idempotently with `XGROUP CREATE ... MKSTREAM`, then reads new messages
with `XREADGROUP` using the worker ID as the Redis consumer name. Empty reads
return a stable no-message result so workers can poll while still respecting
shutdown.

```text
                     Redis Stream: goflow:tasks
        +------------------------------------------------+
        | 178...-0  workflow_run_id=... task_run_id=A    |
        | 179...-0  workflow_run_id=... task_run_id=B    |
        | 180...-0  workflow_run_id=... task_run_id=C    |
        +------------------------------------------------+
                             |
                             v
                Consumer Group: goflow-workers
                             |
             +---------------+---------------+
             |                               |
             v                               v
       worker-1 XREADGROUP             worker-2 XREADGROUP
             |                               |
             v                               v
       receives task_run A             receives task_run B
```

Consumer groups prevent every worker from receiving every new message. Within a
group, Redis assigns each new stream entry to one consumer. After delivery, the
message is pending for that consumer until GoFlow acknowledges it.

Task message fields are stable and explicit:

| Field | Purpose |
| --- | --- |
| `schema_version` | Message schema version. Currently `1`. |
| `workflow_id` | Workflow definition ID. |
| `workflow_run_id` | Workflow run ID. |
| `task_id` | Task definition ID. |
| `task_run_id` | Logical task run ID. |

The queue intentionally does not embed full task configuration, input payloads
or secrets. Workers use the IDs in the message to load the current task state
and task input from PostgreSQL.

Delivery semantics are at least once. Workers must be idempotent because Redis
may redeliver messages after consumer failures or acknowledgement gaps. The
current worker flow processes one message at a time:

1. Receive one Redis message.
2. Parse and validate the task message fields.
3. Claim the referenced task run in PostgreSQL by moving it from `queued` to
   `running`.
4. Load the task definition, executor type, task config and task-run input.
5. Create a running `task_attempt`.
6. Resolve and run the configured executor.
7. Complete the task attempt and task run as `completed` or `failed`.
8. Acknowledge the Redis message only after completion is persisted.

```text
Redis stream entry
  |
  | XREADGROUP GROUP goflow-workers worker-1 STREAMS goflow:tasks >
  v
Redis pending entry
  |
  | UPDATE task_runs
  | SET status = 'running'
  | WHERE id = task_run_id
  |   AND status = 'queued'
  v
PostgreSQL claim result
  |
  +-- claim succeeds
  |      |
  |      v
  |    Load task + create attempt
  |      |
  |      v
  |    Executor.Execute
  |      |
  |      v
  |    CompleteTaskAttempt
  |      |
  |      v
  |    XACK goflow:tasks goflow-workers message_id
  |      |
  |      v
  |    Redis pending entry removed
  |
  +-- claim fails
         |
         v
       Load current task-run status
         |
         +-- running/completed/failed/dead_letter: XACK duplicate
         |
         +-- missing/other error/pending/retry_wait: no XACK
```

If the claim fails, the worker checks PostgreSQL before acknowledging. Messages
for task runs already `running`, `completed`, `failed` or `dead_letter` are
harmless duplicates and are acknowledged without creating another attempt.
Unknown task runs, ambiguous lookup failures and non-ready states remain
pending. If completion persistence fails, the worker also leaves the message
pending so the task is not acknowledged before PostgreSQL records the outcome.
Acknowledged duplicate messages emit `duplicate_task_message` with workflow,
task, Redis message, worker, status and reason fields.

Built-in executors are intentionally small:

| Executor type | Behavior |
| --- | --- |
| `sleep` | Sleeps for a configured Go duration and returns `{"status":"completed"}`. |
| `log` | Logs a message from task config or task-run input and returns it as output. |
| `random_fail` | Fails based on `failure_probability`; useful for failure-path testing. |

Pending-message recovery, leases, retries and dead-letter handling remain
future work.

## DAG Scheduling

The scheduler is a small coordinator between PostgreSQL workflow state and the
queue publisher. It asks the workflow repository to move runnable task runs
from `pending` to `queued`, then publishes one Redis task message for each row
returned by that update.

```text
workflow run created or task completed
  |
  v
UPDATE task_runs
SET status = 'queued'
WHERE status = 'pending'
  AND all predecessor task runs are completed
RETURNING task_run identifiers
  |
  v
Publish one Redis message per returned task run
```

Root tasks are handled by the same query because they have no predecessor rows.
Fan-in tasks stay `pending` until every predecessor task run is `completed`.
Repeated scheduler calls are safe because already queued, running or terminal
task runs are ignored by the conditional update.
Scheduler passes that find no runnable task runs emit `scheduler_noop` with the
workflow run ID and reason.

The API triggers root scheduling after workflow-run creation commits. Worker
completion can call the same scheduler path to release newly ready successors.
Until the Day 11 outbox exists, GoFlow still has a dual-write gap: a task run
may be moved to `queued` in PostgreSQL and then fail to publish to Redis.
