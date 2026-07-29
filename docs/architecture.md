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
records that carry enough identifiers for later worker code to load the
authoritative state from PostgreSQL.

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
or secrets. Workers should use the IDs in the message to load the current task
state and payload references from PostgreSQL.

Delivery semantics are at least once. Workers must be idempotent because Redis
may redeliver messages after consumer failures or acknowledgement gaps. The Day
7 worker flow is intentionally narrow:

1. Receive one Redis message.
2. Parse and validate the task message fields.
3. Claim the referenced task run in PostgreSQL by moving it from `queued` to
   `running`.
4. Acknowledge the Redis message only after the claim succeeds.

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
  |    XACK goflow:tasks goflow-workers message_id
  |      |
  |      v
  |    Redis pending entry removed
  |
  +-- claim fails
         |
         v
       no XACK; Redis pending entry remains
```

If the claim fails because the task run is missing or no longer queued, the
worker leaves the Redis message pending. Pending-message recovery, leases,
dead-letter handling, retry behavior, executor logic and downstream scheduling
remain future work.

The current workflow service does not publish root task runs when creating a
workflow run. When publishing is wired into workflow-run creation, publishing
must happen after the database transaction commits unless a transactional
outbox is introduced. Until an outbox exists, GoFlow must not claim atomic
database update plus Redis publish guarantees.
