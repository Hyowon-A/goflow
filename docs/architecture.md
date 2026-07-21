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
