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
