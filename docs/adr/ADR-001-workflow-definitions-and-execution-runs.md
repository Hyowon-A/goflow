# ADR-001: Separate Workflow Definitions from Execution State

- **Status:** Accepted
- **Date:** 2026-07-20
- **Decision owners:** GoFlow project
- **Related components:** PostgreSQL schema, workflow API, scheduler, worker pool

## Context

GoFlow is a distributed workflow orchestration engine that executes workflows represented as directed acyclic graphs.

A workflow contains reusable task definitions and dependency relationships. The same workflow may be executed many times with different inputs and may produce different outputs, statuses, timings, failures and retry histories.

The database therefore needs to represent two different kinds of data:

1. Reusable workflow definitions
2. Mutable execution state

The schema must also support:

- Multiple executions of the same workflow
- One logical task run per task within a workflow run
- Multiple execution attempts for a logical task
- Task dependencies within the same workflow
- Runtime inputs and outputs
- Retry and failure history
- Database-level protection against cross-workflow relationships
- Future scheduling, recovery and observability features

## Problem

Storing workflow definitions and runtime state in the same records would create several problems.

A task definition describes what should be executed, while a task run describes what happened during one particular execution. These records have different lifecycles and different data requirements.

For example:

- A task definition may contain an executor type, configuration and input schema.
- A task run may contain actual input, output, status and timestamps.
- A task attempt may contain one attempt number, failure reason and execution duration.

If these concepts are combined:

- Re-running a workflow may duplicate definitions.
- Runtime updates may accidentally modify reusable configuration.
- Retry history may overwrite logical task state.
- Queries for workflow structure and execution history become harder to understand.
- Constraints between workflows, tasks and runs become less explicit.

The schema must therefore clearly separate definitions, logical executions and physical attempts.

## Decision

GoFlow will use separate tables for workflow definitions, logical execution state and individual execution attempts.

### Definition tables

The reusable workflow structure will be stored in:

- `workflows`
- `tasks`
- `task_dependencies`

A workflow is a reusable definition.

A task belongs to one workflow and contains configuration describing what a worker should execute.

A task dependency represents a directed edge between two tasks in the same workflow.

Definition-level JSON fields will describe contracts or configuration rather than runtime values.

Examples include:

- `input_schema`
- `output_schema`
- `config`
- `executor_type`

### Execution tables

Runtime state will be stored in:

- `workflow_runs`
- `task_runs`
- `task_attempts`

A `workflow_run` represents one execution of a workflow definition.

A `task_run` represents one logical execution of a task within a workflow run.

A `task_attempt` represents one physical attempt to execute that logical task.

This means one task run may have several task attempts due to retries.

### Runtime inputs and outputs

Actual input and output values will belong to run records rather than definition records.

The intended ownership is:

- `workflow_runs.input`: input supplied when starting a workflow
- `workflow_runs.output`: final workflow result
- `task_runs.input`: input supplied to a logical task execution
- `task_runs.output`: final successful output of that logical task

Definition tables may store schemas describing the expected structure of these values, but they will not store execution-specific values.

### Task run uniqueness

Only one logical task run may exist for each task inside a workflow run.

This will be enforced using a unique constraint on: (workflow_run_id, task_id)

Retries will not create additional `task_runs` rows. They will create additional `task_attempts` rows.

### Attempt history

`task_attempts` will preserve individual execution history.

Each attempt will have:

- `task_run_id`
- `attempt_number`
- attempt status
- start timestamp
- completion timestamp
- failure reason when applicable

The combination below will be unique: (task_run_id, attempt_number)

This preserves failure history and avoids overwriting the reason from an earlier attempt.

`task_runs.attempt_count` will store the aggregate number of attempts for the logical task.

### Execution statuses

Separate status types will be used because workflow runs, logical task runs and task attempts have different lifecycles.

Workflow runs may use statuses such as:

- `pending`
- `running`
- `completed`
- `failed`

Logical task runs may use statuses such as:

- `pending`
- `queued`
- `running`
- `retry_wait`
- `completed`
- `failed`
- `dead_letter`

Task attempts may use statuses such as:

- `running`
- `completed`
- `failed`

An individual attempt may fail while the logical task remains in `retry_wait`. Therefore, attempt failure does not necessarily mean the task run or workflow run has permanently failed.

### Failure information

Detailed execution failures will be stored on `task_attempts`.

The final logical task outcome will be represented by `task_runs.status`.

Workflow failure details will normally be derived from failed task runs and their attempts rather than duplicated as a workflow-level failure string.

A workflow-level failure field may be introduced later only for failures that are not attributable to an individual task, such as scheduler initialisation failure or a workflow-wide timeout.

### Same-workflow integrity

The database must prevent the following invalid relationships:

- A task depending on a task from another workflow
- A task run referencing a workflow run from one workflow and a task from another workflow

Application validation alone will not be considered sufficient because alternative code paths, scripts or future services could bypass it.

GoFlow will include `workflow_id` in `task_dependencies` and `task_runs` and use composite foreign keys.

The `tasks` table will expose the candidate key:

```text
(workflow_id, id)
```

The `workflow_runs` table will expose the candidate key:

```text
(workflow_id, id)
```

The dependency table will enforce:

```text
(workflow_id, predecessor_task_id)
    references tasks(workflow_id, id)

(workflow_id, successor_task_id)
    references tasks(workflow_id, id)
```

The task-run table will enforce:

```text
(workflow_id, workflow_run_id)
    references workflow_runs(workflow_id, id)

(workflow_id, task_id)
    references tasks(workflow_id, id)
```

The duplicated `workflow_id` is intentional. It allows the database to enforce an important domain invariant using declarative constraints.

Application-level validation will still be used to return clearer API errors before attempting the insert.

### Dependency constraints

A task dependency will use the pair below as its primary key:

```text
(predecessor_task_id, successor_task_id)
```

This prevents duplicate edges.

A check constraint will prevent direct self-dependencies:

```text
predecessor_task_id <> successor_task_id
```

Longer cycles cannot be prevented by this check and will be handled through DAG cycle validation in the application.

### Timestamps

Execution timestamps will use PostgreSQL timezone-aware timestamps.

The schema will use `timestamptz` for fields such as:

- `created_at`
- `started_at`
- `completed_at`
- `next_retry_at`

`created_at` will be required and default to the current time.

`started_at` and `completed_at` will remain nullable until the relevant transition occurs.

Checks will ensure that completion timestamps are not earlier than start timestamps.

## Alternatives Considered

### Store definitions and execution state together

This option would use the same workflow and task rows for both reusable configuration and mutable execution state.

It was rejected because it would make repeated executions difficult to model, mix data with different lifecycles and increase the risk of modifying definitions during execution.

### Create a new task-run row for every retry

This option would treat each retry as a separate `task_runs` row.

It was rejected because `task_runs` is intended to represent the logical execution of one task within one workflow run.

A separate `task_attempts` table provides clearer separation between logical state and physical attempts.

### Store only an attempt count and overwrite failures

This option would keep retries entirely within `task_runs` and replace the previous failure reason after every attempt.

It was rejected because GoFlow requires retry history for debugging, reliability demonstrations and later observability features.

### Enforce workflow consistency only in application code

This option would validate workflow IDs in the API before creating dependencies or task runs.

It was rejected as the sole protection because database integrity would depend on every caller correctly implementing the same validation.

Application validation will still be used alongside database constraints.

### Use database triggers for workflow consistency

This option would use triggers to load related workflow IDs and reject mismatches.

It was rejected because the same rule can be represented using composite foreign keys.

Composite keys are more explicit, easier to inspect and less likely to hide important integrity behaviour.

Triggers may be considered later for rules that cannot be represented declaratively.

### Avoid duplicating `workflow_id`

This option would keep `workflow_id` only in parent records and rely on joins to determine whether related records belong to the same workflow.

It was rejected because normal foreign keys would only verify that each referenced row exists. They would not enforce that the rows belong to the same workflow.

The duplication is accepted as intentional denormalisation for integrity.

### Store input and output values in definition tables

This option would store `input` and `output` directly on `workflows` and `tasks`.

It was rejected because actual values differ between executions.

Definition tables may store input and output schemas, while run tables store actual values.

## Consequences

### Positive consequences

- Workflow definitions can be reused across many executions.
- Runtime updates do not modify reusable configuration.
- The scheduler can query workflow structure separately from execution state.
- Retry history is preserved without duplicating logical task runs.
- Cross-workflow dependencies and task runs are rejected by PostgreSQL.
- Runtime inputs and outputs have clear ownership.
- The schema supports future retry, DLQ, recovery and observability features.
- Failure analysis can identify both the logical task and the specific failed attempt.

### Negative consequences

- The schema contains more tables than a combined model.
- `workflow_id` is duplicated in some relationship tables.
- Composite foreign keys make migrations and DBML diagrams slightly more complex.
- Creating task runs and dependencies requires supplying the correct workflow ID.
- Keeping `task_runs.attempt_count` consistent with `task_attempts` requires careful transactional updates.
- Workflow failure summaries require querying task-run and attempt data.

### Risks

The duplicated `workflow_id` values could become inconsistent if composite constraints are omitted or implemented incorrectly.

The migration must therefore create the required candidate keys and composite foreign keys, not only standalone foreign keys.

The `attempt_count` field may become inconsistent with the number of attempt rows if updates are not transactional.

The application should increment the count and insert the new attempt within the same transaction.

A self-dependency check does not prevent longer cycles. Cycle detection must still be implemented before accepting a workflow DAG.

## Validation

The decision will be validated with database and application tests covering:

- Creating multiple workflow runs from one workflow definition
- Creating one task run per task within a workflow run
- Rejecting duplicate task runs
- Creating multiple attempts for one task run
- Rejecting duplicate attempt numbers
- Rejecting a dependency between tasks in different workflows
- Rejecting a task run whose task belongs to a different workflow
- Rejecting direct self-dependencies
- Preserving earlier failure reasons after retries
- Ensuring completed timestamps are not earlier than started timestamps
- Confirming runtime inputs and outputs remain independent between workflow runs

## AI-Assisted Review Record

AI was used to review the initial schema and identify missing relationships, constraints and execution-history requirements.

### Accepted suggestions

- Separate workflow definitions from workflow runs.
- Separate task definitions from task runs.
- Add `task_attempts` to preserve retry and failure history.
- Store runtime input and output on run tables.
- Use separate status enums for workflows, logical tasks and attempts.
- Use composite foreign keys to prevent cross-workflow relationships.
- Use timezone-aware timestamps.
- Add unique constraints for logical task runs and attempt numbers.

### Modified suggestions

The initial suggestion allowed retry information to remain only on `task_runs`.

This was modified to retain `attempt_count` on `task_runs` while also adding `task_attempts` for detailed history.

This keeps aggregate task state easy to query while preserving attempt-level evidence.

### Rejected suggestions

Using database triggers as the primary method for enforcing same-workflow relationships was rejected because composite foreign keys express the invariant more clearly.

Application-only validation was rejected as the sole integrity mechanism because it could be bypassed.

## Final Decision Summary

GoFlow will model workflow definitions, logical executions and physical attempts as separate concepts.

The database will use declarative constraints, including composite foreign keys, to enforce that tasks, dependencies, workflow runs and task runs remain within the correct workflow.

Runtime values will be stored on run records, while reusable configuration and schemas will remain on definition records.

Retries will create attempt records while preserving one logical task run per workflow run and task.
