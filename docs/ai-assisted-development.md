# AI-Assisted Development Log

## Purpose

AI is used to support design review, implementation, testing and documentation.

All architectural decisions, code changes and reliability claims are reviewed and validated manually.

## Day 1

### AI-assisted work

- Reviewed possible Go project structures
- Explained unfamiliar Go syntax and standard-library behaviour
- Helped identify local PostgreSQL and port conflicts
- Suggested health and readiness endpoints
- Reviewed configuration, database connection and graceful shutdown code

### Decisions retained

- Separate API and worker entry points
- PostgreSQL connection pool using `pgx`
- Chi router for HTTP endpoints
- Environment-based configuration
- Graceful shutdown using OS signals
- Docker Compose for local PostgreSQL

### Decisions modified

- PostgreSQL was moved from port `5432` to `5433` because a local PostgreSQL instance was already using port `5432`
- The API port was changed from `8080` because another process was using it

### Validation performed

- Ran `docker compose config`
- Confirmed PostgreSQL container health
- Ran `gofmt`
- Ran `go vet`
- Ran `go test ./...`
- Verified database connectivity during API startup

## Day 2

### AI-assisted work

- Reviewed the initial database schema design against the workflow execution model
- Identified that reusable definitions, logical runs and retry attempts have different lifecycles
- Suggested constraint-proving tests for PostgreSQL integrity rules
- Drafted ADR-001 to record the schema separation and integrity decisions
- Reviewed the final migration for redundant constraints and avoidable deferrable clauses

### Accepted suggestions

- Added `task_attempts` so every retry has its own attempt number, status, timestamps and failure reason
- Moved runtime `input` and `output` values to `workflow_runs` and `task_runs`
- Kept reusable schemas and executor configuration on `workflows` and `tasks`
- Used separate status enums for workflow runs, logical task runs and task attempts
- Used composite foreign keys to enforce same-workflow dependencies and task runs
- Added named constraints, timestamp-order checks, attempt-number checks and uniqueness checks
- Added Go integration tests that prove the main PostgreSQL constraints against a real database

### Modified suggestions

- Retained `task_runs.attempt_count` as a query-friendly aggregate while making `task_attempts` the source of detailed retry history
- Duplicated `workflow_id` in `task_dependencies` and `task_runs` only where it enables declarative same-workflow integrity
- Kept task names unique within a workflow, while allowing the same task name across different workflow definitions
- Kept the SQL validation script optional because the Go integration tests are the primary automated proof

### Rejected suggestions

- Rejected storing retry history only on `task_runs` because each failed attempt would overwrite useful failure evidence
- Rejected storing runtime inputs and outputs on definition tables because those values vary by execution
- Rejected one shared status enum because workflows, logical task runs and attempts have different lifecycle states
- Rejected trigger-based same-workflow validation because composite foreign keys express the invariant directly
- Rejected standalone foreign keys that duplicated the composite foreign keys for dependencies and task runs
- Rejected unnecessary `DEFERRABLE` clauses because immediate constraint checks are sufficient for the current insert model

### Validation performed

- Ran `go test ./...`
- Applied `migrations/001_initial_schema.up.sql` to a temporary PostgreSQL database
- Ran `migrations/001_initial_schema.down.sql` successfully
- Reapplied `migrations/001_initial_schema.up.sql` after rollback
- Ran the schema constraint integration tests against the temporary PostgreSQL database
- Created `migrations/001_initial_schema.up.sql`
- Created `migrations/001_initial_schema.down.sql`
- Created `internal/database/schema_constraints_test.go`
- Created `tests/integration/schema_constraints.sql`
- Created `docs/adr/ADR-001-workflow-definitions-and-execution-runs.md`
