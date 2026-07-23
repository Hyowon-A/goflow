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

## Day 3

### AI-assisted work

- Compared Go HTTP handler, service and repository layering with a Spring Boot controller/service/repository structure
- Reviewed request and response DTO placement for workflow API handlers
- Designed strict JSON decoding for malformed bodies, empty bodies, unsupported content types and unknown fields
- Added the workflow API implementation for creating workflows, tasks, dependencies and workflow runs
- Added repository methods using `context.Context`, parameterised SQL and PostgreSQL transactions
- Added request IDs, consistent JSON error responses and structured request logging
- Added API integration tests against real PostgreSQL for workflow, task, dependency and run creation

### Accepted suggestions

- Kept HTTP request and response DTOs in the `httpserver` package
- Added a `workflow` package for application types, service logic, domain errors and PostgreSQL persistence
- Used repository interfaces so the service layer is not tied directly to HTTP handlers
- Used `json.Decoder.DisallowUnknownFields` to reject unknown JSON fields
- Created workflow runs transactionally with one pending `task_runs` row per task definition
- Returned `409 Conflict` for duplicate task names and duplicate dependencies
- Validated UUID path parameters before calling PostgreSQL
- Logged each request with method, path, status, duration and request ID

### Modified suggestions

- Started with the real PostgreSQL repository instead of a mock repository because Day 3 validation depends on database constraints
- Kept body DTOs small and endpoint-specific instead of creating broad shared DTO packages
- Deferred OpenAPI/Swagger generation until the API surface is more stable
- Deferred full dependency cycle detection because the project plan assigns DAG validation to a later day

### Rejected suggestions

- Rejected returning raw PostgreSQL errors from handlers because clients need stable API error codes
- Rejected passing invalid UUID path parameters into repository methods because that turns validation errors into database failures
- Rejected creating task attempts during workflow-run creation because attempts represent physical execution tries, not initial logical task state
- Rejected generated handlers from an OpenAPI spec for now because it would add tooling before the API contract has settled

### Validation performed

- Ran `go test ./internal/httpserver -v`
- Ran `go test ./...`
- Verified successful workflow creation
- Verified invalid workflow request handling
- Verified successful task creation
- Verified duplicate task name rejection
- Verified successful dependency creation
- Verified self-dependency, duplicate dependency and invalid task reference handling
- Verified workflow-run creation creates one pending task run per task
- Verified workflow-run creation does not create task attempts
- Verified request IDs appear in responses and structured logs

## Day 4

### AI-assisted work

- Reviewed the state model for workflow runs, logical task runs and task attempts
- Added typed Go status constants that align with the PostgreSQL enum values
- Added transition tables for all three lifecycles
- Added transition validators for known-state checks, invalid transitions and terminal states
- Added table-driven tests for valid, invalid, same-state, terminal and unknown-state transitions
- Added enum-alignment tests that compare Go constants with the initial PostgreSQL migration
- Refactored duplicated transition validation into a private generic state-machine helper

### Accepted suggestions

- Kept workflow-run, task-run and task-attempt statuses as separate Go types
- Used `completed` instead of `success` because the database schema already uses `completed`
- Made `completed`, `failed` and `dead_letter` terminal only in the lifecycles where they apply
- Used private generic code for shared transition mechanics while preserving lifecycle-specific public functions
- Added distinct unknown-status errors for workflow runs, task runs and task attempts
- Kept transition logic inside the `workflow` package instead of HTTP handlers or PostgreSQL repository code

### Modified suggestions

- Started with pure in-memory transition validation and deferred repository update methods until worker and scheduler code need them
- Kept the generic helper unexported so callers still use domain-specific functions such as `ValidateTaskRunTransition`
- Treated empty transition maps as the source of truth for terminal states instead of maintaining separate terminal-state lists

### Rejected suggestions

- Rejected one shared status type because the three lifecycles allow different states and transitions
- Rejected `SUCCESS` as the terminal task-run name because it would drift from the existing `completed` database enum
- Rejected allowing same-state transitions by default because retries and duplicate messages should be handled explicitly
- Rejected exposing a generic public state-machine API because it would make callers responsible for lifecycle selection

### Validation performed

- Ran `gofmt` on the workflow state-machine files
- Ran `go test ./internal/workflow`
- Ran `go test ./...`
- Verified task-run transition tests cover valid, invalid, terminal, same-state and unknown-state cases
- Verified workflow-run transition tests cover valid, invalid, terminal, same-state and unknown-state cases
- Verified task-attempt transition tests cover valid, invalid, terminal, same-state and unknown-state cases
- Verified Go status constants align with `workflow_run_status`, `task_run_status` and `task_attempt_status` in `migrations/001_initial_schema.up.sql`
