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

## Day 9

### AI-assisted work

- Added `QueueRunnableTaskRuns` to move runnable pending task runs to `queued`
- Used one conditional PostgreSQL `UPDATE ... RETURNING` as the scheduler idempotency boundary
- Added a scheduler service that publishes one queue message for each task run returned by the update
- Wired workflow-run creation to trigger scheduler queueing after the database transaction commits
- Added tests for fan-out, fan-in, duplicate scheduler calls and scheduler publish behavior
- Added an integration test for `A -> B, C -> D` using real PostgreSQL and an in-memory queue

### Accepted suggestions

- Collapsed root and successor scheduling into the same runnable-task query
- Published only rows returned by the conditional database update
- Kept HTTP handlers out of scheduling decisions except for dependency injection
- Kept Redis out of the DAG E2E test and used the queue interface instead

### Rejected suggestions

- Rejected separate root/successor/readiness query methods before a caller needs them
- Rejected transactional outbox guarantees during Day 9
- Rejected retries, dead-letter handling and worker leases during Day 9
- Rejected a separate scheduler process before the in-process service boundary proves insufficient

### Validation performed

- Ran `go test ./internal/workflow ./internal/scheduler ./internal/httpserver`
- Ran `go test ./internal/worker -run TestSchedulerAndWorkerExecuteFanOutFanInDAG -v`
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

## Day 10

### AI-assisted work

- Added workflow-run `Idempotency-Key` handling for duplicate API requests
- Added request hashing so the same key can replay the same request but reject a different request
- Hardened duplicate Redis task messages by acknowledging already-owned or terminal task runs without creating another attempt
- Added structured logs for idempotency replay, idempotency conflict, duplicate task messages and scheduler no-op decisions
- Added focused tests for duplicate handling and log output

### Accepted suggestions

- Scoped workflow-run idempotency keys to one workflow ID
- Kept request hashes internal and out of logs
- Reused the existing PostgreSQL claim boundary for duplicate queue messages
- Logged idempotency and duplicate-delivery decisions through existing logging tools instead of adding metrics early

### Rejected suggestions

- Rejected a new logging abstraction for Day 10
- Rejected Prometheus metrics before Day 15
- Rejected exactly-once execution claims because Redis delivery remains at least once
- Rejected retry, dead-letter, lease recovery and outbox behavior during Day 10

### Validation performed

- Ran `go test ./internal/httpserver ./internal/scheduler ./internal/worker ./internal/workflow`
- Ran `go test ./...`

## Day 11

### AI-assisted work

- Added `task_outbox_events` and repository methods for claiming, publishing and retrying task queue events
- Moved runnable task-run queueing and outbox row creation into one PostgreSQL transaction
- Added an outbox dispatcher that publishes the existing Redis task message schema and records Redis message IDs
- Wired API and worker scheduling to run one dispatcher pass after durable scheduling commits
- Added worker-command periodic dispatcher passes for crash-before-publish recovery
- Added tests for outbox constraints, repository claiming, dispatcher retry behavior, API recovery and worker acknowledgement ordering

### Accepted suggestions

- Kept PostgreSQL as the reliability boundary and Redis as the delivery boundary
- Kept Redis task messages small and unchanged
- Used repeated dispatcher passes instead of a job framework
- Logged dispatch failures after commit without failing workflow-run creation

### Rejected suggestions

- Rejected retries, dead-letter handling, leases and metrics during Day 11
- Rejected a separate outbox process before the worker-side recovery loop proves insufficient
- Rejected embedding generic JSON payloads in the outbox while explicit task-message columns are enough

### Validation performed

- Ran `go test ./internal/worker ./internal/queue ./internal/scheduler`
- Ran `go test ./...`

## Day 12

### AI-assisted work

- Added task retry policy parsing for `max_attempts`, `initial_delay` and `multiplier`
- Added deterministic retry decision logic using attempt number, retryability and retry policy
- Extended attempt completion so retryable failures move task runs to `retry_wait` with `next_retry_at`
- Added due-retry queueing through the same transactional task outbox path used by DAG scheduling
- Added scheduler service support for dispatching due retry outbox events
- Updated worker execution so retry policy is validated before executor execution and retried queue messages create later attempt numbers
- Added tests for retry policy validation, retry decisions, retry-wait persistence, due retry queueing, second-attempt execution, exhausted retries and invalid retry configs

### Accepted suggestions

- Kept retry policy in task config instead of adding a retry-policy table
- Reused Go duration strings and the existing `task_runs.next_retry_at` column
- Reused the task outbox for retry queue messages instead of adding a retry queue
- Kept jitter out until synchronized retry load becomes a measured problem
- Treated unknown executor types and non-retryable executor failures as permanent failures

### Rejected suggestions

- Rejected leases, dead-letter inspection APIs and metrics during Day 12
- Rejected a separate retry scheduler binary while the scheduler service boundary is enough
- Rejected custom backoff abstractions beyond the current small policy parser and decider

### Validation performed

- Ran `go test ./internal/workflow -count=1`
- Ran `go test ./internal/worker -count=1`
- Ran `go test ./internal/scheduler -count=1`

## Day 13

### AI-assisted work

- Moved permanent task-run outcomes from final `failed` paths to terminal `dead_letter`
- Added idempotent workflow-run finalization after task outcomes persist
- Marked workflow runs `completed` when every task run completed and `failed` when any task run dead-lettered
- Wired worker completion to finalize before acknowledgement and to schedule successors only after completed task runs
- Added read APIs for workflow runs, task runs and task attempts with failure reasons
- Added tests for workflow finalization, dead-letter persistence, worker finalization ordering and inspection endpoints

### Accepted suggestions

- Kept failure detail on `task_attempts` instead of copying full failure history to task runs or workflow runs
- Reused existing `dead_letter` task-run status as the durable permanent-failure state
- Kept finalization in the workflow repository and called it from the worker after persistence
- Skipped pagination for inspection endpoints until result sizes prove it is needed

### Rejected suggestions

- Rejected manual dead-letter replay during Day 13
- Rejected worker leases and expired-lease recovery during Day 13
- Rejected dashboards, metrics and a separate failures table during Day 13

### Validation performed

- Ran `go test ./internal/workflow ./internal/httpserver ./internal/worker`

## Day 14

### AI-assisted work

- Added task-run lease fields: `locked_by`, `lease_expires_at` and `last_heartbeat_at`
- Updated task-run claiming to record worker ownership and lease expiry
- Added worker heartbeat extension while executors run
- Added expired running task-run recovery with `SKIP LOCKED`
- Marked open attempts failed with `lease_expired` during recovery
- Requeued recovered task runs with attempts remaining and moved exhausted task runs to `dead_letter`
- Reused the transactional task outbox for recovered queued work
- Rejected late worker completion unless the current attempt is running, the task run is still `running`, the worker owns `locked_by` and the lease is active
- Added `WORKER_LEASE_DURATION`, `WORKER_HEARTBEAT_INTERVAL` and `WORKER_RECOVERY_INTERVAL`
- Added tests for lease schema, claiming, heartbeat extension, recovery, stale completion rejection, worker no-ack behavior and config validation

### Accepted suggestions

- Kept lease ownership on `task_runs` instead of adding a separate lease table
- Used PostgreSQL row locks with `SKIP LOCKED` instead of leader election
- Reused the existing worker process and scheduler service instead of adding a recovery binary
- Kept recovered task dispatch on the existing task outbox path
- Required heartbeat interval to be shorter than lease duration

### Rejected suggestions

- Rejected leader election during Day 14
- Rejected manual dead-letter replay during Day 14
- Rejected claiming exactly-once external side effects
- Rejected dashboards and metrics during Day 14
- Rejected a separate due-retry loop in the worker command during Day 14

### Validation performed

- Ran `go test -count=1 ./...`

## Day 15

### AI-assisted work

- Added a small in-process metrics registry with Prometheus text rendering
- Exposed API process metrics at `GET /metrics`
- Wired workflow, scheduler, outbox and worker service counters
- Added repository-backed gauges for pending outbox rows, running task runs and
  expired running leases
- Tightened scheduler, outbox and worker logs around recovery and failure paths
- Added `cmd/loadcheck` for a repeatable local `A -> B, C -> D` workload
- Documented local failure-injection checks and operational inspection queries

### Accepted suggestions

- Kept metrics process-local and dependency-free for Day 15
- Used low-cardinality metric names without workflow, task or worker labels
- Reused PostgreSQL count queries for gauges instead of caching state in memory
- Kept load checking as one Go command with in-process workers
- Kept destructive or timing-sensitive failure checks manual in operations docs

### Modified suggestions

- Documented worker service metrics as injectable hooks because `cmd/worker`
  does not expose a worker HTTP metrics endpoint yet
- Used a dedicated load-check Redis stream by default so local queue data is
  not mixed with validation runs

### Rejected suggestions

- Rejected a Prometheus client dependency while stdlib rendering is enough
- Rejected Grafana dashboards during Day 15
- Rejected distributed tracing and high-cardinality metric labels
- Rejected a separate operations service or chaos framework

### Validation performed

- Ran `go test -count=1 ./...`
- Ran `go vet ./...`
- Ran `git diff --check`
- Ran `make check`
- Ran `go run ./cmd/loadcheck -runs 2 -workers 1 -timeout 20s`

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

## Day 5

### AI-assisted work

- Reviewed workflow dependency validation requirements before scheduler logic depends on them
- Added a workflow graph type with task IDs, outgoing adjacency lists, in-degree counts, root tasks and leaf tasks
- Added deterministic graph output by sorting successor lists, root task IDs and leaf task IDs
- Added cycle detection with Kahn's algorithm
- Added dependency creation validation so cyclic edges are rejected before insertion
- Added transaction-scoped PostgreSQL graph reads for dependency creation
- Added API error mapping for dependency cycles
- Added graph, service and API tests for Day 5 behavior

### Accepted suggestions

- Represented dependencies as predecessor-to-successor edges
- Kept graph construction and cycle detection in the `workflow` package, independent of HTTP handlers
- Used Kahn's algorithm instead of recursive DFS because it naturally uses in-degree counts and produces deterministic traversal when roots are sorted
- Allowed disconnected workflow components and tested that each component root is returned
- Kept PostgreSQL constraints as the first line of defense for same-workflow references, self-dependencies and duplicate dependency rows
- Used application-level graph validation for dependency cycles because PostgreSQL constraints do not express transitive graph acyclicity cleanly
- Wrapped workflow-row locking, graph reads, graph validation and dependency insertion in one transaction

### Modified suggestions

- Initially considered public repository methods for loading graph task IDs and dependency edges
- Moved those reads into private transaction-scoped repository helpers so graph validation and insertion share the same locked transaction
- Kept dependency validation on creation rather than adding a separate public service method, because the current API only needs immediate rejection of invalid edges
- Added API integration coverage for cycle errors while still allowing the tests to skip when local PostgreSQL is unavailable

### Rejected suggestions

- Rejected PostgreSQL recursive queries for Day 5 because the Go implementation is small, pure and easy to unit test
- Rejected keeping graph-loading reads in the public service repository interface after dependency creation became transactional
- Rejected relying only on self-dependency and foreign-key constraints because longer cycles require graph traversal
- Rejected adding Redis Streams, worker execution, retries, leases, outbox publishing or executor logic during Day 5

### Validation performed

- Ran `gofmt` on updated Go files
- Ran `go test ./internal/workflow -v`
- Ran `go test ./internal/httpserver -v`
- Ran `go test ./...`
- Verified graph tests cover single-task, linear, fan-out, fan-in, diamond, disconnected, invalid-reference and cycle cases
- Verified dependency cycle errors map to stable API error code `dependency_cycle`

## Day 6

### AI-assisted work

- Planned the queue boundary before wiring scheduler or worker behavior
- Compared Redis Streams, Redis Pub/Sub, RabbitMQ and Asynq for the initial queue layer
- Added a small `queue.TaskPublisher` interface for task publishing
- Added a task message schema with stable Redis field names
- Added Redis queue configuration and local Docker Compose support
- Implemented a Redis Streams publisher using `XADD`
- Added unit tests for queue config and message serialization
- Added a Redis integration test that skips clearly when Redis is unavailable

### Accepted suggestions

- Used Redis Streams as the initial backend because it supports durable stream entries, consumer groups for later workers and local Docker simplicity
- Used `github.com/redis/go-redis/v9` because it is a mature Go client with context-aware APIs
- Kept PostgreSQL as the source of truth and Redis as task delivery infrastructure
- Returned the Redis stream message ID from `PublishTask`
- Kept Redis client types out of the queue interface
- Added `REDIS_ADDR` and `QUEUE_STREAM_NAME` configuration

### Modified suggestions

- Considered Asynq, but deferred it because GoFlow still needs explicit workflow state transitions and queue semantics; direct Redis Streams keeps the Day 6 boundary small
- Kept publishing limited to the queue package instead of wiring workflow-run creation to Redis immediately
- Documented the future dual-write failure window instead of claiming transactional outbox behavior before it exists

### Rejected suggestions

- Rejected Redis Pub/Sub because messages disappear when consumers are unavailable
- Rejected RabbitMQ for Day 6 because it adds operational scope before worker consumption exists
- Rejected implementing worker consumers, acknowledgement, claiming, leases, retries, dead-letter handling or scheduler dependency release during Day 6
- Rejected putting full task configuration, secrets or large payloads into Redis messages

### Validation performed

- Ran `go mod tidy`
- Started Redis with `docker compose up -d redis`
- Ran `go test ./internal/queue -count=1 -v`
- Ran `go test ./internal/config -v`
- Ran `go test ./internal/workflow -v`
- Ran `go test ./internal/httpserver -v`
- Ran `go test ./...`
- Ran `make check`
- Verified the Redis integration test publishes multiple stream entries with the expected fields

## Day 7

### AI-assisted work

- Added worker queue configuration for worker ID, consumer group, block timeout and read count
- Added a Redis Streams consumer boundary separate from the publisher boundary
- Added parsing from Redis stream field maps back into validated `TaskMessage` values
- Implemented Redis consumer group setup with idempotent `XGROUP CREATE ... MKSTREAM`
- Implemented `XREADGROUP` consumption and `XACK` acknowledgement
- Added conditional PostgreSQL task-run claiming from `queued` to `running`
- Added a small worker service that coordinates receive, claim and acknowledgement
- Wired `cmd/worker` to load configuration, connect to PostgreSQL and Redis, and process one message at a time until shutdown

### Accepted suggestions

- Kept PostgreSQL as the source of truth and used Redis only for task delivery
- Used Redis consumer groups so multiple workers share one stream without every worker receiving every new message
- Used the worker ID as the Redis consumer name and as the identifier passed into task-run claims
- Acknowledged Redis messages only after the PostgreSQL claim step succeeds
- Left failed-claim messages pending so acknowledgement does not hide unprocessed work
- Kept executor behavior out of Day 7

### Modified suggestions

- Implemented single-message worker processing before optimizing batches, even though the Redis consumer config includes a read count
- Treated `ErrNoMessage` as a normal polling result in the worker loop instead of logging it as a failure
- Used a dedicated validation stream for manual Redis pending-entry checks so local default queue data is not disturbed

### Rejected suggestions

- Rejected acknowledging messages before PostgreSQL state changes succeed
- Rejected completing task runs during Day 7 because real executor behavior belongs to Day 8
- Rejected retry, dead-letter and pending-message recovery logic during Day 7
- Rejected lease or heartbeat columns before the recovery model is designed
- Rejected embedding task configuration or input payloads into Redis messages

### Validation performed

- Ran `go test ./internal/config`
- Ran `go test ./internal/queue -v`
- Ran `go test ./internal/workflow -v`
- Ran `go test ./internal/worker`
- Ran `go test ./cmd/worker -v`
- Ran `go test ./internal/httpserver -v`
- Ran `make check`
- Started Redis and PostgreSQL with Docker Compose
- Published one non-claimable validation message and verified it remained pending
- Published one claimable validation message and verified the task run moved to `running`
- Verified the acknowledged message did not remain in Redis pending entries

## Day 8

### AI-assisted work

- Added PostgreSQL repository methods to load task execution data, create task attempts and complete task attempts with task-run terminal state updates
- Added built-in worker executors for `sleep`, `log` and `random_fail`
- Added executor resolution by task executor type
- Updated the worker service to claim a task run, load execution data, create an attempt, run the executor, complete the attempt and acknowledge the Redis message
- Wired `cmd/worker` with the built-in executor registry
- Added repository integration tests for task-attempt creation, completion and task-run execution loading
- Added worker unit and integration tests for successful execution, executor failure and unknown executor types

### Accepted suggestions

- Kept PostgreSQL as the source of truth for task outcome and attempt history
- Acknowledged Redis messages only after completion is persisted
- Kept Redis messages small and loaded executor config/input from PostgreSQL
- Used simple built-in executors to prove the worker execution path before adding user-defined execution

### Modified suggestions

- Collapsed executor output to `map[string]any` so executors return the shape the repository already persists
- Kept executor lookup as a small map-backed registry instead of adding a larger plugin system
- Kept random failure deterministic in tests with a function field instead of a custom random-source abstraction

### Rejected suggestions

- Rejected retries, leases, pending-message recovery and dead-letter behavior during Day 8
- Rejected scheduler publishing during Day 8
- Rejected embedding full task configuration or input payloads into Redis messages
- Rejected a general-purpose executor plugin framework before external executor loading exists

### Validation performed

- Ran `go test ./...`
