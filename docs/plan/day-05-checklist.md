# Day 5 Checklist: Graph Basics

Goal: validate workflow task dependencies as a DAG before scheduler logic depends on them.

## Scope

- [x] Keep Day 5 focused on graph correctness only.
  - [x] Do not add Redis Streams yet.
  - [x] Do not implement workers yet.
  - [x] Do not implement retries, leases, outbox publishing or executor logic.
- [x] Use the current schema as the source of truth.
  - [x] Read task nodes from `tasks`.
  - [x] Read dependency edges from `task_dependencies`.
  - [x] Leave runtime state in `workflow_runs`, `task_runs` and `task_attempts`.
- [x] Define the intended Day 5 behavior before coding.
  - [x] A workflow graph is valid only when it is acyclic.
  - [x] A non-empty workflow must have at least one root task.
  - [x] Multiple root tasks are valid.
  - [x] Multiple leaf tasks are valid.
  - [x] Disconnected components are allowed only if you explicitly decide they are valid.

## Implementation

- [x] Define a workflow graph type in the `workflow` package.
  - [x] Include all task IDs, even tasks with no dependencies.
  - [x] Store outgoing edges as an adjacency list keyed by predecessor task ID.
  - [x] Store or calculate in-degree counts keyed by task ID.
  - [x] Represent edges as predecessor-to-successor dependencies.
- [x] Add a graph builder function.
  - [x] Accept task IDs and dependency edges as inputs.
  - [x] Initialize empty adjacency and in-degree entries for every task.
  - [x] Reject dependency edges that reference an unknown task ID.
  - [x] Keep output deterministic by sorting root task IDs and leaf task IDs.
- [x] Calculate graph facts.
  - [x] Detect root tasks with zero in-degree.
  - [x] Detect leaf tasks with no outgoing edges.
  - [x] Skip explicit total task count and edge count because current tests and logging do not need them.
- [x] Add cycle detection with Kahn's algorithm.
  - [x] Start with all zero in-degree tasks.
  - [x] Pop roots in deterministic order.
  - [x] Decrement successors' in-degree as predecessors are visited.
  - [x] Count visited tasks.
  - [x] Reject the graph if visited task count is less than total task count.
- [x] Return stable workflow errors.
  - [x] Add `ErrDependencyCycle` in `internal/workflow/errors.go`.
  - [x] Reuse `ErrInvalidTaskReference` for graph edges pointing outside the workflow.
  - [x] Keep `ErrDuplicateDependency` and `ErrSelfDependency` as separate errors.
- [x] Keep graph validation independent from HTTP handlers.
  - [x] Put graph construction in `internal/workflow`.
  - [x] Keep HTTP handlers responsible only for request decoding, UUID validation and error mapping.
- [x] Decide where graph validation should run:
  - [x] Prefer validating when creating each dependency so invalid graphs are rejected early.
  - [x] Do not validate when starting a workflow run on Day 5.
  - [x] Record the tradeoff in `docs/ai-assisted-development.md` after implementation.

## Graph Invariants

- [x] Every dependency edge must reference tasks in the same workflow.
- [x] A task cannot depend on itself.
- [x] A valid workflow graph must be acyclic.
- [x] A non-empty workflow must have at least one root task.
- [x] Every task in a valid graph must be reachable from at least one root task.
- [x] Multiple root tasks are allowed.
- [x] Multiple leaf tasks are allowed.
- [x] Duplicate dependency edges remain invalid.

## Algorithm

- [x] Start with adjacency-list construction from task IDs and dependencies.
- [x] Use Kahn's algorithm for cycle detection and in-degree calculation.
- [x] Count visited tasks during topological traversal.
- [x] Reject the graph if visited task count is less than total task count.
- [x] Return root task IDs in deterministic order.
- [x] Avoid recursive DFS unless there is a clear reason.
- [x] Avoid relying on PostgreSQL recursive queries for Day 5 unless the Go
  implementation proves inadequate.
- [x] Keep the algorithm pure enough to unit test without PostgreSQL.

## Service and Repository Work

- [x] Keep graph-loading reads out of the public `workflow.Repository` interface.
  - [x] Use private transaction-scoped helpers to load all task IDs for a workflow.
  - [x] Use private transaction-scoped helpers to load all dependencies for a workflow.
  - [x] Keep graph validation and dependency insert in one repository transaction.
- [x] Implement the repository reads in `internal/workflow/postgres_repository.go`.
  - [x] Query `tasks` by `workflow_id`.
  - [x] Query `task_dependencies` by `workflow_id`.
  - [x] Order rows deterministically by creation order or ID.
  - [x] Use `context.Context` and parameterised SQL.
- [x] Update the dependency creation path.
  - [x] Trim and validate workflow ID.
  - [x] Trim and validate task IDs.
  - [x] Load the current graph inside the dependency creation path.
  - [x] Check whether adding the requested edge would create a cycle.
  - [x] Return `ErrDependencyCycle` before persisting the edge if possible.
  - [x] Preserve existing behavior for self-dependency, duplicate dependency and invalid task references.
- [x] Decide transaction behavior for dependency creation.
  - [x] If validation happens before insert, consider the race where another dependency is inserted concurrently.
  - [x] If avoiding that race on Day 5, wrap graph load, validation and insert in one transaction.
  - [x] Do not defer full concurrency handling for dependency creation; serialize by locking the workflow row.
- [x] Do not create `task_attempts` in Day 5.
  - [x] Attempts still belong to worker execution, not graph validation.

## Tests

- [x] Add unit tests for graph construction in `internal/workflow`.
  - [x] Test a single-task workflow has one root, one leaf and no cycle.
  - [x] Test a linear graph: `A -> B -> C`.
  - [x] Test a fan-out graph: `A -> B, C`.
  - [x] Test a fan-in graph: `A, B -> C`.
  - [x] Test a diamond graph: `A -> B, C -> D`.
  - [x] Test multiple independent roots.
  - [x] Test a dependency edge referencing an unknown task is rejected.
  - [x] Test deterministic root ordering.
  - [x] Test deterministic leaf ordering.
- [x] Add cycle tests.
  - [x] Test a direct self-cycle is rejected.
  - [x] Test a two-node cycle is rejected.
  - [x] Test a longer cycle is rejected.
  - [x] Test a graph with a cycle in one component and an acyclic separate component is rejected.
- [x] Decide and test disconnected graph behavior.
  - [x] If disconnected components are allowed, test that each component root is returned.
  - [x] Do not add a disconnected-component error because disconnected components are allowed.
- [x] Add service or integration tests for dependency creation.
  - [x] Creating `A -> B` succeeds.
  - [x] Creating `B -> A` after `A -> B` returns `ErrDependencyCycle`.
  - [x] Creating `C -> A` after `A -> B -> C` returns `ErrDependencyCycle`.
  - [x] Duplicate dependency behavior still returns `ErrDuplicateDependency`.
  - [x] Missing or cross-workflow task references still return `ErrInvalidTaskReference`.
- [x] Add API integration coverage if dependency validation is exposed through the API.
  - [x] Cyclic dependency creation returns `400 Bad Request`.
  - [x] Response error code is stable, such as `dependency_cycle`.
  - [x] Response includes the request ID like existing workflow API errors.

## API and Persistence

- [x] Add repository query support for loading workflow tasks and dependencies.
- [x] Do not add a separate service method for validating a workflow graph; dependency creation validates transactionally.
- [x] Decide whether `CreateDependency` should reject cycles immediately.
- [x] If cycle rejection happens on dependency creation, wrap the dependency insert and graph validation in a transaction.
- [x] Map cycle errors to a stable HTTP response.
- [x] Keep endpoint paths unchanged.
- [x] Keep request and response DTOs unchanged unless the API needs to expose graph validation results.
- [x] Keep PostgreSQL constraints as the first line of defense for duplicate edges, self-dependencies and cross-workflow references.
- [x] Use application-level graph validation for invariants PostgreSQL does not express cleanly, especially cycles.

## Manual Validation Steps

- [x] Start PostgreSQL:

```sh
make postgres-up
```

- [x] Run focused workflow tests:

```sh
go test ./internal/workflow -v
```

- [x] Run API integration tests:

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

## Documentation

- [x] Add a DAG diagram to README or architecture docs.
- [x] Document graph invariants in `docs/architecture.md`.
- [x] Record AI-suggested graph algorithms and rejected ideas in `docs/ai-assisted-development.md`.
- [x] Note why graph validation is separate from PostgreSQL constraints.
- [x] Update `docs/test-strategy.md` with graph unit tests and dependency API integration tests.
- [x] Update `README.md` current capabilities only after the code is implemented and validated.

## Deliverable

- [x] Workflow graph builder implemented.
- [x] Cycle detection implemented.
- [x] Root task detection implemented.
- [x] Leaf task detection implemented.
- [x] Unit tests pass locally.
- [x] API or service path rejects cyclic dependencies.
- [x] Graph invariants documented.
- [x] Existing Day 3 and Day 4 behavior still passes.
