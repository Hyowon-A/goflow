-- Manual verification script for the constraints in
-- migrations/001_initial_schema.up.sql.
--
-- Run after `make postgres-up`:
--
--   psql "$DATABASE_URL" -f tests/integration/schema_constraints.sql
--
-- Everything below runs inside a single transaction that is rolled back at
-- the very end, so nothing here is ever persisted. ON_ERROR_ROLLBACK makes
-- psql create an implicit savepoint before each statement and roll back to
-- it on error, so Section 2's expected failures don't abort the session.

\set ON_ERROR_ROLLBACK on
\pset pager off

BEGIN;

-- ============================================================
-- Section 1: valid inserts
-- ============================================================

INSERT INTO workflows (id, name) VALUES
    ('00000000-0000-0000-0000-0000000000a0', 'etl-pipeline');

INSERT INTO tasks (id, workflow_id, name, executor_type) VALUES
    ('00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000a0', 'extract', 'http'),
    ('00000000-0000-0000-0000-0000000000a2', '00000000-0000-0000-0000-0000000000a0', 'transform', 'http'),
    ('00000000-0000-0000-0000-0000000000a3', '00000000-0000-0000-0000-0000000000a0', 'load', 'http');

-- Multiple dependencies within the same workflow.
INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id) VALUES
    ('00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000a2'),
    ('00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000a2', '00000000-0000-0000-0000-0000000000a3');

-- One workflow run...
INSERT INTO workflow_runs (id, workflow_id) VALUES
    ('00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000a0');

-- ...and a second, independent run of the SAME workflow definition.
INSERT INTO workflow_runs (id, workflow_id) VALUES
    ('00000000-0000-0000-0000-0000000000c2', '00000000-0000-0000-0000-0000000000a0');

-- One task run per task, all under the first workflow run.
INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id) VALUES
    ('00000000-0000-0000-0000-0000000000d1', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000a1'),
    ('00000000-0000-0000-0000-0000000000d2', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000a2'),
    ('00000000-0000-0000-0000-0000000000d3', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000a3');

-- Multiple attempts for one task run: first attempt fails, second succeeds.
INSERT INTO task_attempts (id, task_run_id, attempt_number, status, failure_reason) VALUES
    ('00000000-0000-0000-0000-0000000000e1', '00000000-0000-0000-0000-0000000000d3', 1, 'failed', 'connection timeout');

INSERT INTO task_attempts (id, task_run_id, attempt_number, status) VALUES
    ('00000000-0000-0000-0000-0000000000e2', '00000000-0000-0000-0000-0000000000d3', 2, 'completed');

-- A second workflow. Reusing the "extract" task name here proves task names
-- are unique within a workflow, not globally unique across all workflows.
INSERT INTO workflows (id, name) VALUES
    ('00000000-0000-0000-0000-0000000000b0', 'other-workflow');

INSERT INTO tasks (id, workflow_id, name, executor_type) VALUES
    ('00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-0000000000b0', 'extract', 'http');

-- ============================================================
-- Section 2: invalid inserts -- every statement below is expected to fail
-- ============================================================

-- expect: ERROR duplicate key value violates unique constraint "uq_tasks_workflow_name"
INSERT INTO tasks (id, workflow_id, name, executor_type) VALUES
    ('00000000-0000-0000-0000-0000000000a9', '00000000-0000-0000-0000-0000000000a0', 'extract', 'http');

-- expect: ERROR new row for relation "task_dependencies" violates check constraint "chk_task_dependencies_not_self"
INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id) VALUES
    ('00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000a1');

-- expect: ERROR duplicate key value violates unique constraint "pk_task_dependencies"
INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id) VALUES
    ('00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000a2');

-- expect: ERROR insert or update on table "task_dependencies" violates foreign key constraint "fk_task_dependencies_successor"
INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id) VALUES
    ('00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000b1');

-- expect: ERROR insert or update on table "task_runs" violates foreign key constraint "fk_task_runs_task"
INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id) VALUES
    ('00000000-0000-0000-0000-0000000000d9', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000b1');

-- expect: ERROR duplicate key value violates unique constraint "uq_task_runs_workflow_run_task"
INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id) VALUES
    ('00000000-0000-0000-0000-0000000000d8', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000a1');

-- expect: ERROR duplicate key value violates unique constraint "uq_task_attempts_task_run_number"
INSERT INTO task_attempts (id, task_run_id, attempt_number, status) VALUES
    ('00000000-0000-0000-0000-0000000000e8', '00000000-0000-0000-0000-0000000000d3', 1, 'completed');

-- expect: ERROR new row for relation "task_runs" violates check constraint "chk_task_runs_attempt_count"
-- (uses workflow run c2, which has no task runs yet, so only the count check can fire)
INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, attempt_count) VALUES
    ('00000000-0000-0000-0000-0000000000d6', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c2', '00000000-0000-0000-0000-0000000000a1', -1);

-- expect: ERROR new row for relation "task_attempts" violates check constraint "chk_task_attempts_number_positive"
INSERT INTO task_attempts (id, task_run_id, attempt_number, status) VALUES
    ('00000000-0000-0000-0000-0000000000e9', '00000000-0000-0000-0000-0000000000d3', 0, 'running');

-- expect: ERROR new row for relation "workflow_runs" violates check constraint "chk_workflow_runs_timestamp_order"
INSERT INTO workflow_runs (id, workflow_id, started_at, completed_at) VALUES
    ('00000000-0000-0000-0000-0000000000c5', '00000000-0000-0000-0000-0000000000a0', '2026-07-20T12:00:00Z', '2026-07-20T11:00:00Z');

-- expect: ERROR new row for relation "task_runs" violates check constraint "chk_task_runs_timestamp_order"
-- (uses workflow run c2 + task a2, an unused pair, so only the timestamp check can fire)
INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, started_at, completed_at) VALUES
    ('00000000-0000-0000-0000-0000000000d7', '00000000-0000-0000-0000-0000000000a0', '00000000-0000-0000-0000-0000000000c2', '00000000-0000-0000-0000-0000000000a2', '2026-07-20T12:00:00Z', '2026-07-20T11:00:00Z');

-- expect: ERROR new row for relation "task_attempts" violates check constraint "chk_task_attempts_timestamp_order"
INSERT INTO task_attempts (id, task_run_id, attempt_number, status, started_at, completed_at) VALUES
    ('00000000-0000-0000-0000-0000000000ea', '00000000-0000-0000-0000-0000000000d3', 3, 'running', '2026-07-20T12:00:00Z', '2026-07-20T11:00:00Z');

-- expect: ERROR new row for relation "task_attempts" violates check constraint "chk_task_attempts_failure_reason"
INSERT INTO task_attempts (id, task_run_id, attempt_number, status, failure_reason) VALUES
    ('00000000-0000-0000-0000-0000000000eb', '00000000-0000-0000-0000-0000000000d3', 4, 'completed', 'should not be allowed');

ROLLBACK;
