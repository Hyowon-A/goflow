package database

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultTestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	migrationPath          = "../../migrations/001_initial_schema.up.sql"
	idempotencyPath        = "../../migrations/002_workflow_run_idempotency.up.sql"
	outboxPath             = "../../migrations/003_task_outbox_events.up.sql"
	leasePath              = "../../migrations/004_task_run_lease.up.sql"
)

const (
	workflowAID = "00000000-0000-0000-0000-0000000000a0"
	workflowBID = "00000000-0000-0000-0000-0000000000b0"

	taskExtractID   = "00000000-0000-0000-0000-0000000000a1"
	taskTransformID = "00000000-0000-0000-0000-0000000000a2"
	taskLoadID      = "00000000-0000-0000-0000-0000000000a3"

	otherWorkflowTaskID = "00000000-0000-0000-0000-0000000000b1"
)

var (
	poolOnce   sync.Once
	sharedPool *pgxpool.Pool
	poolErr    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPool != nil {
		sharedPool.Close()
	}
	os.Exit(code)
}

// testPool returns a shared connection pool for the live schema-constraint
// tests, bootstrapping the schema on first use. Tests skip instead of
// failing when no database is reachable, since this repo has no CI wiring
// and running one requires `make postgres-up`.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	poolOnce.Do(func() {
		sharedPool, poolErr = setupTestDatabase(context.Background())
	})

	if poolErr != nil {
		t.Skipf("postgres not available (run `make postgres-up`): %v", poolErr)
	}

	return sharedPool
}

func setupTestDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultTestDatabaseURL
	}

	pool, err := Connect(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	var schemaApplied bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'workflows'
		)
	`).Scan(&schemaApplied)
	if err != nil {
		pool.Close()
		return nil, err
	}

	if !schemaApplied {
		migrationSQL, err := os.ReadFile(migrationPath)
		if err != nil {
			pool.Close()
			return nil, err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if err := ensureIdempotencySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureOutboxSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureLeaseSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func ensureIdempotencySchema(ctx context.Context, pool *pgxpool.Pool) error {
	var columnExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'workflow_runs'
				AND column_name = 'idempotency_key'
		)
	`).Scan(&columnExists)
	if err != nil {
		return err
	}
	if !columnExists {
		migrationSQL, err := os.ReadFile(idempotencyPath)
		if err != nil {
			return err
		}
		_, err = pool.Exec(ctx, string(migrationSQL))
		return err
	}

	if _, err := pool.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_runs_idempotency
			ON workflow_runs (workflow_id, idempotency_key)
			WHERE idempotency_key IS NOT NULL
	`); err != nil {
		return err
	}

	var constraintExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = 'chk_workflow_runs_idempotency_hash'
		)
	`).Scan(&constraintExists)
	if err != nil || constraintExists {
		return err
	}
	_, err = pool.Exec(ctx, `
		ALTER TABLE workflow_runs
			ADD CONSTRAINT chk_workflow_runs_idempotency_hash
			CHECK (idempotency_key IS NULL OR request_hash IS NOT NULL)
	`)
	return err
}

func ensureOutboxSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var tableExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public'
				AND table_name = 'task_outbox_events'
		)
	`).Scan(&tableExists)
	if err != nil || tableExists {
		if err != nil {
			return err
		}
		return ensureOutboxClaimSchema(ctx, pool)
	}

	migrationSQL, err := os.ReadFile(outboxPath)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(migrationSQL))
	return err
}

func ensureOutboxClaimSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		ALTER TABLE task_outbox_events
			DROP CONSTRAINT IF EXISTS chk_task_outbox_events_status;
		ALTER TABLE task_outbox_events
			ADD CONSTRAINT chk_task_outbox_events_status
			CHECK (status IN ('pending', 'publishing', 'published'));
		DROP INDEX IF EXISTS uq_task_outbox_events_unpublished_task_run;
		CREATE UNIQUE INDEX uq_task_outbox_events_unpublished_task_run
			ON task_outbox_events (task_run_id, event_type)
			WHERE status <> 'published';
	`)
	return err
}

func ensureLeaseSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var columnExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'task_runs'
				AND column_name = 'lease_expires_at'
		)
	`).Scan(&columnExists)
	if err != nil {
		return err
	}
	if !columnExists {
		migrationSQL, err := os.ReadFile(leasePath)
		if err != nil {
			return err
		}
		_, err = pool.Exec(ctx, string(migrationSQL))
		return err
	}

	_, err = pool.Exec(ctx, `
		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
					AND table_name = 'task_runs'
					AND column_name = 'last_hearbeat_at'
			) AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
					AND table_name = 'task_runs'
					AND column_name = 'last_heartbeat_at'
			) THEN
				ALTER TABLE task_runs RENAME COLUMN last_hearbeat_at TO last_heartbeat_at;
			END IF;
		END $$;
		ALTER TABLE task_runs
			ADD COLUMN IF NOT EXISTS locked_by TEXT,
			ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
			ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
		DROP INDEX IF EXISTS idx_task_runs_expired;
		CREATE INDEX idx_task_runs_expired
			ON task_runs (lease_expires_at, id)
			WHERE status = 'running'
				AND lease_expires_at IS NOT NULL;
	`)
	return err
}

// beginTx opens a transaction that is always rolled back at the end of the
// test, so fixtures inserted here never persist in the shared dev database.
func beginTx(t *testing.T, pool *pgxpool.Pool) (context.Context, pgx.Tx) {
	t.Helper()

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback(ctx)
	})

	return ctx, tx
}

// expectPgError asserts that err is a Postgres error raised by the named
// constraint, proving the specific constraint fired rather than any error.
func expectPgError(t *testing.T, err error, wantConstraint string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected a violation of constraint %q, got no error", wantConstraint)
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError for constraint %q, got %T: %v", wantConstraint, err, err)
	}

	if pgErr.ConstraintName != wantConstraint {
		t.Fatalf("expected constraint %q, got %q (sqlstate %s): %v", wantConstraint, pgErr.ConstraintName, pgErr.Code, err)
	}
}

func strPtr(s string) *string { return &s }

func insertWorkflow(ctx context.Context, tx pgx.Tx, id, name string) error {
	_, err := tx.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, id, name)
	return err
}

func insertTask(ctx context.Context, tx pgx.Tx, id, workflowID, name, executorType string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type)
		VALUES ($1, $2, $3, $4)
	`, id, workflowID, name, executorType)
	return err
}

func insertDependency(ctx context.Context, tx pgx.Tx, workflowID, predecessorID, successorID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id)
		VALUES ($1, $2, $3)
	`, workflowID, predecessorID, successorID)
	return err
}

func insertWorkflowRun(ctx context.Context, tx pgx.Tx, id, workflowID string) error {
	_, err := tx.Exec(ctx, `INSERT INTO workflow_runs (id, workflow_id) VALUES ($1, $2)`, id, workflowID)
	return err
}

func insertTaskRun(ctx context.Context, tx pgx.Tx, id, workflowID, workflowRunID, taskID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id)
		VALUES ($1, $2, $3, $4)
	`, id, workflowID, workflowRunID, taskID)
	return err
}

func insertTaskAttempt(ctx context.Context, tx pgx.Tx, id, taskRunID string, attemptNumber int, status string, failureReason *string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_attempts (id, task_run_id, attempt_number, status, failure_reason)
		VALUES ($1, $2, $3, $4, $5)
	`, id, taskRunID, attemptNumber, status, failureReason)
	return err
}

func insertTaskOutboxEvent(ctx context.Context, tx pgx.Tx, id, workflowID, workflowRunID, taskID, taskRunID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id)
		VALUES ($1, $2, $3, $4, $5)
	`, id, workflowID, workflowRunID, taskID, taskRunID)
	return err
}

// seedSingleTaskRun inserts one workflow, one task, one workflow run and one
// task run, returning the task run id. Used by tests that only care about
// task_attempts constraints and don't need a full workflow graph.
func seedSingleTaskRun(t *testing.T, ctx context.Context, tx pgx.Tx) string {
	t.Helper()

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	workflowRunID := "00000000-0000-0000-0000-0000000000c9"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	taskRunID := "00000000-0000-0000-0000-0000000000d0"
	if err := insertTaskRun(ctx, tx, taskRunID, workflowAID, workflowRunID, taskExtractID); err != nil {
		t.Fatalf("insert task run: %v", err)
	}

	return taskRunID
}

// ------------------------------------------------------------------------
// Valid inserts
// ------------------------------------------------------------------------

func TestValidInserts_WorkflowLifecycle(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "etl-pipeline"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task extract: %v", err)
	}
	if err := insertTask(ctx, tx, taskTransformID, workflowAID, "transform", "http"); err != nil {
		t.Fatalf("insert task transform: %v", err)
	}
	if err := insertTask(ctx, tx, taskLoadID, workflowAID, "load", "http"); err != nil {
		t.Fatalf("insert task load: %v", err)
	}

	if err := insertDependency(ctx, tx, workflowAID, taskExtractID, taskTransformID); err != nil {
		t.Fatalf("insert dependency extract->transform: %v", err)
	}
	if err := insertDependency(ctx, tx, workflowAID, taskTransformID, taskLoadID); err != nil {
		t.Fatalf("insert dependency transform->load: %v", err)
	}

	workflowRunID := "00000000-0000-0000-0000-0000000000c1"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	taskRunExtractID := "00000000-0000-0000-0000-0000000000d1"
	taskRunTransformID := "00000000-0000-0000-0000-0000000000d2"
	taskRunLoadID := "00000000-0000-0000-0000-0000000000d3"

	if err := insertTaskRun(ctx, tx, taskRunExtractID, workflowAID, workflowRunID, taskExtractID); err != nil {
		t.Fatalf("insert task run extract: %v", err)
	}
	if err := insertTaskRun(ctx, tx, taskRunTransformID, workflowAID, workflowRunID, taskTransformID); err != nil {
		t.Fatalf("insert task run transform: %v", err)
	}
	if err := insertTaskRun(ctx, tx, taskRunLoadID, workflowAID, workflowRunID, taskLoadID); err != nil {
		t.Fatalf("insert task run load: %v", err)
	}

	// Multiple attempts for the same logical task run: first fails, second succeeds.
	if err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e1", taskRunLoadID, 1, "failed", strPtr("connection timeout")); err != nil {
		t.Fatalf("insert task attempt 1: %v", err)
	}
	if err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e2", taskRunLoadID, 2, "completed", nil); err != nil {
		t.Fatalf("insert task attempt 2: %v", err)
	}

	var taskCount, dependencyCount, taskRunCount, attemptCount int

	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE workflow_id = $1`, workflowAID).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 3 {
		t.Errorf("expected 3 tasks, got %d", taskCount)
	}

	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_dependencies WHERE workflow_id = $1`, workflowAID).Scan(&dependencyCount); err != nil {
		t.Fatalf("count dependencies: %v", err)
	}
	if dependencyCount != 2 {
		t.Errorf("expected 2 dependencies, got %d", dependencyCount)
	}

	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_runs WHERE workflow_run_id = $1`, workflowRunID).Scan(&taskRunCount); err != nil {
		t.Fatalf("count task runs: %v", err)
	}
	if taskRunCount != 3 {
		t.Errorf("expected 3 task runs, got %d", taskRunCount)
	}

	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE task_run_id = $1`, taskRunLoadID).Scan(&attemptCount); err != nil {
		t.Fatalf("count task attempts: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("expected 2 attempts for the load task run, got %d", attemptCount)
	}
}

func TestValidInserts_MultipleWorkflowRunsForSameDefinition(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "reusable-workflow"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}

	runOneID := "00000000-0000-0000-0000-0000000000f1"
	runTwoID := "00000000-0000-0000-0000-0000000000f2"

	if err := insertWorkflowRun(ctx, tx, runOneID, workflowAID); err != nil {
		t.Fatalf("insert first workflow run: %v", err)
	}
	if err := insertWorkflowRun(ctx, tx, runTwoID, workflowAID); err != nil {
		t.Fatalf("insert second workflow run: %v", err)
	}

	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM workflow_runs WHERE workflow_id = $1`, workflowAID).Scan(&count); err != nil {
		t.Fatalf("count workflow runs: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 workflow runs for the same workflow definition, got %d", count)
	}
}

func TestWorkflowRunIdempotencyKeyUniquePerWorkflow(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf-a"); err != nil {
		t.Fatalf("insert workflow a: %v", err)
	}
	if err := insertWorkflow(ctx, tx, workflowBID, "wf-b"); err != nil {
		t.Fatalf("insert workflow b: %v", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4)
	`, "00000000-0000-0000-0000-000000000101", workflowAID, "same-key", "hash-a")
	if err != nil {
		t.Fatalf("insert first idempotent workflow run: %v", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4)
	`, "00000000-0000-0000-0000-000000000102", workflowBID, "same-key", "hash-b")
	if err != nil {
		t.Fatalf("expected same idempotency key to be reusable for another workflow: %v", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4)
	`, "00000000-0000-0000-0000-000000000103", workflowAID, "same-key", "hash-c")
	expectPgError(t, err, "uq_workflow_runs_idempotency")
}

func TestWorkflowRunIdempotencyKeyRequiresRequestHash(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf-a"); err != nil {
		t.Fatalf("insert workflow a: %v", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, idempotency_key)
		VALUES ($1, $2, $3)
	`, "00000000-0000-0000-0000-000000000104", workflowAID, "key-without-hash")
	expectPgError(t, err, "chk_workflow_runs_idempotency_hash")
}

func TestTaskOutboxEventsRejectDuplicatePendingEventForTaskRun(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)
	taskRunID := seedSingleTaskRun(t, ctx, tx)

	if err := insertTaskOutboxEvent(ctx, tx, "00000000-0000-0000-0000-000000000301", workflowAID, "00000000-0000-0000-0000-0000000000c9", taskExtractID, taskRunID); err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}

	err := insertTaskOutboxEvent(ctx, tx, "00000000-0000-0000-0000-000000000302", workflowAID, "00000000-0000-0000-0000-0000000000c9", taskExtractID, taskRunID)
	expectPgError(t, err, "uq_task_outbox_events_unpublished_task_run")
}

func TestTaskOutboxEventsPendingScanIsDeterministicAndIgnoresPublishedRows(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert extract task: %v", err)
	}
	if err := insertTask(ctx, tx, taskTransformID, workflowAID, "transform", "http"); err != nil {
		t.Fatalf("insert transform task: %v", err)
	}

	workflowRunID := "00000000-0000-0000-0000-000000000309"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	taskRunExtractID := "00000000-0000-0000-0000-000000000310"
	taskRunTransformID := "00000000-0000-0000-0000-000000000311"
	if err := insertTaskRun(ctx, tx, taskRunExtractID, workflowAID, workflowRunID, taskExtractID); err != nil {
		t.Fatalf("insert extract task run: %v", err)
	}
	if err := insertTaskRun(ctx, tx, taskRunTransformID, workflowAID, workflowRunID, taskTransformID); err != nil {
		t.Fatalf("insert transform task run: %v", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO task_outbox_events (
			id, workflow_id, workflow_run_id, task_id, task_run_id, status, redis_message_id, published_at, created_at
		)
		VALUES ($1, $2, $3, $4, $5, 'published', 'redis-1-0', now(), '2026-01-01T00:00:00Z')
	`, "00000000-0000-0000-0000-000000000312", workflowAID, workflowRunID, taskExtractID, taskRunExtractID)
	if err != nil {
		t.Fatalf("insert published outbox event: %v", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id, created_at)
		VALUES
			($1, $2, $3, $4, $5, '2026-01-01T00:00:02Z'),
			($6, $2, $3, $7, $8, '2026-01-01T00:00:01Z')
	`, "00000000-0000-0000-0000-000000000313", workflowAID, workflowRunID, taskExtractID, taskRunExtractID, "00000000-0000-0000-0000-000000000314", taskTransformID, taskRunTransformID)
	if err != nil {
		t.Fatalf("insert pending outbox events: %v", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT id
		FROM task_outbox_events
		WHERE status = 'pending'
		ORDER BY created_at, id
	`)
	if err != nil {
		t.Fatalf("scan pending outbox events: %v", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan outbox event id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox event ids: %v", err)
	}

	if len(ids) != 2 || ids[0] != "00000000-0000-0000-0000-000000000314" || ids[1] != "00000000-0000-0000-0000-000000000313" {
		t.Fatalf("unexpected pending outbox order: %#v", ids)
	}
}

func TestValidInserts_SameTaskNameAcrossDifferentWorkflows(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf-a"); err != nil {
		t.Fatalf("insert workflow a: %v", err)
	}
	if err := insertWorkflow(ctx, tx, workflowBID, "wf-b"); err != nil {
		t.Fatalf("insert workflow b: %v", err)
	}

	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task in workflow a: %v", err)
	}
	if err := insertTask(ctx, tx, otherWorkflowTaskID, workflowBID, "extract", "http"); err != nil {
		t.Fatalf("insert task with same name in workflow b: %v", err)
	}

	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE name = 'extract'`).Scan(&count); err != nil {
		t.Fatalf("count same-name tasks: %v", err)
	}
	if count != 2 {
		t.Errorf("expected same task name to be allowed across workflows, got %d rows", count)
	}
}

func TestTaskRunLeaseSchema(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for _, column := range []string{"locked_by", "lease_expires_at", "last_heartbeat_at"} {
		var nullable string
		err := pool.QueryRow(ctx, `
			SELECT is_nullable
			FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'task_runs'
				AND column_name = $1
		`, column).Scan(&nullable)
		if err != nil {
			t.Fatalf("load lease column %s: %v", column, err)
		}
		if nullable != "YES" {
			t.Fatalf("expected lease column %s to be nullable, got %s", column, nullable)
		}
	}

	var indexDef string
	err := pool.QueryRow(ctx, `
		SELECT pg_get_indexdef(indexrelid)
		FROM pg_index
		WHERE indexrelid = 'idx_task_runs_expired'::regclass
	`).Scan(&indexDef)
	if err != nil {
		t.Fatalf("load expired lease index: %v", err)
	}
	for _, want := range []string{"lease_expires_at", "running", "lease_expires_at IS NOT NULL"} {
		if !strings.Contains(indexDef, want) {
			t.Fatalf("expected expired lease index to contain %q, got %s", want, indexDef)
		}
	}
}

func TestExpiredLeaseQueryIgnoresTerminalTaskRuns(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if err := insertTask(ctx, tx, taskTransformID, workflowAID, "transform", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if err := insertTask(ctx, tx, taskLoadID, workflowAID, "load", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	extraTaskID := "00000000-0000-0000-0000-0000000000a4"
	if err := insertTask(ctx, tx, extraTaskID, workflowAID, "notify", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	workflowRunID := "00000000-0000-0000-0000-0000000000ca"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	rows := []struct {
		id      string
		taskID  string
		status  string
		expires time.Time
	}{
		{"00000000-0000-0000-0000-0000000000d8", taskExtractID, "running", now.Add(-time.Minute)},
		{"00000000-0000-0000-0000-0000000000d9", taskTransformID, "completed", now.Add(-time.Minute)},
		{"00000000-0000-0000-0000-0000000000da", taskLoadID, "dead_letter", now.Add(-time.Minute)},
		{"00000000-0000-0000-0000-0000000000db", extraTaskID, "running", now.Add(time.Minute)},
	}
	for _, row := range rows {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, locked_by, lease_expires_at, last_heartbeat_at)
			VALUES ($1, $2, $3, $4, $5, 'worker-1', $6, $7)
		`, row.id, workflowAID, workflowRunID, row.taskID, row.status, row.expires, now)
		if err != nil {
			t.Fatalf("insert leased task run %s: %v", row.id, err)
		}
	}

	var expired []string
	queryRows, err := tx.Query(ctx, `
		SELECT id
		FROM task_runs
		WHERE status = 'running'
			AND lease_expires_at <= $1
		ORDER BY id
	`, now)
	if err != nil {
		t.Fatalf("query expired leases: %v", err)
	}
	defer queryRows.Close()
	for queryRows.Next() {
		var id string
		if err := queryRows.Scan(&id); err != nil {
			t.Fatalf("scan expired lease: %v", err)
		}
		expired = append(expired, id)
	}
	if err := queryRows.Err(); err != nil {
		t.Fatalf("iterate expired leases: %v", err)
	}

	if len(expired) != 1 || expired[0] != "00000000-0000-0000-0000-0000000000d8" {
		t.Fatalf("expected only expired running task run, got %#v", expired)
	}
}

// ------------------------------------------------------------------------
// Invalid inserts
// ------------------------------------------------------------------------

func TestInvalidInsert_DuplicateTaskNameInWorkflow(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert first task: %v", err)
	}

	err := insertTask(ctx, tx, "00000000-0000-0000-0000-0000000000a9", workflowAID, "extract", "http")
	expectPgError(t, err, "uq_tasks_workflow_name")
}

func TestInvalidInsert_SelfDependency(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	err := insertDependency(ctx, tx, workflowAID, taskExtractID, taskExtractID)
	expectPgError(t, err, "chk_task_dependencies_not_self")
}

func TestInvalidInsert_DuplicateDependency(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task extract: %v", err)
	}
	if err := insertTask(ctx, tx, taskTransformID, workflowAID, "transform", "http"); err != nil {
		t.Fatalf("insert task transform: %v", err)
	}
	if err := insertDependency(ctx, tx, workflowAID, taskExtractID, taskTransformID); err != nil {
		t.Fatalf("insert dependency: %v", err)
	}

	err := insertDependency(ctx, tx, workflowAID, taskExtractID, taskTransformID)
	expectPgError(t, err, "pk_task_dependencies")
}

func TestInvalidInsert_DependencyAcrossWorkflows(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf-a"); err != nil {
		t.Fatalf("insert workflow a: %v", err)
	}
	if err := insertWorkflow(ctx, tx, workflowBID, "wf-b"); err != nil {
		t.Fatalf("insert workflow b: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task in workflow a: %v", err)
	}
	if err := insertTask(ctx, tx, otherWorkflowTaskID, workflowBID, "other", "http"); err != nil {
		t.Fatalf("insert task in workflow b: %v", err)
	}

	err := insertDependency(ctx, tx, workflowAID, taskExtractID, otherWorkflowTaskID)
	expectPgError(t, err, "fk_task_dependencies_successor")
}

func TestInvalidInsert_TaskRunAcrossWorkflows(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf-a"); err != nil {
		t.Fatalf("insert workflow a: %v", err)
	}
	if err := insertWorkflow(ctx, tx, workflowBID, "wf-b"); err != nil {
		t.Fatalf("insert workflow b: %v", err)
	}
	if err := insertTask(ctx, tx, otherWorkflowTaskID, workflowBID, "other", "http"); err != nil {
		t.Fatalf("insert task in workflow b: %v", err)
	}

	workflowRunID := "00000000-0000-0000-0000-0000000000c2"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run under workflow a: %v", err)
	}

	err := insertTaskRun(ctx, tx, "00000000-0000-0000-0000-0000000000d9", workflowAID, workflowRunID, otherWorkflowTaskID)
	expectPgError(t, err, "fk_task_runs_task")
}

func TestInvalidInsert_DuplicateTaskRunForWorkflowRunAndTask(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	workflowRunID := "00000000-0000-0000-0000-0000000000c3"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	if err := insertTaskRun(ctx, tx, "00000000-0000-0000-0000-0000000000d4", workflowAID, workflowRunID, taskExtractID); err != nil {
		t.Fatalf("insert first task run: %v", err)
	}

	err := insertTaskRun(ctx, tx, "00000000-0000-0000-0000-0000000000d5", workflowAID, workflowRunID, taskExtractID)
	expectPgError(t, err, "uq_task_runs_workflow_run_task")
}

func TestInvalidInsert_DuplicateAttemptNumber(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	taskRunID := seedSingleTaskRun(t, ctx, tx)

	if err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e3", taskRunID, 1, "completed", nil); err != nil {
		t.Fatalf("insert first attempt: %v", err)
	}

	err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e4", taskRunID, 1, "completed", nil)
	expectPgError(t, err, "uq_task_attempts_task_run_number")
}

func TestInvalidInsert_NegativeAttemptCount(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	workflowRunID := "00000000-0000-0000-0000-0000000000c4"
	if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, attempt_count)
		VALUES ($1, $2, $3, $4, -1)
	`, "00000000-0000-0000-0000-0000000000d6", workflowAID, workflowRunID, taskExtractID)
	expectPgError(t, err, "chk_task_runs_attempt_count")
}

func TestInvalidInsert_AttemptNumberZero(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	taskRunID := seedSingleTaskRun(t, ctx, tx)

	err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e5", taskRunID, 0, "running", nil)
	expectPgError(t, err, "chk_task_attempts_number_positive")
}

func TestInvalidInsert_CompletionBeforeStart(t *testing.T) {
	started := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	completed := started.Add(-1 * time.Hour)

	t.Run("workflow_runs", func(t *testing.T) {
		pool := testPool(t)
		ctx, tx := beginTx(t, pool)

		if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
			t.Fatalf("insert workflow: %v", err)
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO workflow_runs (id, workflow_id, started_at, completed_at)
			VALUES ($1, $2, $3, $4)
		`, "00000000-0000-0000-0000-0000000000c5", workflowAID, started, completed)
		expectPgError(t, err, "chk_workflow_runs_timestamp_order")
	})

	t.Run("task_runs", func(t *testing.T) {
		pool := testPool(t)
		ctx, tx := beginTx(t, pool)

		if err := insertWorkflow(ctx, tx, workflowAID, "wf"); err != nil {
			t.Fatalf("insert workflow: %v", err)
		}
		if err := insertTask(ctx, tx, taskExtractID, workflowAID, "extract", "http"); err != nil {
			t.Fatalf("insert task: %v", err)
		}
		workflowRunID := "00000000-0000-0000-0000-0000000000c6"
		if err := insertWorkflowRun(ctx, tx, workflowRunID, workflowAID); err != nil {
			t.Fatalf("insert workflow run: %v", err)
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, started_at, completed_at)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, "00000000-0000-0000-0000-0000000000d7", workflowAID, workflowRunID, taskExtractID, started, completed)
		expectPgError(t, err, "chk_task_runs_timestamp_order")
	})

	t.Run("task_attempts", func(t *testing.T) {
		pool := testPool(t)
		ctx, tx := beginTx(t, pool)

		taskRunID := seedSingleTaskRun(t, ctx, tx)

		_, err := tx.Exec(ctx, `
			INSERT INTO task_attempts (id, task_run_id, attempt_number, status, started_at, completed_at)
			VALUES ($1, $2, 1, 'completed', $3, $4)
		`, "00000000-0000-0000-0000-0000000000e6", taskRunID, started, completed)
		expectPgError(t, err, "chk_task_attempts_timestamp_order")
	})
}

func TestInvalidInsert_FailureReasonOnNonFailedAttempt(t *testing.T) {
	pool := testPool(t)
	ctx, tx := beginTx(t, pool)

	taskRunID := seedSingleTaskRun(t, ctx, tx)

	err := insertTaskAttempt(ctx, tx, "00000000-0000-0000-0000-0000000000e7", taskRunID, 1, "completed", strPtr("should not be allowed"))
	expectPgError(t, err, "chk_task_attempts_failure_reason")
}
