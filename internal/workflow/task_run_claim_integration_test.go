package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkflowClaimTestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	workflowClaimMigrationPath          = "../../migrations/001_initial_schema.up.sql"
	workflowClaimIdempotencyPath        = "../../migrations/002_workflow_run_idempotency.up.sql"
	workflowClaimOutboxPath             = "../../migrations/003_task_outbox_events.up.sql"
	workflowClaimLeasePath              = "../../migrations/004_task_run_lease.up.sql"
)

var (
	workflowClaimPoolOnce sync.Once
	workflowClaimShared   *pgxpool.Pool
	workflowClaimPoolErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if workflowClaimShared != nil {
		workflowClaimShared.Close()
	}
	os.Exit(code)
}

func workflowClaimTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	workflowClaimPoolOnce.Do(func() {
		workflowClaimShared, workflowClaimPoolErr = setupWorkflowClaimTestDatabase(context.Background())
	})

	if workflowClaimPoolErr != nil {
		t.Skipf("postgres not available for Day 7 claim tests (run `make postgres-up`): %v", workflowClaimPoolErr)
	}

	return workflowClaimShared
}

func setupWorkflowClaimTestDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultWorkflowClaimTestDatabaseURL
	}

	pool, err := database.Connect(ctx, databaseURL)
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
		migrationSQL, err := os.ReadFile(workflowClaimMigrationPath)
		if err != nil {
			pool.Close()
			return nil, err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if err := ensureWorkflowClaimIdempotencySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureWorkflowClaimOutboxSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureWorkflowClaimLeaseSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func ensureWorkflowClaimIdempotencySchema(ctx context.Context, pool *pgxpool.Pool) error {
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
		migrationSQL, err := os.ReadFile(workflowClaimIdempotencyPath)
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

func ensureWorkflowClaimOutboxSchema(ctx context.Context, pool *pgxpool.Pool) error {
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
		return ensureWorkflowClaimOutboxClaimSchema(ctx, pool)
	}

	migrationSQL, err := os.ReadFile(workflowClaimOutboxPath)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(migrationSQL))
	return err
}

func ensureWorkflowClaimOutboxClaimSchema(ctx context.Context, pool *pgxpool.Pool) error {
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

func ensureWorkflowClaimLeaseSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var columnExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'task_runs'
				AND column_name = 'lease_expires_at'
		)
	`).Scan(&columnExists)
	if err != nil || columnExists {
		return err
	}

	migrationSQL, err := os.ReadFile(workflowClaimLeasePath)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(migrationSQL))
	return err
}

func TestPostgresRepositoryClaimTaskRunMovesQueuedTaskRunToRunning(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	claimed, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim task run: %v", err)
	}

	if claimed.ID != fixture.taskRunID {
		t.Fatalf("expected claimed task run ID %q, got %q", fixture.taskRunID, claimed.ID)
	}
	if claimed.Status != TaskRunStatusRunning {
		t.Fatalf("expected claimed status running, got %q", claimed.Status)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusRunning {
		t.Fatalf("expected persisted status running, got %q", state.status)
	}
	if state.startedAt == nil {
		t.Fatal("expected started_at to be set when task run is claimed")
	}
	if state.lockedBy == nil || *state.lockedBy != "worker-1" {
		t.Fatalf("expected locked_by worker-1, got %#v", state.lockedBy)
	}
	if state.leaseExpiresAt == nil {
		t.Fatal("expected lease_expires_at to be set")
	}
	if state.lastHeartbeatAt == nil {
		t.Fatal("expected last_heartbeat_at to be set")
	}
	if !state.leaseExpiresAt.After(*state.lastHeartbeatAt) {
		t.Fatalf("expected lease_expires_at after heartbeat, got lease=%s heartbeat=%s", state.leaseExpiresAt, state.lastHeartbeatAt)
	}
}

func TestPostgresRepositoryClaimTaskRunRejectsBlankWorkerID(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      " ",
		LeaseDuration: 30 * time.Second,
	})
	if !errors.Is(err, ErrTaskRunNotClaimable) {
		t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusQueued {
		t.Fatalf("expected status to remain queued, got %q", state.status)
	}
	if state.lockedBy != nil || state.leaseExpiresAt != nil || state.lastHeartbeatAt != nil {
		t.Fatalf("expected lease fields to stay empty, got locked_by=%#v lease=%v heartbeat=%v", state.lockedBy, state.leaseExpiresAt, state.lastHeartbeatAt)
	}
}

func TestPostgresRepositoryClaimTaskRunRejectsInvalidLeaseDuration(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 0,
	})
	if !errors.Is(err, ErrTaskRunNotClaimable) {
		t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusQueued {
		t.Fatalf("expected status to remain queued, got %q", state.status)
	}
}

func TestPostgresRepositoryClaimTaskRunRejectsNonQueuedTaskRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	statuses := []TaskRunStatus{
		TaskRunStatusPending,
		TaskRunStatusRunning,
		TaskRunStatusCompleted,
		TaskRunStatusFailed,
		TaskRunStatusDeadLetter,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			fixture := seedTaskRunForClaim(t, pool, status)

			_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
				TaskRunID:     fixture.taskRunID,
				WorkerID:      "worker-1",
				LeaseDuration: 30 * time.Second,
			})
			if !errors.Is(err, ErrTaskRunNotClaimable) {
				t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
			}

			state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
			if state.status != status {
				t.Fatalf("expected status to remain %q, got %q", status, state.status)
			}
		})
	}
}

func TestPostgresRepositoryClaimTaskRunRejectsMissingTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     uuid.NewString(),
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	if !errors.Is(err, ErrTaskRunNotClaimable) {
		t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
	}
}

func TestPostgresRepositoryLoadTaskRunStatus(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusCompleted)
	repo := NewPostgresRepository(pool)

	status, err := repo.LoadTaskRunStatus(context.Background(), LoadTaskRunStatusInput{
		TaskRunID: " " + fixture.taskRunID + " ",
	})
	if err != nil {
		t.Fatalf("load task run status: %v", err)
	}
	if status != TaskRunStatusCompleted {
		t.Fatalf("expected completed status, got %q", status)
	}
}

func TestPostgresRepositoryLoadTaskRunStatusRejectsMissingTaskRun(t *testing.T) {
	repo := NewPostgresRepository(workflowClaimTestPool(t))

	_, err := repo.LoadTaskRunStatus(context.Background(), LoadTaskRunStatusInput{
		TaskRunID: uuid.NewString(),
	})
	if !errors.Is(err, ErrTaskRunNotFound) {
		t.Fatalf("expected ErrTaskRunNotFound, got %v", err)
	}
}

func TestPostgresRepositoryClaimTaskRunAllowsOnlyOneConcurrentClaim(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(workerNumber int) {
			defer wg.Done()
			_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
				TaskRunID:     fixture.taskRunID,
				WorkerID:      fmt.Sprintf("worker-%d", workerNumber),
				LeaseDuration: 30 * time.Second,
			})
			errs <- err
		}(i + 1)
	}
	wg.Wait()
	close(errs)

	successes := 0
	notClaimable := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrTaskRunNotClaimable):
			notClaimable++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}

	if successes != 1 || notClaimable != 1 {
		t.Fatalf("expected one successful claim and one not-claimable result, got successes=%d not_claimable=%d", successes, notClaimable)
	}
}

func TestPostgresRepositoryExtendTaskRunLeaseUpdatesOwnerRunningTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	claimed, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim task run: %v", err)
	}
	before := loadTaskRunClaimState(t, pool, fixture.taskRunID)

	extended, err := repo.ExtendTaskRunLease(context.Background(), ExtendTaskRunLeaseInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("extend task run lease: %v", err)
	}

	if extended.ID != claimed.ID {
		t.Fatalf("expected extended task run ID %q, got %q", claimed.ID, extended.ID)
	}
	if extended.Status != TaskRunStatusRunning {
		t.Fatalf("expected extended task run status running, got %q", extended.Status)
	}
	if !extended.LeaseExpiresAt.After(claimed.LeaseExpiresAt) {
		t.Fatalf("expected extended lease after claimed lease, got claimed=%s extended=%s", claimed.LeaseExpiresAt, extended.LeaseExpiresAt)
	}

	after := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if after.status != TaskRunStatusRunning {
		t.Fatalf("expected persisted status running, got %q", after.status)
	}
	if after.lockedBy == nil || *after.lockedBy != "worker-1" {
		t.Fatalf("expected locked_by worker-1, got %#v", after.lockedBy)
	}
	if after.leaseExpiresAt == nil || !after.leaseExpiresAt.After(*before.leaseExpiresAt) {
		t.Fatalf("expected persisted lease to extend after %v, got %v", before.leaseExpiresAt, after.leaseExpiresAt)
	}
	if after.lastHeartbeatAt == nil || before.lastHeartbeatAt == nil || after.lastHeartbeatAt.Before(*before.lastHeartbeatAt) {
		t.Fatalf("expected heartbeat to move forward from %v, got %v", before.lastHeartbeatAt, after.lastHeartbeatAt)
	}
}

func TestPostgresRepositoryExtendTaskRunLeaseRejectsDifferentWorker(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	if _, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	}); err != nil {
		t.Fatalf("claim task run: %v", err)
	}

	_, err := repo.ExtendTaskRunLease(context.Background(), ExtendTaskRunLeaseInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-2",
		LeaseDuration: 30 * time.Second,
	})
	if !errors.Is(err, ErrTaskRunLeaseNotExtensible) {
		t.Fatalf("expected ErrTaskRunLeaseNotExtensible, got %v", err)
	}
}

func TestPostgresRepositoryExtendTaskRunLeaseRejectsCompletedTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	if _, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	}); err != nil {
		t.Fatalf("claim task run: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE task_runs
		SET status = $2
		WHERE id = $1
	`, fixture.taskRunID, TaskRunStatusCompleted); err != nil {
		t.Fatalf("mark task run completed: %v", err)
	}

	_, err := repo.ExtendTaskRunLease(context.Background(), ExtendTaskRunLeaseInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	if !errors.Is(err, ErrTaskRunLeaseNotExtensible) {
		t.Fatalf("expected ErrTaskRunLeaseNotExtensible, got %v", err)
	}
}

func TestPostgresRepositoryExtendTaskRunLeaseRejectsInvalidInput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	tests := []struct {
		name  string
		input ExtendTaskRunLeaseInput
	}{
		{
			name:  "blank task run id",
			input: ExtendTaskRunLeaseInput{TaskRunID: " ", WorkerID: "worker-1", LeaseDuration: 30 * time.Second},
		},
		{
			name:  "blank worker id",
			input: ExtendTaskRunLeaseInput{TaskRunID: uuid.NewString(), WorkerID: " ", LeaseDuration: 30 * time.Second},
		},
		{
			name:  "zero lease duration",
			input: ExtendTaskRunLeaseInput{TaskRunID: uuid.NewString(), WorkerID: "worker-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.ExtendTaskRunLease(context.Background(), tt.input)
			if !errors.Is(err, ErrTaskRunLeaseNotExtensible) {
				t.Fatalf("expected ErrTaskRunLeaseNotExtensible, got %v", err)
			}
		})
	}
}

type taskRunClaimFixture struct {
	workflowName  string
	workflowID    string
	taskID        string
	workflowRunID string
	taskRunID     string
}

func seedTaskRunForClaim(t *testing.T, pool *pgxpool.Pool, status TaskRunStatus) taskRunClaimFixture {
	t.Helper()

	ctx := context.Background()
	fixture := taskRunClaimFixture{
		workflowName:  fmt.Sprintf("day7-claim-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		taskID:        uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskRunID:     uuid.NewString(),
	}

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type)
		VALUES ($1, $2, $3, $4)
	`, fixture.taskID, fixture.workflowID, "task", "log")
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
		VALUES ($1, $2, $3, $4, $5)
	`, fixture.taskRunID, fixture.workflowID, fixture.workflowRunID, fixture.taskID, status)
	if err != nil {
		t.Fatalf("insert task run: %v", err)
	}
	if status == TaskRunStatusRunning {
		_, err = pool.Exec(ctx, `
			UPDATE task_runs
			SET locked_by = $2,
				lease_expires_at = now() + interval '1 hour',
				last_heartbeat_at = now()
			WHERE id = $1
		`, fixture.taskRunID, "worker-1")
		if err != nil {
			t.Fatalf("set running task run lease: %v", err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `
			WITH target_workflows AS (
				SELECT id FROM workflows WHERE name = $1
			)
			DELETE FROM task_attempts
			WHERE task_run_id IN (
				SELECT id FROM task_runs
				WHERE workflow_id IN (SELECT id FROM target_workflows)
			)
		`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}

func taskRunStatusAndStartedAt(t *testing.T, pool *pgxpool.Pool, taskRunID string) (TaskRunStatus, *time.Time) {
	t.Helper()

	var status TaskRunStatus
	var startedAt *time.Time
	err := pool.QueryRow(context.Background(), `
		SELECT status, started_at
		FROM task_runs
		WHERE id = $1
	`, taskRunID).Scan(&status, &startedAt)
	if err != nil {
		t.Fatalf("load task run status: %v", err)
	}

	return status, startedAt
}

type taskRunClaimState struct {
	status          TaskRunStatus
	startedAt       *time.Time
	lockedBy        *string
	leaseExpiresAt  *time.Time
	lastHeartbeatAt *time.Time
}

func loadTaskRunClaimState(t *testing.T, pool *pgxpool.Pool, taskRunID string) taskRunClaimState {
	t.Helper()

	var state taskRunClaimState
	err := pool.QueryRow(context.Background(), `
		SELECT status, started_at, locked_by, lease_expires_at, last_heartbeat_at
		FROM task_runs
		WHERE id = $1
	`, taskRunID).Scan(&state.status, &state.startedAt, &state.lockedBy, &state.leaseExpiresAt, &state.lastHeartbeatAt)
	if err != nil {
		t.Fatalf("load task run claim state: %v", err)
	}

	return state
}
