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

func TestPostgresRepositoryClaimTaskRunMovesQueuedTaskRunToRunning(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusQueued)
	repo := NewPostgresRepository(pool)

	claimed, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID: fixture.taskRunID,
		WorkerID:  "worker-1",
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

	status, started := taskRunStatusAndStartedAt(t, pool, fixture.taskRunID)
	if status != TaskRunStatusRunning {
		t.Fatalf("expected persisted status running, got %q", status)
	}
	if started == nil {
		t.Fatal("expected started_at to be set when task run is claimed")
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
				TaskRunID: fixture.taskRunID,
				WorkerID:  "worker-1",
			})
			if !errors.Is(err, ErrTaskRunNotClaimable) {
				t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
			}

			gotStatus, _ := taskRunStatusAndStartedAt(t, pool, fixture.taskRunID)
			if gotStatus != status {
				t.Fatalf("expected status to remain %q, got %q", status, gotStatus)
			}
		})
	}
}

func TestPostgresRepositoryClaimTaskRunRejectsMissingTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID: uuid.NewString(),
		WorkerID:  "worker-1",
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
				TaskRunID: fixture.taskRunID,
				WorkerID:  fmt.Sprintf("worker-%d", workerNumber),
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

	t.Cleanup(func() {
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
