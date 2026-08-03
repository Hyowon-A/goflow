package scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultSchedulerOutboxTestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	schedulerOutboxMigrationPath          = "../../migrations/001_initial_schema.up.sql"
	schedulerOutboxIdempotencyPath        = "../../migrations/002_workflow_run_idempotency.up.sql"
	schedulerOutboxTablePath              = "../../migrations/003_task_outbox_events.up.sql"
)

var (
	schedulerOutboxPoolOnce sync.Once
	schedulerOutboxShared   *pgxpool.Pool
	schedulerOutboxPoolErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if schedulerOutboxShared != nil {
		schedulerOutboxShared.Close()
	}
	os.Exit(code)
}

func TestOutboxDispatcherRecoversQueuedRowsAfterCrash(t *testing.T) {
	pool := schedulerOutboxTestPool(t)
	fixture := seedSchedulerOutboxRoots(t, pool, 2)
	repo := workflow.NewPostgresRepository(pool)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	if len(queued) != 2 {
		t.Fatalf("expected two queued task runs, got %#v", queued)
	}
	assertSchedulerOutboxStatusCount(t, pool, fixture.workflowRunID, "pending", 2)

	publisher := &fakePublisher{}
	dispatcher := NewOutboxDispatcher(repo, publisher)
	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch recovered outbox events: %v", err)
	}

	if len(publisher.messages) != 2 {
		t.Fatalf("expected two recovered publishes, got %#v", publisher.messages)
	}
	assertSchedulerOutboxPublishedCount(t, pool, fixture.workflowRunID, 2)
	assertSchedulerOutboxStatusCount(t, pool, fixture.workflowRunID, "pending", 0)

	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch no-op recovery pass: %v", err)
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("expected second recovery pass to be a no-op, got %#v", publisher.messages)
	}
}

func schedulerOutboxTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	schedulerOutboxPoolOnce.Do(func() {
		schedulerOutboxShared, schedulerOutboxPoolErr = setupSchedulerOutboxTestDatabase(context.Background())
	})
	if schedulerOutboxPoolErr != nil {
		t.Skipf("postgres not available for Day 11 outbox recovery tests (run `make postgres-up`): %v", schedulerOutboxPoolErr)
	}
	return schedulerOutboxShared
}

func setupSchedulerOutboxTestDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultSchedulerOutboxTestDatabaseURL
	}

	pool, err := database.Connect(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := ensureSchedulerOutboxSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func ensureSchedulerOutboxSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var workflowsExists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'workflows'
		)
	`).Scan(&workflowsExists); err != nil {
		return err
	}
	if !workflowsExists {
		migrationSQL, err := os.ReadFile(schedulerOutboxMigrationPath)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			return err
		}
	}

	var idempotencyExists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'workflow_runs'
				AND column_name = 'idempotency_key'
		)
	`).Scan(&idempotencyExists); err != nil {
		return err
	}
	if !idempotencyExists {
		migrationSQL, err := os.ReadFile(schedulerOutboxIdempotencyPath)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			return err
		}
	}

	var outboxExists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public'
				AND table_name = 'task_outbox_events'
		)
	`).Scan(&outboxExists); err != nil {
		return err
	}
	if !outboxExists {
		migrationSQL, err := os.ReadFile(schedulerOutboxTablePath)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			return err
		}
	}

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

type schedulerOutboxFixture struct {
	workflowName  string
	workflowID    string
	workflowRunID string
}

func seedSchedulerOutboxRoots(t *testing.T, pool *pgxpool.Pool, count int) schedulerOutboxFixture {
	t.Helper()

	ctx := context.Background()
	fixture := schedulerOutboxFixture{
		workflowName:  fmt.Sprintf("day11-outbox-recovery-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
	}
	if _, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName); err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, workflow.WorkflowRunStatusRunning); err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	for i := 0; i < count; i++ {
		taskID := uuid.NewString()
		if _, err := pool.Exec(ctx, `
			INSERT INTO tasks (id, workflow_id, name, executor_type)
			VALUES ($1, $2, $3, $4)
		`, taskID, fixture.workflowID, fmt.Sprintf("task-%d", i), "log"); err != nil {
			t.Fatalf("insert task: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), fixture.workflowID, fixture.workflowRunID, taskID, workflow.TaskRunStatusPending); err != nil {
			t.Fatalf("insert task run: %v", err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id = $1`, fixture.workflowID)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id = $1`, fixture.workflowID)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id = $1`, fixture.workflowID)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id = $1`, fixture.workflowID)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE id = $1`, fixture.workflowID)
	})

	return fixture
}

func assertSchedulerOutboxStatusCount(t *testing.T, pool *pgxpool.Pool, workflowRunID, status string, want int) {
	t.Helper()

	var got int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_outbox_events
		WHERE workflow_run_id = $1
			AND status = $2
	`, workflowRunID, status).Scan(&got); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d %s outbox events, got %d", want, status, got)
	}
}

func assertSchedulerOutboxPublishedCount(t *testing.T, pool *pgxpool.Pool, workflowRunID string, want int) {
	t.Helper()

	var got int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_outbox_events
		WHERE workflow_run_id = $1
			AND status = 'published'
			AND redis_message_id IS NOT NULL
			AND published_at IS NOT NULL
	`, workflowRunID).Scan(&got); err != nil {
		t.Fatalf("count published outbox events: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d published outbox events, got %d", want, got)
	}
}
