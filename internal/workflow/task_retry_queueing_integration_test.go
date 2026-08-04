package workflow

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryQueueDueRetryTaskRunsQueuesDueRetryWaitRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRetryQueueTaskRuns(t, pool)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueDueRetryTaskRuns(context.Background(), fixture.now)
	if err != nil {
		t.Fatalf("queue due retry task runs: %v", err)
	}

	if len(queued) != 1 {
		t.Fatalf("expected one queued retry task run, got %#v", queued)
	}
	if queued[0].ID != fixture.dueTaskRunID || queued[0].Status != TaskRunStatusQueued {
		t.Fatalf("unexpected queued retry task run: %#v", queued[0])
	}

	statuses := taskRunStatusesByTask(t, pool, fixture.workflowRunID)
	want := map[string]TaskRunStatus{
		fixture.dueTaskID:       TaskRunStatusQueued,
		fixture.futureTaskID:    TaskRunStatusRetryWait,
		fixture.exhaustedTaskID: TaskRunStatusRetryWait,
	}
	if !reflect.DeepEqual(statuses, want) {
		t.Fatalf("unexpected task run statuses: got %#v, want %#v", statuses, want)
	}
	assertTaskOutboxEvent(t, pool, queued[0])
}

func TestPostgresRepositoryQueueDueRetryTaskRunsSkipsFutureRetries(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRetryQueueTaskRuns(t, pool)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueDueRetryTaskRuns(context.Background(), fixture.now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("queue future retry task runs: %v", err)
	}
	if len(queued) != 0 {
		t.Fatalf("expected no queued retry task runs, got %#v", queued)
	}

	assertTaskRunStatuses(t, pool, fixture.workflowRunID, map[string]TaskRunStatus{
		fixture.dueTaskID:       TaskRunStatusRetryWait,
		fixture.futureTaskID:    TaskRunStatusRetryWait,
		fixture.exhaustedTaskID: TaskRunStatusRetryWait,
	})
	assertWorkflowRunOutboxEventCount(t, pool, fixture.workflowRunID, 0)
}

func TestPostgresRepositoryQueueDueRetryTaskRunsIsIdempotent(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRetryQueueTaskRuns(t, pool)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueDueRetryTaskRuns(context.Background(), fixture.now)
	if err != nil {
		t.Fatalf("queue due retry task runs: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("expected one queued retry task run, got %#v", queued)
	}

	queuedAgain, err := repo.QueueDueRetryTaskRuns(context.Background(), fixture.now)
	if err != nil {
		t.Fatalf("queue due retry task runs again: %v", err)
	}
	if len(queuedAgain) != 0 {
		t.Fatalf("expected idempotent second queue to return no rows, got %#v", queuedAgain)
	}
	assertTaskOutboxEventCount(t, pool, fixture.dueTaskRunID, 1)
}

func TestPostgresRepositoryQueueDueRetryTaskRunsIgnoresTerminalTaskRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRetryQueueTaskRuns(t, pool)
	repo := NewPostgresRepository(pool)

	terminalTaskIDs := map[string]string{
		"completed": uuid.NewString(),
		"failed":    uuid.NewString(),
	}
	for name, taskID := range terminalTaskIDs {
		_, err := pool.Exec(context.Background(), `
			INSERT INTO tasks (id, workflow_id, name, executor_type, config)
			VALUES ($1, $2, $3, $4, $5)
		`, taskID, fixture.workflowID, name, "log", map[string]any{"retry": map[string]any{"max_attempts": 3}})
		if err != nil {
			t.Fatalf("insert terminal task %s: %v", name, err)
		}
		status := TaskRunStatusCompleted
		if name == "failed" {
			status = TaskRunStatusFailed
		}
		_, err = pool.Exec(context.Background(), `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, attempt_count, next_retry_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, uuid.NewString(), fixture.workflowID, fixture.workflowRunID, taskID, status, 1, fixture.now)
		if err != nil {
			t.Fatalf("insert terminal task run %s: %v", name, err)
		}
	}

	queued, err := repo.QueueDueRetryTaskRuns(context.Background(), fixture.now)
	if err != nil {
		t.Fatalf("queue due retry task runs: %v", err)
	}

	if len(queued) != 1 || queued[0].ID != fixture.dueTaskRunID {
		t.Fatalf("expected only retry_wait due task run to queue, got %#v", queued)
	}
	assertTaskRunStatuses(t, pool, fixture.workflowRunID, map[string]TaskRunStatus{
		fixture.dueTaskID:            TaskRunStatusQueued,
		fixture.futureTaskID:         TaskRunStatusRetryWait,
		fixture.exhaustedTaskID:      TaskRunStatusRetryWait,
		terminalTaskIDs["completed"]: TaskRunStatusCompleted,
		terminalTaskIDs["failed"]:    TaskRunStatusFailed,
	})
}

type retryQueueTaskRunsFixture struct {
	workflowName    string
	workflowID      string
	workflowRunID   string
	now             time.Time
	dueTaskID       string
	dueTaskRunID    string
	futureTaskID    string
	exhaustedTaskID string
}

func seedRetryQueueTaskRuns(t *testing.T, pool *pgxpool.Pool) retryQueueTaskRunsFixture {
	t.Helper()

	ctx := context.Background()
	fixture := retryQueueTaskRunsFixture{
		workflowName:    fmt.Sprintf("day12-retry-queue-%d", time.Now().UnixNano()),
		workflowID:      uuid.NewString(),
		workflowRunID:   uuid.NewString(),
		now:             time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		dueTaskID:       uuid.NewString(),
		dueTaskRunID:    uuid.NewString(),
		futureTaskID:    uuid.NewString(),
		exhaustedTaskID: uuid.NewString(),
	}

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	for _, task := range []struct {
		id   string
		name string
	}{
		{fixture.dueTaskID, "due"},
		{fixture.futureTaskID, "future"},
		{fixture.exhaustedTaskID, "exhausted"},
	} {
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, workflow_id, name, executor_type, config)
			VALUES ($1, $2, $3, $4, $5)
		`, task.id, fixture.workflowID, task.name, "log", map[string]any{"retry": map[string]any{"max_attempts": 3}})
		if err != nil {
			t.Fatalf("insert task %s: %v", task.name, err)
		}
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	for _, taskRun := range []struct {
		id           string
		taskID       string
		attemptCount int
		nextRetryAt  time.Time
	}{
		{fixture.dueTaskRunID, fixture.dueTaskID, 1, fixture.now},
		{uuid.NewString(), fixture.futureTaskID, 1, fixture.now.Add(time.Hour)},
		{uuid.NewString(), fixture.exhaustedTaskID, 3, fixture.now},
	} {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, attempt_count, next_retry_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, taskRun.id, fixture.workflowID, fixture.workflowRunID, taskRun.taskID, TaskRunStatusRetryWait, taskRun.attemptCount, taskRun.nextRetryAt)
		if err != nil {
			t.Fatalf("insert retry task run: %v", err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}
