package worker

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkerServiceTestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	workerServiceMigrationPath          = "../../migrations/001_initial_schema.up.sql"
)

var (
	workerServicePoolOnce sync.Once
	workerServiceShared   *pgxpool.Pool
	workerServicePoolErr  error
)

func TestExecutionServiceIntegrationCompletesSuccessfulTaskAndAcknowledgesMessage(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeLog, map[string]any{"message": "hello worker"})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    fixture.workflowID,
				WorkflowRunID: fixture.workflowRunID,
				TaskID:        fixture.taskID,
				TaskRunID:     fixture.taskRunID,
			},
		},
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeLog: NewLogExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged, got %#v", consumer.acks)
	}

	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusCompleted {
		t.Fatalf("expected task run completed, got %q", state.taskRunStatus)
	}
	if state.attemptCount != 1 {
		t.Fatalf("expected attempt_count 1, got %d", state.attemptCount)
	}
	if state.attemptStatus != workflow.TaskAttemptStatusCompleted {
		t.Fatalf("expected task attempt completed, got %q", state.attemptStatus)
	}
	if state.taskRunCompletedAt == nil || state.attemptCompletedAt == nil {
		t.Fatal("expected task run and task attempt completed_at to be set")
	}
	if state.failureReason != nil {
		t.Fatalf("expected no failure reason, got %q", *state.failureReason)
	}
	if state.output["status"] != "completed" || state.output["message"] != "hello worker" {
		t.Fatalf("unexpected task run output: %#v", state.output)
	}
}

func TestExecutionServiceIntegrationLastTaskCompletionFinalizesWorkflowCompleted(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeLog, map[string]any{"message": "done"})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{received: workerServiceMessage(fixture, "redis-message-id")}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeLog: NewLogExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	status, completedAt := loadWorkerServiceWorkflowRunState(t, pool, fixture.workflowRunID)
	if status != workflow.WorkflowRunStatusCompleted {
		t.Fatalf("expected workflow run completed, got %q", status)
	}
	if completedAt == nil {
		t.Fatal("expected workflow run completed_at to be set")
	}
}

func TestExecutionServiceIntegrationFailsTaskAndAcknowledgesMessage(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeRandomFail, map[string]any{"failure_probability": 1})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    fixture.workflowID,
				WorkflowRunID: fixture.workflowRunID,
				TaskID:        fixture.taskID,
				TaskRunID:     fixture.taskRunID,
			},
		},
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeRandomFail: NewRandomFailExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged, got %#v", consumer.acks)
	}

	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusDeadLetter {
		t.Fatalf("expected task run dead_letter, got %q", state.taskRunStatus)
	}
	if state.attemptCount != 1 {
		t.Fatalf("expected attempt_count 1, got %d", state.attemptCount)
	}
	if state.attemptStatus != workflow.TaskAttemptStatusFailed {
		t.Fatalf("expected task attempt failed, got %q", state.attemptStatus)
	}
	if state.taskRunCompletedAt == nil || state.attemptCompletedAt == nil {
		t.Fatal("expected task run and task attempt completed_at to be set")
	}
	if state.failureReason == nil || *state.failureReason != "random failure" {
		t.Fatalf("expected random failure reason, got %#v", state.failureReason)
	}
}

func TestExecutionServiceIntegrationDeadLetterFinalizesWorkflowFailed(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeRandomFail, map[string]any{"failure_probability": 1})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{received: workerServiceMessage(fixture, "redis-message-id")}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeRandomFail: NewRandomFailExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	status, completedAt := loadWorkerServiceWorkflowRunState(t, pool, fixture.workflowRunID)
	if status != workflow.WorkflowRunStatusFailed {
		t.Fatalf("expected workflow run failed, got %q", status)
	}
	if completedAt == nil {
		t.Fatal("expected workflow run completed_at to be set")
	}
}

func TestExecutionServiceIntegrationOneBranchCompletionLeavesWorkflowRunning(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeLog, map[string]any{"message": "branch done"})
	seedWorkerServiceSiblingTaskRun(t, pool, fixture, workflow.TaskRunStatusRunning)
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{received: workerServiceMessage(fixture, "redis-message-id")}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeLog: NewLogExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	status, completedAt := loadWorkerServiceWorkflowRunState(t, pool, fixture.workflowRunID)
	if status != workflow.WorkflowRunStatusRunning {
		t.Fatalf("expected workflow run to stay running, got %q", status)
	}
	if completedAt != nil {
		t.Fatalf("expected workflow run completed_at to stay empty, got %s", completedAt)
	}
}

func TestExecutionServiceIntegrationRunsQueuedRetryAsSecondAttempt(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeLog, map[string]any{"message": "retry success"})
	seedWorkerServiceFailedAttempt(t, pool, fixture.taskRunID)
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    fixture.workflowID,
				WorkflowRunID: fixture.workflowRunID,
				TaskID:        fixture.taskID,
				TaskRunID:     fixture.taskRunID,
			},
		},
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeLog: NewLogExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process retry task: %v", err)
	}

	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged after retry persistence, got %#v", consumer.acks)
	}

	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusCompleted {
		t.Fatalf("expected task run completed, got %q", state.taskRunStatus)
	}
	if state.attemptCount != 2 {
		t.Fatalf("expected attempt_count 2, got %d", state.attemptCount)
	}
	attempts := loadWorkerServiceTaskAttempts(t, pool, fixture.taskRunID)
	want := []workerServiceAttemptState{
		{number: 1, status: workflow.TaskAttemptStatusFailed},
		{number: 2, status: workflow.TaskAttemptStatusCompleted},
	}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("unexpected retry attempts: got %#v, want %#v", attempts, want)
	}
	if state.output["status"] != "completed" || state.output["message"] != "retry success" {
		t.Fatalf("unexpected task run output: %#v", state.output)
	}
}

func TestExecutionServiceIntegrationExhaustedRetryDeadLettersTaskRun(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeRandomFail, map[string]any{
		"failure_probability": float64(1),
		"retry":               map[string]any{"max_attempts": 2},
	})
	seedWorkerServiceFailedAttempt(t, pool, fixture.taskRunID)
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: workerServiceMessage(fixture, "redis-message-id"),
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeRandomFail: NewRandomFailExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process exhausted retry task: %v", err)
	}

	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged, got %#v", consumer.acks)
	}
	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusDeadLetter {
		t.Fatalf("expected exhausted retry task run dead_letter, got %q", state.taskRunStatus)
	}
	if state.attemptCount != 2 {
		t.Fatalf("expected attempt_count 2, got %d", state.attemptCount)
	}
	want := []workerServiceAttemptState{
		{number: 1, status: workflow.TaskAttemptStatusFailed},
		{number: 2, status: workflow.TaskAttemptStatusFailed},
	}
	if attempts := loadWorkerServiceTaskAttempts(t, pool, fixture.taskRunID); !reflect.DeepEqual(attempts, want) {
		t.Fatalf("unexpected exhausted retry attempts: got %#v, want %#v", attempts, want)
	}
}

func TestExecutionServiceIntegrationUnknownExecutorFailureIsPermanentEvenWithRetryPolicy(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, "unknown", map[string]any{
		"retry": map[string]any{"max_attempts": 3},
	})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: workerServiceMessage(fixture, "redis-message-id"),
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(nil),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process unknown executor task: %v", err)
	}

	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusDeadLetter {
		t.Fatalf("expected unknown executor task run dead_letter, got %q", state.taskRunStatus)
	}
	if state.failureReason == nil || *state.failureReason != ErrUnknownExecutorType.Error() {
		t.Fatalf("expected unknown executor failure reason, got %#v", state.failureReason)
	}
	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged, got %#v", consumer.acks)
	}
}

func TestExecutionServiceIntegrationInvalidRetryPolicyFailsWithoutRetryLoop(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerServiceTaskRun(t, pool, ExecutorTypeRandomFail, map[string]any{
		"failure_probability": float64(1),
		"retry":               map[string]any{"multiplier": 0.5},
	})
	repo := workflow.NewPostgresRepository(pool)
	consumer := &fakeConsumer{
		received: workerServiceMessage(fixture, "redis-message-id"),
	}
	service := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		consumer,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeRandomFail: NewRandomFailExecutor(nil),
		}),
	)

	if err := service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process invalid retry policy task: %v", err)
	}

	state := loadWorkerServiceTaskRunState(t, pool, fixture.taskRunID)
	if state.taskRunStatus != workflow.TaskRunStatusDeadLetter {
		t.Fatalf("expected invalid retry policy task run dead_letter, got %q", state.taskRunStatus)
	}
	if state.attemptCount != 1 {
		t.Fatalf("expected invalid retry policy to stop after one attempt, got %d", state.attemptCount)
	}
	if state.failureReason == nil || !strings.Contains(*state.failureReason, workflow.ErrValidation.Error()) {
		got := "<nil>"
		if state.failureReason != nil {
			got = *state.failureReason
		}
		t.Fatalf("expected validation failure reason, got %q", got)
	}
	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acknowledged, got %#v", consumer.acks)
	}
}

func workerServiceTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	workerServicePoolOnce.Do(func() {
		workerServiceShared, workerServicePoolErr = setupWorkerServiceTestDatabase(context.Background())
	})

	if workerServicePoolErr != nil {
		t.Skipf("postgres not available for Day 8 Step 10 worker service tests (run `make postgres-up`): %v", workerServicePoolErr)
	}

	return workerServiceShared
}

func setupWorkerServiceTestDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultWorkerServiceTestDatabaseURL
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
		migrationSQL, err := os.ReadFile(workerServiceMigrationPath)
		if err != nil {
			pool.Close()
			return nil, err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			pool.Close()
			return nil, err
		}
	}

	return pool, nil
}

type workerServiceTaskRunFixture struct {
	workflowName  string
	workflowID    string
	taskID        string
	workflowRunID string
	taskRunID     string
}

func seedWorkerServiceTaskRun(
	t *testing.T,
	pool *pgxpool.Pool,
	executorType string,
	config map[string]any,
) workerServiceTaskRunFixture {
	t.Helper()

	ctx := context.Background()
	fixture := workerServiceTaskRunFixture{
		workflowName:  fmt.Sprintf("day8-step10-worker-%d", time.Now().UnixNano()),
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
		INSERT INTO tasks (id, workflow_id, name, executor_type, config)
		VALUES ($1, $2, $3, $4, $5)
	`, fixture.taskID, fixture.workflowID, "task", executorType, config)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, workflow.WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
		VALUES ($1, $2, $3, $4, $5)
	`, fixture.taskRunID, fixture.workflowID, fixture.workflowRunID, fixture.taskID, workflow.TaskRunStatusQueued)
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

func workerServiceMessage(fixture workerServiceTaskRunFixture, messageID string) queue.ReceivedTaskMessage {
	return queue.ReceivedTaskMessage{
		MessageID: messageID,
		TaskMessage: queue.TaskMessage{
			WorkflowID:    fixture.workflowID,
			WorkflowRunID: fixture.workflowRunID,
			TaskID:        fixture.taskID,
			TaskRunID:     fixture.taskRunID,
		},
	}
}

func seedWorkerServiceFailedAttempt(t *testing.T, pool *pgxpool.Pool, taskRunID string) {
	t.Helper()

	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		UPDATE task_runs
		SET attempt_count = 1
		WHERE id = $1
	`, taskRunID)
	if err != nil {
		t.Fatalf("seed retry attempt count: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_attempts (id, task_run_id, attempt_number, status, completed_at, failure_reason)
		VALUES ($1, $2, $3, $4, now(), $5)
	`, uuid.NewString(), taskRunID, 1, workflow.TaskAttemptStatusFailed, "temporary failure")
	if err != nil {
		t.Fatalf("seed failed first attempt: %v", err)
	}
}

func seedWorkerServiceSiblingTaskRun(t *testing.T, pool *pgxpool.Pool, fixture workerServiceTaskRunFixture, status workflow.TaskRunStatus) {
	t.Helper()

	ctx := context.Background()
	taskID := uuid.NewString()
	_, err := pool.Exec(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type)
		VALUES ($1, $2, $3, $4)
	`, taskID, fixture.workflowID, "sibling", ExecutorTypeLog)
	if err != nil {
		t.Fatalf("insert sibling task: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.NewString(), fixture.workflowID, fixture.workflowRunID, taskID, status)
	if err != nil {
		t.Fatalf("insert sibling task run: %v", err)
	}
}

type workerServiceTaskRunState struct {
	taskRunStatus      workflow.TaskRunStatus
	attemptCount       int
	output             map[string]any
	attemptStatus      workflow.TaskAttemptStatus
	failureReason      *string
	taskRunCompletedAt *time.Time
	attemptCompletedAt *time.Time
}

func loadWorkerServiceTaskRunState(t *testing.T, pool *pgxpool.Pool, taskRunID string) workerServiceTaskRunState {
	t.Helper()

	var state workerServiceTaskRunState
	err := pool.QueryRow(context.Background(), `
		SELECT
			task_runs.status,
			task_runs.attempt_count,
			task_runs.output,
			task_attempts.status,
			task_attempts.failure_reason,
			task_runs.completed_at,
			task_attempts.completed_at
		FROM task_runs
		JOIN task_attempts
			ON task_attempts.task_run_id = task_runs.id
		WHERE task_runs.id = $1
	`, taskRunID).Scan(
		&state.taskRunStatus,
		&state.attemptCount,
		&state.output,
		&state.attemptStatus,
		&state.failureReason,
		&state.taskRunCompletedAt,
		&state.attemptCompletedAt,
	)
	if err != nil {
		t.Fatalf("load worker service task run state: %v", err)
	}

	return state
}

func loadWorkerServiceWorkflowRunState(t *testing.T, pool *pgxpool.Pool, workflowRunID string) (workflow.WorkflowRunStatus, *time.Time) {
	t.Helper()

	var status workflow.WorkflowRunStatus
	var completedAt *time.Time
	err := pool.QueryRow(context.Background(), `
		SELECT status, completed_at
		FROM workflow_runs
		WHERE id = $1
	`, workflowRunID).Scan(&status, &completedAt)
	if err != nil {
		t.Fatalf("load worker service workflow run state: %v", err)
	}
	return status, completedAt
}

type workerServiceAttemptState struct {
	number uint
	status workflow.TaskAttemptStatus
}

func loadWorkerServiceTaskAttempts(t *testing.T, pool *pgxpool.Pool, taskRunID string) []workerServiceAttemptState {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT attempt_number, status
		FROM task_attempts
		WHERE task_run_id = $1
		ORDER BY attempt_number
	`, taskRunID)
	if err != nil {
		t.Fatalf("query worker service task attempts: %v", err)
	}
	defer rows.Close()

	var attempts []workerServiceAttemptState
	for rows.Next() {
		var attempt workerServiceAttemptState
		if err := rows.Scan(&attempt.number, &attempt.status); err != nil {
			t.Fatalf("scan worker service task attempt: %v", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate worker service task attempts: %v", err)
	}
	return attempts
}
