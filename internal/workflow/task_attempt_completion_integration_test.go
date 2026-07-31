package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryCompleteTaskAttemptMarksAttemptAndTaskRunCompleted(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	attempt, err := repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
	if err != nil {
		t.Fatalf("create task attempt: %v", err)
	}

	output := map[string]any{"message": "done"}
	result, err := repo.CompleteTaskAttempt(context.Background(), CompleteTaskAttemptInput{
		TaskAttemptID: " " + attempt.ID + " ",
		TaskRunID:     " " + fixture.taskRunID + " ",
		Success:       true,
		Output:        output,
	})
	if err != nil {
		t.Fatalf("complete task attempt: %v", err)
	}

	if result.TaskAttempt.Status != TaskAttemptStatusCompleted {
		t.Fatalf("expected completed task attempt, got %q", result.TaskAttempt.Status)
	}
	if result.TaskRun.Status != TaskRunStatusCompleted {
		t.Fatalf("expected completed task run, got %q", result.TaskRun.Status)
	}

	persisted := loadCompletedAttemptState(t, pool, attempt.ID, fixture.taskRunID)
	if persisted.attemptStatus != TaskAttemptStatusCompleted {
		t.Fatalf("expected persisted attempt status completed, got %q", persisted.attemptStatus)
	}
	if persisted.taskRunStatus != TaskRunStatusCompleted {
		t.Fatalf("expected persisted task run status completed, got %q", persisted.taskRunStatus)
	}
	if persisted.attemptCompletedAt == nil {
		t.Fatal("expected task attempt completed_at to be set")
	}
	if persisted.taskRunCompletedAt == nil {
		t.Fatal("expected task run completed_at to be set")
	}
	if persisted.failureReason != nil {
		t.Fatalf("expected no failure reason, got %q", *persisted.failureReason)
	}
	if !reflect.DeepEqual(persisted.output, output) {
		t.Fatalf("unexpected task run output: got %#v, want %#v", persisted.output, output)
	}
}

func TestPostgresRepositoryCompleteTaskAttemptMarksAttemptAndTaskRunFailed(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	attempt, err := repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
	if err != nil {
		t.Fatalf("create task attempt: %v", err)
	}

	result, err := repo.CompleteTaskAttempt(context.Background(), CompleteTaskAttemptInput{
		TaskAttemptID: attempt.ID,
		TaskRunID:     fixture.taskRunID,
		Success:       false,
		FailureReason: " random failure ",
	})
	if err != nil {
		t.Fatalf("fail task attempt: %v", err)
	}

	if result.TaskAttempt.Status != TaskAttemptStatusFailed {
		t.Fatalf("expected failed task attempt, got %q", result.TaskAttempt.Status)
	}
	if result.TaskRun.Status != TaskRunStatusFailed {
		t.Fatalf("expected failed task run, got %q", result.TaskRun.Status)
	}

	persisted := loadCompletedAttemptState(t, pool, attempt.ID, fixture.taskRunID)
	if persisted.attemptStatus != TaskAttemptStatusFailed {
		t.Fatalf("expected persisted attempt status failed, got %q", persisted.attemptStatus)
	}
	if persisted.taskRunStatus != TaskRunStatusFailed {
		t.Fatalf("expected persisted task run status failed, got %q", persisted.taskRunStatus)
	}
	if persisted.failureReason == nil || *persisted.failureReason != "random failure" {
		t.Fatalf("expected trimmed failure reason, got %#v", persisted.failureReason)
	}
}

func TestPostgresRepositoryCompleteTaskAttemptRejectsInvalidTransition(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	attempt, err := repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
	if err != nil {
		t.Fatalf("create task attempt: %v", err)
	}
	_, err = repo.CompleteTaskAttempt(context.Background(), CompleteTaskAttemptInput{
		TaskAttemptID: attempt.ID,
		TaskRunID:     fixture.taskRunID,
		Success:       true,
	})
	if err != nil {
		t.Fatalf("complete task attempt: %v", err)
	}

	_, err = repo.CompleteTaskAttempt(context.Background(), CompleteTaskAttemptInput{
		TaskAttemptID: attempt.ID,
		TaskRunID:     fixture.taskRunID,
		Success:       true,
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestPostgresRepositoryCompleteTaskAttemptRejectsMissingAttempt(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.CompleteTaskAttempt(context.Background(), CompleteTaskAttemptInput{
		TaskAttemptID: uuid.NewString(),
		TaskRunID:     uuid.NewString(),
		Success:       true,
	})
	if !errors.Is(err, ErrTaskAttemptNotFound) {
		t.Fatalf("expected ErrTaskAttemptNotFound, got %v", err)
	}
}

type completedAttemptState struct {
	attemptStatus      TaskAttemptStatus
	taskRunStatus      TaskRunStatus
	output             map[string]any
	failureReason      *string
	attemptCompletedAt *time.Time
	taskRunCompletedAt *time.Time
}

func loadCompletedAttemptState(t *testing.T, pool *pgxpool.Pool, taskAttemptID, taskRunID string) completedAttemptState {
	t.Helper()

	var state completedAttemptState
	err := pool.QueryRow(context.Background(), `
		SELECT
			task_attempts.status,
			task_runs.status,
			task_runs.output,
			task_attempts.failure_reason,
			task_attempts.completed_at,
			task_runs.completed_at
		FROM task_attempts
		JOIN task_runs
			ON task_runs.id = task_attempts.task_run_id
		WHERE task_attempts.id = $1
			AND task_runs.id = $2
	`, taskAttemptID, taskRunID).Scan(
		&state.attemptStatus,
		&state.taskRunStatus,
		&state.output,
		&state.failureReason,
		&state.attemptCompletedAt,
		&state.taskRunCompletedAt,
	)
	if err != nil {
		t.Fatalf("load completed attempt state: %v", err)
	}

	return state
}
