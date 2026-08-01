package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryCreateTaskAttemptCreatesFirstAttempt(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	attempt, err := repo.CreateTaskAttempt(context.Background(), " "+fixture.taskRunID+" ")
	if err != nil {
		t.Fatalf("create task attempt: %v", err)
	}

	if attempt.TaskRunID != fixture.taskRunID {
		t.Fatalf("expected task run ID %q, got %q", fixture.taskRunID, attempt.TaskRunID)
	}
	if attempt.AttemptNumber != 1 {
		t.Fatalf("expected attempt number 1, got %d", attempt.AttemptNumber)
	}
	if attempt.Status != TaskAttemptStatusRunning {
		t.Fatalf("expected attempt status running, got %q", attempt.Status)
	}

	assertTaskRunAttemptCount(t, pool, fixture.taskRunID, 1)
}

func TestPostgresRepositoryCreateTaskAttemptRejectsSecondAttemptUntilRetriesExist(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	first, err := repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
	if err != nil {
		t.Fatalf("create first task attempt: %v", err)
	}
	_, err = repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
	if !errors.Is(err, ErrTaskAttemptAlreadyExists) {
		t.Fatalf("expected ErrTaskAttemptAlreadyExists, got %v", err)
	}

	if first.AttemptNumber != 1 {
		t.Fatalf("expected first attempt number 1, got %d", first.AttemptNumber)
	}

	assertTaskRunAttemptCount(t, pool, fixture.taskRunID, 1)
}

func TestPostgresRepositoryCreateTaskAttemptRejectsMissingTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.CreateTaskAttempt(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrTaskRunNotFound) {
		t.Fatalf("expected ErrTaskRunNotFound, got %v", err)
	}

	_, err = repo.CreateTaskAttempt(context.Background(), " ")
	if !errors.Is(err, ErrTaskRunNotFound) {
		t.Fatalf("expected ErrTaskRunNotFound for blank task run ID, got %v", err)
	}
}

func TestPostgresRepositoryCreateTaskAttemptConcurrentCallsCreateOnlyFirstAttempt(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	repo := NewPostgresRepository(pool)

	const attemptCount = 4
	errs := make(chan error, attemptCount)
	numbers := make(chan uint, attemptCount)

	var wg sync.WaitGroup
	for i := 0; i < attemptCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			attempt, err := repo.CreateTaskAttempt(context.Background(), fixture.taskRunID)
			if err != nil {
				errs <- err
				return
			}
			numbers <- attempt.AttemptNumber
		}()
	}
	wg.Wait()
	close(errs)
	close(numbers)

	for err := range errs {
		if err != nil && !errors.Is(err, ErrTaskAttemptAlreadyExists) {
			t.Fatalf("unexpected concurrent create error: %v", err)
		}
	}

	var got []uint
	for number := range numbers {
		got = append(got, number)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected only attempt number 1 to be created, got %#v", got)
	}

	assertTaskRunAttemptCount(t, pool, fixture.taskRunID, 1)
}

func assertTaskRunAttemptCount(t *testing.T, pool *pgxpool.Pool, taskRunID string, want int) {
	t.Helper()

	var got int
	err := pool.QueryRow(context.Background(), `
		SELECT attempt_count
		FROM task_runs
		WHERE id = $1
	`, taskRunID).Scan(&got)
	if err != nil {
		t.Fatalf("load task run attempt count: %v", err)
	}
	if got != want {
		t.Fatalf("expected task run attempt_count %d, got %d", want, got)
	}
}
