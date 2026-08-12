package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsRequeuesWithAttemptsRemaining(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 1, 2, time.Now().Add(-time.Minute))
	repo := NewPostgresRepository(pool)

	recovered, err := repo.RecoverExpiredRunningTaskRuns(context.Background())
	if err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}
	taskRun := requireRecoveredTaskRun(t, recovered, fixture.taskRunID)
	if taskRun.Status != TaskRunStatusQueued {
		t.Fatalf("expected recovered status queued, got %q", taskRun.Status)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusQueued {
		t.Fatalf("expected persisted status queued, got %q", state.status)
	}
	if state.lockedBy != nil || state.leaseExpiresAt != nil || state.lastHeartbeatAt != nil {
		t.Fatalf("expected lease fields cleared, got locked_by=%#v lease=%v heartbeat=%v", state.lockedBy, state.leaseExpiresAt, state.lastHeartbeatAt)
	}
	assertTaskOutboxEvent(t, pool, taskRun)
}

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsDeadLettersAtMaxAttempts(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 2, 2, time.Now().Add(-time.Minute))
	repo := NewPostgresRepository(pool)

	recovered, err := repo.RecoverExpiredRunningTaskRuns(context.Background())
	if err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}
	taskRun := requireRecoveredTaskRun(t, recovered, fixture.taskRunID)
	if taskRun.Status != TaskRunStatusDeadLetter {
		t.Fatalf("expected recovered status dead_letter, got %q", taskRun.Status)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusDeadLetter {
		t.Fatalf("expected persisted status dead_letter, got %q", state.status)
	}
	if state.lockedBy != nil || state.leaseExpiresAt != nil || state.lastHeartbeatAt != nil {
		t.Fatalf("expected lease fields cleared, got locked_by=%#v lease=%v heartbeat=%v", state.lockedBy, state.leaseExpiresAt, state.lastHeartbeatAt)
	}
	assertTaskOutboxEventCount(t, pool, fixture.taskRunID, 0)
}

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsMarksOpenAttemptFailed(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 1, 2, time.Now().Add(-time.Minute))
	repo := NewPostgresRepository(pool)

	if _, err := repo.RecoverExpiredRunningTaskRuns(context.Background()); err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}

	attempt := loadCompletedAttemptState(t, pool, fixture.attemptID, fixture.taskRunID)
	if attempt.attemptStatus != TaskAttemptStatusFailed {
		t.Fatalf("expected failed attempt, got %q", attempt.attemptStatus)
	}
	if attempt.attemptCompletedAt == nil {
		t.Fatal("expected attempt completed_at to be set")
	}
	if attempt.failureReason == nil || *attempt.failureReason != "lease_expired" {
		t.Fatalf("expected lease_expired failure reason, got %#v", attempt.failureReason)
	}
}

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsLeavesNonExpiredRunningTaskRunUntouched(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 1, 2, time.Now().Add(time.Hour))
	repo := NewPostgresRepository(pool)

	recovered, err := repo.RecoverExpiredRunningTaskRuns(context.Background())
	if err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}
	for _, taskRun := range recovered {
		if taskRun.ID == fixture.taskRunID {
			t.Fatalf("expected non-expired task run to be untouched, got recovered row %#v", taskRun)
		}
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.status != TaskRunStatusRunning {
		t.Fatalf("expected persisted status running, got %q", state.status)
	}
	if state.lockedBy == nil || state.leaseExpiresAt == nil || state.lastHeartbeatAt == nil {
		t.Fatalf("expected lease fields to remain set, got locked_by=%#v lease=%v heartbeat=%v", state.lockedBy, state.leaseExpiresAt, state.lastHeartbeatAt)
	}
	attempt := loadCompletedAttemptState(t, pool, fixture.attemptID, fixture.taskRunID)
	if attempt.attemptStatus != TaskAttemptStatusRunning {
		t.Fatalf("expected running attempt, got %q", attempt.attemptStatus)
	}
	assertTaskOutboxEventCount(t, pool, fixture.taskRunID, 0)
}

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsConcurrentLoopsRecoverOnce(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 1, 2, time.Now().Add(-time.Minute))
	repo := NewPostgresRepository(pool)

	type result struct {
		taskRuns []TaskRun
		err      error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			taskRuns, err := repo.RecoverExpiredRunningTaskRuns(context.Background())
			results <- result{taskRuns: taskRuns, err: err}
		}()
	}
	wg.Wait()
	close(results)

	recoveredCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("recover expired task runs: %v", result.err)
		}
		for _, taskRun := range result.taskRuns {
			if taskRun.ID == fixture.taskRunID {
				recoveredCount++
				if taskRun.LockedBy != "worker-1" {
					t.Fatalf("expected previous worker worker-1, got %q", taskRun.LockedBy)
				}
			}
		}
	}
	if recoveredCount != 1 {
		t.Fatalf("expected one recovery across concurrent loops, got %d", recoveredCount)
	}
	assertTaskOutboxEventCount(t, pool, fixture.taskRunID, 1)
}

func TestPostgresRepositoryRecoverExpiredRunningTaskRunsRecoveredTaskCanBeClaimedByDifferentWorker(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedLeasedRunningTaskRun(t, pool, 1, 2, time.Now().Add(-time.Minute))
	repo := NewPostgresRepository(pool)

	if _, err := repo.RecoverExpiredRunningTaskRuns(context.Background()); err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}

	claimed, err := repo.ClaimTaskRun(context.Background(), ClaimTaskRunInput{
		TaskRunID:     fixture.taskRunID,
		WorkerID:      "worker-2",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim recovered task run: %v", err)
	}
	if claimed.Status != TaskRunStatusRunning {
		t.Fatalf("expected claimed status running, got %q", claimed.Status)
	}

	state := loadTaskRunClaimState(t, pool, fixture.taskRunID)
	if state.lockedBy == nil || *state.lockedBy != "worker-2" {
		t.Fatalf("expected locked_by worker-2, got %#v", state.lockedBy)
	}
}

type leasedRunningTaskRunFixture struct {
	taskRunClaimFixture
	attemptID string
}

func seedLeasedRunningTaskRun(t *testing.T, pool *pgxpool.Pool, attemptCount, maxAttempts int, leaseExpiresAt time.Time) leasedRunningTaskRunFixture {
	t.Helper()

	fixture := seedTaskRunForClaim(t, pool, TaskRunStatusRunning)
	attemptID := uuid.NewString()
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		UPDATE tasks
		SET config = $2
		WHERE id = $1
	`, fixture.taskID, map[string]any{"retry": map[string]any{"max_attempts": maxAttempts}})
	if err != nil {
		t.Fatalf("set retry config: %v", err)
	}
	_, err = pool.Exec(ctx, `
		UPDATE task_runs
		SET attempt_count = $2,
			locked_by = $3,
			lease_expires_at = $4,
			last_heartbeat_at = $5
		WHERE id = $1
	`, fixture.taskRunID, attemptCount, "worker-1", leaseExpiresAt, leaseExpiresAt.Add(-time.Minute))
	if err != nil {
		t.Fatalf("set task run lease: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_attempts (id, task_run_id, attempt_number, status)
		VALUES ($1, $2, $3, $4)
	`, attemptID, fixture.taskRunID, attemptCount, TaskAttemptStatusRunning)
	if err != nil {
		t.Fatalf("insert running attempt: %v", err)
	}

	return leasedRunningTaskRunFixture{
		taskRunClaimFixture: fixture,
		attemptID:           attemptID,
	}
}

func requireRecoveredTaskRun(t *testing.T, taskRuns []TaskRun, taskRunID string) TaskRun {
	t.Helper()

	for _, taskRun := range taskRuns {
		if taskRun.ID == taskRunID {
			return taskRun
		}
	}
	t.Fatalf("expected recovered task run %s, got %#v", taskRunID, taskRuns)
	return TaskRun{}
}
