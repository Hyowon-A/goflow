package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryCreateWorkflowRunWithoutIdempotencyKeyCreatesNewRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowForIdempotency(t, pool)
	repo := NewPostgresRepository(pool)
	input := CreateWorkflowRunInput{Input: map[string]any{"document_id": "doc-123"}}

	first, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, input)
	if err != nil {
		t.Fatalf("create first workflow run: %v", err)
	}
	second, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, input)
	if err != nil {
		t.Fatalf("create second workflow run: %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("expected separate workflow runs without idempotency key, got %s twice", first.ID)
	}
	expectWorkflowRunRows(t, pool, fixture.workflowID, 2)
	expectTaskRunRows(t, pool, fixture.workflowID, 2)
}

func TestPostgresRepositoryCreateWorkflowRunReturnsExistingRunForMatchingIdempotencyKey(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowForIdempotency(t, pool)
	repo := NewPostgresRepository(pool)
	input := CreateWorkflowRunInput{
		Input:          map[string]any{"document_id": "doc-123"},
		IdempotencyKey: " day10-key ",
	}

	first, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, input)
	if err != nil {
		t.Fatalf("create first workflow run: %v", err)
	}
	second, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{
		Input:          map[string]any{"document_id": "doc-123"},
		IdempotencyKey: "day10-key",
	})
	if err != nil {
		t.Fatalf("replay workflow run: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected replay to return workflow run %s, got %s", first.ID, second.ID)
	}
	if got := storedWorkflowRunRequestHash(t, pool, first.ID); got == "" {
		t.Fatal("expected workflow run request hash to be stored")
	}
	expectWorkflowRunRows(t, pool, fixture.workflowID, 1)
	expectTaskRunRows(t, pool, fixture.workflowID, 1)
}

func TestPostgresRepositoryCreateWorkflowRunRejectsConflictingIdempotencyKey(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowForIdempotency(t, pool)
	repo := NewPostgresRepository(pool)

	_, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{
		Input:          map[string]any{"document_id": "doc-123"},
		IdempotencyKey: "day10-conflict-key",
	})
	if err != nil {
		t.Fatalf("create first workflow run: %v", err)
	}

	_, err = repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{
		Input:          map[string]any{"document_id": "doc-456"},
		IdempotencyKey: "day10-conflict-key",
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
	expectWorkflowRunRows(t, pool, fixture.workflowID, 1)
	expectTaskRunRows(t, pool, fixture.workflowID, 1)
}

type idempotencyWorkflowFixture struct {
	workflowName string
	workflowID   string
}

func seedWorkflowForIdempotency(t *testing.T, pool *pgxpool.Pool) idempotencyWorkflowFixture {
	t.Helper()

	ctx := context.Background()
	fixture := idempotencyWorkflowFixture{
		workflowName: fmt.Sprintf("day10-idempotency-%d", time.Now().UnixNano()),
		workflowID:   uuid.NewString(),
	}
	taskID := uuid.NewString()

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type)
		VALUES ($1, $2, $3, $4)
	`, taskID, fixture.workflowID, "extract", "log")
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}

func expectWorkflowRunRows(t *testing.T, pool *pgxpool.Pool, workflowID string, want int) {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs WHERE workflow_id = $1`, workflowID).Scan(&count); err != nil {
		t.Fatalf("count workflow runs: %v", err)
	}
	if count != want {
		t.Fatalf("expected %d workflow runs, got %d", want, count)
	}
}

func expectTaskRunRows(t *testing.T, pool *pgxpool.Pool, workflowID string, want int) {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM task_runs WHERE workflow_id = $1`, workflowID).Scan(&count); err != nil {
		t.Fatalf("count task runs: %v", err)
	}
	if count != want {
		t.Fatalf("expected %d task runs, got %d", want, count)
	}
}

func storedWorkflowRunRequestHash(t *testing.T, pool *pgxpool.Pool, workflowRunID string) string {
	t.Helper()

	var hash string
	if err := pool.QueryRow(context.Background(), `
		SELECT request_hash
		FROM workflow_runs
		WHERE id = $1
	`, workflowRunID).Scan(&hash); err != nil {
		t.Fatalf("load workflow run request hash: %v", err)
	}
	return hash
}
