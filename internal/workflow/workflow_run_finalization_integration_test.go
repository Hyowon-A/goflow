package workflow

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryFinalizeWorkflowRunCompletesAllCompletedTaskRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalization(t, pool, map[string]TaskRunStatus{
		"A": TaskRunStatusCompleted,
		"B": TaskRunStatusCompleted,
	})
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if !changed {
		t.Fatal("expected workflow run to change")
	}
	if WorkflowRunStatus(workflowRun.Status) != WorkflowRunStatusCompleted {
		t.Fatalf("expected completed workflow run, got %q", workflowRun.Status)
	}

	status, startedAt, completedAt := loadWorkflowRunFinalizationState(t, pool, fixture.workflowRunID)
	if status != WorkflowRunStatusCompleted {
		t.Fatalf("expected persisted workflow run completed, got %q", status)
	}
	if startedAt == nil {
		t.Fatal("expected workflow run started_at to be set")
	}
	if completedAt == nil {
		t.Fatal("expected workflow run completed_at to be set")
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunFailsWhenAnyTaskRunDeadLetters(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalization(t, pool, map[string]TaskRunStatus{
		"A": TaskRunStatusCompleted,
		"B": TaskRunStatusDeadLetter,
	})
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if !changed {
		t.Fatal("expected workflow run to change")
	}
	if WorkflowRunStatus(workflowRun.Status) != WorkflowRunStatusFailed {
		t.Fatalf("expected failed workflow run, got %q", workflowRun.Status)
	}

	status, _, completedAt := loadWorkflowRunFinalizationState(t, pool, fixture.workflowRunID)
	if status != WorkflowRunStatusFailed {
		t.Fatalf("expected persisted workflow run failed, got %q", status)
	}
	if completedAt == nil {
		t.Fatal("expected workflow run completed_at to be set")
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunLeavesInFlightWorkflowRunUnchanged(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalization(t, pool, map[string]TaskRunStatus{
		"A": TaskRunStatusCompleted,
		"B": TaskRunStatusRetryWait,
	})
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if changed {
		t.Fatal("expected workflow run to stay unchanged")
	}
	if WorkflowRunStatus(workflowRun.Status) != WorkflowRunStatusRunning {
		t.Fatalf("expected running workflow run, got %q", workflowRun.Status)
	}

	status, _, completedAt := loadWorkflowRunFinalizationState(t, pool, fixture.workflowRunID)
	if status != WorkflowRunStatusRunning {
		t.Fatalf("expected persisted workflow run running, got %q", status)
	}
	if completedAt != nil {
		t.Fatalf("expected completed_at to stay empty, got %s", completedAt)
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunIsIdempotent(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalization(t, pool, map[string]TaskRunStatus{
		"A": TaskRunStatusCompleted,
	})
	repo := NewPostgresRepository(pool)

	if _, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID); err != nil || !changed {
		t.Fatalf("first finalize: changed=%t err=%v", changed, err)
	}
	_, _, firstCompletedAt := loadWorkflowRunFinalizationState(t, pool, fixture.workflowRunID)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("second finalize: %v", err)
	}
	if changed {
		t.Fatal("expected second finalize to be unchanged")
	}
	if WorkflowRunStatus(workflowRun.Status) != WorkflowRunStatusCompleted {
		t.Fatalf("expected completed workflow run, got %q", workflowRun.Status)
	}
	_, _, secondCompletedAt := loadWorkflowRunFinalizationState(t, pool, fixture.workflowRunID)
	if firstCompletedAt == nil || secondCompletedAt == nil || !firstCompletedAt.Equal(*secondCompletedAt) {
		t.Fatalf("expected completed_at to stay %v, got %v", firstCompletedAt, secondCompletedAt)
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunStoresSingleLeafOutput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalizationGraph(t, pool,
		map[string]TaskRunStatus{"A": TaskRunStatusCompleted, "B": TaskRunStatusCompleted},
		[][2]string{{"A", "B"}},
	)
	output := map[string]any{"study_set": "ready"}
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], output)
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if !changed {
		t.Fatal("expected workflow run to change")
	}
	if !reflect.DeepEqual(workflowRun.Output, output) {
		t.Fatalf("expected returned output %#v, got %#v", output, workflowRun.Output)
	}

	persisted, err := repo.GetWorkflowRun(context.Background(), fixture.workflowID, fixture.workflowRunID)
	if err != nil {
		t.Fatalf("get workflow run: %v", err)
	}
	if !reflect.DeepEqual(persisted.Output, output) {
		t.Fatalf("expected persisted output %#v, got %#v", output, persisted.Output)
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunStoresMultiLeafOutputByTaskName(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalizationGraph(t, pool,
		map[string]TaskRunStatus{"A": TaskRunStatusCompleted, "B": TaskRunStatusCompleted, "C": TaskRunStatusCompleted},
		[][2]string{{"A", "B"}, {"A", "C"}},
	)
	outputB := map[string]any{"branch": "b"}
	outputC := map[string]any{"branch": "c"}
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], outputB)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], outputC)
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if !changed {
		t.Fatal("expected workflow run to change")
	}
	want := map[string]any{"B": outputB, "C": outputC}
	if !reflect.DeepEqual(workflowRun.Output, want) {
		t.Fatalf("expected output %#v, got %#v", want, workflowRun.Output)
	}
}

func TestPostgresRepositoryFinalizeWorkflowRunLeavesFailedOutputEmpty(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedWorkflowRunForFinalizationGraph(t, pool,
		map[string]TaskRunStatus{"A": TaskRunStatusCompleted, "B": TaskRunStatusDeadLetter},
		[][2]string{{"A", "B"}},
	)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], map[string]any{"partial": "ignored"})
	repo := NewPostgresRepository(pool)

	workflowRun, changed, err := repo.FinalizeWorkflowRun(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}
	if !changed {
		t.Fatal("expected workflow run to change")
	}
	if workflowRun.Output != nil {
		t.Fatalf("expected failed workflow output to stay empty, got %#v", workflowRun.Output)
	}
}

type workflowRunFinalizationFixture struct {
	workflowName  string
	workflowID    string
	workflowRunID string
	taskIDs       map[string]string
}

func seedWorkflowRunForFinalization(t *testing.T, pool *pgxpool.Pool, statuses map[string]TaskRunStatus) workflowRunFinalizationFixture {
	t.Helper()

	taskIDs := map[string]string{}
	for name := range statuses {
		taskIDs[name] = uuid.NewString()
	}
	fixture := workflowRunFinalizationFixture{
		workflowName:  "day13-finalize-" + uuid.NewString(),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskIDs:       taskIDs,
	}
	seedWorkflowRunGraph(t, pool, fixture.workflowName, fixture.workflowID, fixture.workflowRunID, taskIDs, nil, statuses)
	return fixture
}

func seedWorkflowRunForFinalizationGraph(t *testing.T, pool *pgxpool.Pool, statuses map[string]TaskRunStatus, dependencies [][2]string) workflowRunFinalizationFixture {
	t.Helper()

	taskIDs := map[string]string{}
	for name := range statuses {
		taskIDs[name] = uuid.NewString()
	}
	fixture := workflowRunFinalizationFixture{
		workflowName:  "day16-finalize-output-" + uuid.NewString(),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskIDs:       taskIDs,
	}
	seedWorkflowRunGraph(t, pool, fixture.workflowName, fixture.workflowID, fixture.workflowRunID, taskIDs, dependencies, statuses)
	return fixture
}

func loadWorkflowRunFinalizationState(t *testing.T, pool *pgxpool.Pool, workflowRunID string) (WorkflowRunStatus, *time.Time, *time.Time) {
	t.Helper()

	var status WorkflowRunStatus
	var startedAt *time.Time
	var completedAt *time.Time
	err := pool.QueryRow(context.Background(), `
		SELECT status, started_at, completed_at
		FROM workflow_runs
		WHERE id = $1
	`, workflowRunID).Scan(&status, &startedAt, &completedAt)
	if err != nil {
		t.Fatalf("load workflow run finalization state: %v", err)
	}
	return status, startedAt, completedAt
}
