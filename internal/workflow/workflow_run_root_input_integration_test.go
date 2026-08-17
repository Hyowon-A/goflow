package workflow

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryCreateWorkflowRunStoresInputOnSingleRootTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRootInputWorkflow(t, pool, []string{"root", "child"}, [][2]string{{"root", "child"}})
	repo := NewPostgresRepository(pool)
	input := map[string]any{"document_text": "lecture notes", "title": "OS Week 3"}

	run, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{Input: input})
	if err != nil {
		t.Fatalf("create workflow run: %v", err)
	}

	inputs := taskRunInputsByTaskName(t, pool, run.ID)
	if !reflect.DeepEqual(inputs["root"], input) {
		t.Fatalf("expected root input %#v, got %#v", input, inputs["root"])
	}
	if len(inputs["child"]) != 0 {
		t.Fatalf("expected child input to start empty, got %#v", inputs["child"])
	}
}

func TestPostgresRepositoryCreateWorkflowRunStoresInputOnAllRootTaskRuns(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRootInputWorkflow(t, pool, []string{"root_a", "root_b", "child"}, [][2]string{{"root_a", "child"}})
	repo := NewPostgresRepository(pool)
	input := map[string]any{"document_text": "lecture notes"}

	run, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{Input: input})
	if err != nil {
		t.Fatalf("create workflow run: %v", err)
	}

	inputs := taskRunInputsByTaskName(t, pool, run.ID)
	for _, taskName := range []string{"root_a", "root_b"} {
		if !reflect.DeepEqual(inputs[taskName], input) {
			t.Fatalf("expected %s input %#v, got %#v", taskName, input, inputs[taskName])
		}
	}
	if len(inputs["child"]) != 0 {
		t.Fatalf("expected child input to start empty, got %#v", inputs["child"])
	}
}

func TestPostgresRepositoryCreateWorkflowRunIdempotencyReplayKeepsRootInput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRootInputWorkflow(t, pool, []string{"root", "child"}, [][2]string{{"root", "child"}})
	repo := NewPostgresRepository(pool)
	input := map[string]any{"document_text": "lecture notes", "title": "OS Week 3"}

	first, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{
		Input:          input,
		IdempotencyKey: "root-input-key",
	})
	if err != nil {
		t.Fatalf("create workflow run: %v", err)
	}
	before := taskRunInputsByTaskName(t, pool, first.ID)

	second, err := repo.CreateWorkflowRun(context.Background(), fixture.workflowID, CreateWorkflowRunInput{
		Input:          input,
		IdempotencyKey: "root-input-key",
	})
	if err != nil {
		t.Fatalf("replay workflow run: %v", err)
	}
	after := taskRunInputsByTaskName(t, pool, first.ID)

	if first.ID != second.ID || !second.IdempotencyReused {
		t.Fatalf("expected idempotency replay of %s, got %#v", first.ID, second)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("expected replay to keep task inputs:\n before %#v\n after  %#v", before, after)
	}
	expectTaskRunRows(t, pool, fixture.workflowID, 2)
}

type rootInputWorkflowFixture struct {
	workflowName string
	workflowID   string
}

func seedRootInputWorkflow(t *testing.T, pool *pgxpool.Pool, taskNames []string, dependencies [][2]string) rootInputWorkflowFixture {
	t.Helper()

	ctx := context.Background()
	service := NewService(NewPostgresRepository(pool))
	fixture := rootInputWorkflowFixture{
		workflowName: fmt.Sprintf("day16-root-input-%d", time.Now().UnixNano()),
	}

	created, err := service.CreateWorkflow(ctx, CreateWorkflowInput{Name: fixture.workflowName})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	fixture.workflowID = created.ID

	tasks := map[string]Task{}
	for _, name := range taskNames {
		task, err := service.CreateTask(ctx, created.ID, CreateTaskInput{
			Name:         name,
			ExecutorType: "log",
		})
		if err != nil {
			t.Fatalf("create task %s: %v", name, err)
		}
		tasks[name] = task
	}
	for _, dependency := range dependencies {
		if _, err := service.CreateDependency(ctx, created.ID, CreateDependencyInput{
			PredecessorTaskID: tasks[dependency[0]].ID,
			SuccessorTaskID:   tasks[dependency[1]].ID,
		}); err != nil {
			t.Fatalf("create dependency %s->%s: %v", dependency[0], dependency[1], err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}

func taskRunInputsByTaskName(t *testing.T, pool *pgxpool.Pool, workflowRunID string) map[string]map[string]any {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT tasks.name, task_runs.input
		FROM task_runs
		JOIN tasks ON tasks.id = task_runs.task_id
		WHERE task_runs.workflow_run_id = $1
	`, workflowRunID)
	if err != nil {
		t.Fatalf("query task run inputs: %v", err)
	}
	defer rows.Close()

	inputs := map[string]map[string]any{}
	for rows.Next() {
		var taskName string
		var input map[string]any
		if err := rows.Scan(&taskName, &input); err != nil {
			t.Fatalf("scan task run input: %v", err)
		}
		if input == nil {
			input = map[string]any{}
		}
		inputs[taskName] = input
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate task run inputs: %v", err)
	}

	return inputs
}
