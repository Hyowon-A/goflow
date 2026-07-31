package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryLoadTaskRunExecutionLoadsDefinitionAndInput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunExecution(t, pool)
	repo := NewPostgresRepository(pool)

	execution, err := repo.LoadTaskRunExecution(context.Background(), LoadTaskRunExecutionInput{
		TaskRunID:     " " + fixture.taskRunID + " ",
		WorkflowID:    " " + fixture.workflowID + " ",
		WorkflowRunID: " " + fixture.workflowRunID + " ",
		TaskID:        " " + fixture.taskID + " ",
	})
	if err != nil {
		t.Fatalf("load task run execution: %v", err)
	}

	if execution.WorkflowID != fixture.workflowID {
		t.Fatalf("expected workflow ID %q, got %q", fixture.workflowID, execution.WorkflowID)
	}
	if execution.WorkflowRunID != fixture.workflowRunID {
		t.Fatalf("expected workflow run ID %q, got %q", fixture.workflowRunID, execution.WorkflowRunID)
	}
	if execution.TaskID != fixture.taskID {
		t.Fatalf("expected task ID %q, got %q", fixture.taskID, execution.TaskID)
	}
	if execution.TaskRunID != fixture.taskRunID {
		t.Fatalf("expected task run ID %q, got %q", fixture.taskRunID, execution.TaskRunID)
	}
	if execution.ExecutorType != "log" {
		t.Fatalf("expected executor type log, got %q", execution.ExecutorType)
	}
	if !reflect.DeepEqual(execution.Config, fixture.config) {
		t.Fatalf("unexpected config: got %#v, want %#v", execution.Config, fixture.config)
	}
	if !reflect.DeepEqual(execution.TaskRunInput, fixture.taskRunInput) {
		t.Fatalf("unexpected task run input: got %#v, want %#v", execution.TaskRunInput, fixture.taskRunInput)
	}
}

func TestPostgresRepositoryLoadTaskRunExecutionRejectsMissingTaskRun(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.LoadTaskRunExecution(context.Background(), LoadTaskRunExecutionInput{
		TaskRunID:     uuid.NewString(),
		WorkflowID:    uuid.NewString(),
		WorkflowRunID: uuid.NewString(),
		TaskID:        uuid.NewString(),
	})
	if !errors.Is(err, ErrTaskRunExecutionNotFound) {
		t.Fatalf("expected ErrTaskRunExecutionNotFound, got %v", err)
	}
}

func TestPostgresRepositoryLoadTaskRunExecutionRejectsMismatchedQueueIDs(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedTaskRunExecution(t, pool)
	repo := NewPostgresRepository(pool)

	tests := []struct {
		name  string
		input LoadTaskRunExecutionInput
	}{
		{
			name: "wrong workflow id",
			input: LoadTaskRunExecutionInput{
				TaskRunID:     fixture.taskRunID,
				WorkflowID:    uuid.NewString(),
				WorkflowRunID: fixture.workflowRunID,
				TaskID:        fixture.taskID,
			},
		},
		{
			name: "wrong workflow run id",
			input: LoadTaskRunExecutionInput{
				TaskRunID:     fixture.taskRunID,
				WorkflowID:    fixture.workflowID,
				WorkflowRunID: uuid.NewString(),
				TaskID:        fixture.taskID,
			},
		},
		{
			name: "wrong task id",
			input: LoadTaskRunExecutionInput{
				TaskRunID:     fixture.taskRunID,
				WorkflowID:    fixture.workflowID,
				WorkflowRunID: fixture.workflowRunID,
				TaskID:        uuid.NewString(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.LoadTaskRunExecution(context.Background(), tt.input)
			if !errors.Is(err, ErrTaskRunExecutionNotFound) {
				t.Fatalf("expected ErrTaskRunExecutionNotFound, got %v", err)
			}
		})
	}
}

func TestPostgresRepositoryLoadTaskRunExecutionRejectsBlankIDs(t *testing.T) {
	pool := workflowClaimTestPool(t)
	repo := NewPostgresRepository(pool)

	_, err := repo.LoadTaskRunExecution(context.Background(), LoadTaskRunExecutionInput{
		TaskRunID:     " ",
		WorkflowID:    uuid.NewString(),
		WorkflowRunID: uuid.NewString(),
		TaskID:        uuid.NewString(),
	})
	if !errors.Is(err, ErrTaskRunExecutionNotFound) {
		t.Fatalf("expected ErrTaskRunExecutionNotFound, got %v", err)
	}
}

type taskRunExecutionFixture struct {
	workflowName  string
	workflowID    string
	taskID        string
	workflowRunID string
	taskRunID     string
	config        map[string]any
	taskRunInput  map[string]any
}

func seedTaskRunExecution(t *testing.T, pool *pgxpool.Pool) taskRunExecutionFixture {
	t.Helper()

	ctx := context.Background()
	fixture := taskRunExecutionFixture{
		workflowName:  fmt.Sprintf("day8-load-execution-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		taskID:        uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskRunID:     uuid.NewString(),
		config: map[string]any{
			"message": "from config",
		},
		taskRunInput: map[string]any{
			"message": "from task run input",
		},
	}

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type, config)
		VALUES ($1, $2, $3, $4, $5)
	`, fixture.taskID, fixture.workflowID, "task", "log", fixture.config)
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
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, input)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, fixture.taskRunID, fixture.workflowID, fixture.workflowRunID, fixture.taskID, TaskRunStatusRunning, fixture.taskRunInput)
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
