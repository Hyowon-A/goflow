package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *PostgresRepository) CreateWorkflowRun(ctx context.Context, workflowID string, input CreateWorkflowRunInput) (WorkflowRun, error) {
	workflowRunID := uuid.NewString()

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("begin create workflow run: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id
		FROM tasks
		WHERE workflow_id = $1
		ORDER BY created_at, id
	`, workflowID)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("list workflow tasks: %w", err)
	}

	var taskIDs []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			rows.Close()
			return WorkflowRun{}, fmt.Errorf("scan workflow task: %w", err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return WorkflowRun{}, fmt.Errorf("iterate workflow tasks: %w", err)
	}
	rows.Close()

	if len(taskIDs) == 0 {
		var workflowExists bool
		err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM workflows WHERE id = $1
			)
		`, workflowID).Scan(&workflowExists)
		if err != nil {
			return WorkflowRun{}, fmt.Errorf("check workflow exists: %w", err)
		}
		if !workflowExists {
			return WorkflowRun{}, ErrWorkflowNotFound
		}

		return WorkflowRun{}, ErrEmptyWorkflow
	}

	var workflowRun WorkflowRun

	err = tx.QueryRow(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status, input)
		VALUES ($1, $2, $3, $4)
		RETURNING id, workflow_id, status, input
	`, workflowRunID, workflowID, "pending", input.Input).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&workflowRun.Status,
		&workflowRun.Input,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.ConstraintName == "fk_workflow_runs_workflow" {
				return WorkflowRun{}, ErrWorkflowNotFound
			}
		}

		return WorkflowRun{}, fmt.Errorf("create workflowRun: %w", err)
	}

	for _, taskID := range taskIDs {
		taskRunID := uuid.NewString()
		_, err := tx.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, attempt_count)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, taskRunID, workflowID, workflowRunID, taskID, "pending", 0)
		if err != nil {
			return WorkflowRun{}, fmt.Errorf("create task run: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return WorkflowRun{}, fmt.Errorf("commit create workflow run: %w", err)
	}

	return workflowRun, nil
}

func (r *PostgresRepository) ClaimTaskRun(ctx context.Context, input ClaimTaskRunInput) (TaskRun, error) {
	taskRunID := strings.TrimSpace(input.TaskRunID)

	if taskRunID == "" || strings.TrimSpace(input.WorkerID) == "" {
		return TaskRun{}, ErrTaskRunNotClaimable
	}
	if err := ValidateTaskRunTransition(TaskRunStatusQueued, TaskRunStatusRunning); err != nil {
		return TaskRun{}, err
	}

	var taskRun TaskRun
	err := r.db.QueryRow(ctx, `
		UPDATE task_runs
		SET status = $2,
			started_at = COALESCE(started_at, now())
		WHERE id = $1
			AND status = $3
		RETURNING id, workflow_id, workflow_run_id, task_id, status
	`, taskRunID, TaskRunStatusRunning, TaskRunStatusQueued).Scan(
		&taskRun.ID,
		&taskRun.WorkflowID,
		&taskRun.WorkflowRunID,
		&taskRun.TaskID,
		&taskRun.Status,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskRun{}, ErrTaskRunNotClaimable
		}
		return TaskRun{}, fmt.Errorf("claim task run: %w", err)
	}

	return taskRun, nil
}

func (r *PostgresRepository) LoadTaskRunExecution(ctx context.Context, input LoadTaskRunExecutionInput) (TaskRunExecution, error) {
	taskRunID := strings.TrimSpace(input.TaskRunID)
	workflowID := strings.TrimSpace(input.WorkflowID)
	workflowRunID := strings.TrimSpace(input.WorkflowRunID)
	taskID := strings.TrimSpace(input.TaskID)

	if taskRunID == "" || workflowID == "" || workflowRunID == "" || taskID == "" {
		return TaskRunExecution{}, ErrTaskRunExecutionNotFound
	}

	var taskRunExecution TaskRunExecution
	err := r.db.QueryRow(ctx, `
		SELECT
			task_runs.workflow_id,
			task_runs.workflow_run_id,
			task_runs.task_id,
			task_runs.id,
			tasks.executor_type,
			tasks.config,
			task_runs.input
		FROM task_runs
		JOIN tasks
			ON tasks.id = task_runs.task_id
			AND tasks.workflow_id = task_runs.workflow_id
		WHERE task_runs.id = $1
			AND task_runs.workflow_id = $2
			AND task_runs.workflow_run_id = $3
			AND task_runs.task_id = $4
	`, taskRunID, workflowID, workflowRunID, taskID).Scan(
		&taskRunExecution.WorkflowID,
		&taskRunExecution.WorkflowRunID,
		&taskRunExecution.TaskID,
		&taskRunExecution.TaskRunID,
		&taskRunExecution.ExecutorType,
		&taskRunExecution.Config,
		&taskRunExecution.TaskRunInput,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskRunExecution{}, ErrTaskRunExecutionNotFound
		}
		return TaskRunExecution{}, fmt.Errorf("Load task run execution: %w", err)
	}

	return taskRunExecution, nil
}
