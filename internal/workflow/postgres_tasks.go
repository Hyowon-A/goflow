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

func (r *PostgresRepository) CreateTask(ctx context.Context, workflowID string, input CreateTaskInput) (Task, error) {
	taskID := uuid.NewString()

	var task Task
	err := r.db.QueryRow(ctx, `
		INSERT INTO tasks (id, workflow_id, name, executor_type, config, input_schema, output_schema)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, workflow_id, name, executor_type, config, input_schema, output_schema
	`, taskID, workflowID, input.Name, input.ExecutorType, input.Config, input.InputSchema, input.OutputSchema).Scan(
		&task.ID,
		&task.WorkflowID,
		&task.Name,
		&task.ExecutorType,
		&task.Config,
		&task.InputSchema,
		&task.OutputSchema,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.ConstraintName == "fk_tasks_workflow" {
				return Task{}, ErrWorkflowNotFound
			}
			if pgErr.ConstraintName == "uq_tasks_workflow_name" {
				return Task{}, ErrDuplicateTaskName
			}
		}

		return Task{}, fmt.Errorf("create task: %w", err)
	}

	return task, nil
}

func (r *PostgresRepository) CreateTaskAttempt(ctx context.Context, taskRunID string) (TaskAttempt, error) {
	taskRunID = strings.TrimSpace(taskRunID)
	if taskRunID == "" {
		return TaskAttempt{}, ErrTaskRunNotFound
	}

	taskAttemptID := uuid.NewString()

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return TaskAttempt{}, fmt.Errorf("begin create task attempt: %w", err)
	}
	defer tx.Rollback(ctx)

	var attemptNumber uint
	err = tx.QueryRow(ctx, `
		WITH locked_task_run AS (
			SELECT id, attempt_count
			FROM task_runs
			WHERE id = $1
			FOR UPDATE
		),
		next_attempt AS (
			SELECT GREATEST(
				locked_task_run.attempt_count,
				COALESCE(MAX(task_attempts.attempt_number), 0)
			) + 1 AS attempt_number
			FROM locked_task_run
			LEFT JOIN task_attempts
				ON task_attempts.task_run_id = locked_task_run.id
			GROUP BY locked_task_run.attempt_count
		)
		UPDATE task_runs
		SET attempt_count = next_attempt.attempt_number
		FROM next_attempt
		WHERE task_runs.id = $1
		RETURNING next_attempt.attempt_number
	`, taskRunID).Scan(&attemptNumber)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TaskAttempt{}, ErrTaskRunNotFound
		}
		return TaskAttempt{}, fmt.Errorf("reserve task attempt number: %w", err)
	}

	var taskAttempt TaskAttempt
	err = tx.QueryRow(ctx, `
		INSERT INTO task_attempts (id, task_run_id, attempt_number, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id, task_run_id, attempt_number, status
	`, taskAttemptID, taskRunID, attemptNumber, TaskAttemptStatusRunning).Scan(
		&taskAttempt.ID,
		&taskAttempt.TaskRunID,
		&taskAttempt.AttemptNumber,
		&taskAttempt.Status,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.ConstraintName == "fk_task_attempts_task_run" {
				return TaskAttempt{}, ErrTaskRunNotFound
			}
			if pgErr.ConstraintName == "uq_task_attempts_task_run_number" {
				return TaskAttempt{}, fmt.Errorf("create task attempt: %w", err)
			}
		}

		return TaskAttempt{}, fmt.Errorf("create task attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return TaskAttempt{}, fmt.Errorf("commit create task attempt: %w", err)
	}

	return taskAttempt, nil
}

func (r *PostgresRepository) CompleteTaskAttempt(ctx context.Context, input CompleteTaskAttemptInput) (CompleteTaskAttemptResult, error) {
	taskAttemptID := strings.TrimSpace(input.TaskAttemptID)
	taskRunID := strings.TrimSpace(input.TaskRunID)
	if taskAttemptID == "" || taskRunID == "" {
		return CompleteTaskAttemptResult{}, ErrTaskAttemptNotFound
	}

	nextAttemptStatus := TaskAttemptStatusCompleted
	nextTaskRunStatus := TaskRunStatusCompleted
	failureReason := any(nil)
	if !input.Success {
		nextAttemptStatus = TaskAttemptStatusFailed
		nextTaskRunStatus = TaskRunStatusFailed
		reason := strings.TrimSpace(input.FailureReason)
		failureReason = reason
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return CompleteTaskAttemptResult{}, fmt.Errorf("begin complete task attempt: %w", err)
	}
	defer tx.Rollback(ctx)

	var currentAttemptStatus TaskAttemptStatus
	var currentTaskRunStatus TaskRunStatus
	err = tx.QueryRow(ctx, `
		SELECT task_attempts.status, task_runs.status
		FROM task_attempts
		JOIN task_runs
			ON task_runs.id = task_attempts.task_run_id
		WHERE task_attempts.id = $1
			AND task_attempts.task_run_id = $2
		FOR UPDATE OF task_attempts, task_runs
	`, taskAttemptID, taskRunID).Scan(&currentAttemptStatus, &currentTaskRunStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CompleteTaskAttemptResult{}, ErrTaskAttemptNotFound
		}
		return CompleteTaskAttemptResult{}, fmt.Errorf("load running task attempt: %w", err)
	}

	if err := ValidateTaskAttemptTransition(currentAttemptStatus, nextAttemptStatus); err != nil {
		return CompleteTaskAttemptResult{}, err
	}
	if err := ValidateTaskRunTransition(currentTaskRunStatus, nextTaskRunStatus); err != nil {
		return CompleteTaskAttemptResult{}, err
	}

	var result CompleteTaskAttemptResult
	err = tx.QueryRow(ctx, `
		UPDATE task_attempts
		SET status = $3,
			completed_at = now(),
			failure_reason = $4
		WHERE id = $1
			AND task_run_id = $2
		RETURNING id, task_run_id, attempt_number, status
	`, taskAttemptID, taskRunID, nextAttemptStatus, failureReason).Scan(
		&result.TaskAttempt.ID,
		&result.TaskAttempt.TaskRunID,
		&result.TaskAttempt.AttemptNumber,
		&result.TaskAttempt.Status,
	)
	if err != nil {
		return CompleteTaskAttemptResult{}, fmt.Errorf("update task attempt terminal state: %w", err)
	}

	err = tx.QueryRow(ctx, `
		UPDATE task_runs
		SET status = $2,
			output = $3,
			completed_at = now()
		WHERE id = $1
		RETURNING id, workflow_id, workflow_run_id, task_id, status
	`, taskRunID, nextTaskRunStatus, input.Output).Scan(
		&result.TaskRun.ID,
		&result.TaskRun.WorkflowID,
		&result.TaskRun.WorkflowRunID,
		&result.TaskRun.TaskID,
		&result.TaskRun.Status,
	)
	if err != nil {
		return CompleteTaskAttemptResult{}, fmt.Errorf("update task run terminal state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return CompleteTaskAttemptResult{}, fmt.Errorf("commit complete task attempt: %w", err)
	}

	return result, nil
}
