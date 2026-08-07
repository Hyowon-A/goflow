package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *PostgresRepository) CreateWorkflowRun(ctx context.Context, workflowID string, input CreateWorkflowRunInput) (WorkflowRun, error) {
	workflowRunID := uuid.NewString()
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	var idempotencyKeyPtr *string
	var requestHashPtr *string
	if idempotencyKey != "" {
		requestHash, err := workflowRunRequestHash(input.Input)
		if err != nil {
			return WorkflowRun{}, err
		}
		idempotencyKeyPtr = &idempotencyKey
		requestHashPtr = &requestHash
	}

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
		INSERT INTO workflow_runs (id, workflow_id, status, input, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (workflow_id, idempotency_key)
		WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id, workflow_id, status, input
	`, workflowRunID, workflowID, "pending", input.Input, idempotencyKeyPtr, requestHashPtr).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&workflowRun.Status,
		&workflowRun.Input,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) && idempotencyKeyPtr != nil {
			return existingWorkflowRunForIdempotencyKey(ctx, tx, workflowID, *idempotencyKeyPtr, *requestHashPtr)
		}
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

func workflowRunRequestHash(input map[string]any) (string, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("hash workflow run request: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

func existingWorkflowRunForIdempotencyKey(ctx context.Context, tx pgx.Tx, workflowID, idempotencyKey, requestHash string) (WorkflowRun, error) {
	var workflowRun WorkflowRun
	var existingHash *string
	err := tx.QueryRow(ctx, `
		SELECT id, workflow_id, status, input, request_hash
		FROM workflow_runs
		WHERE workflow_id = $1
			AND idempotency_key = $2
	`, workflowID, idempotencyKey).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&workflowRun.Status,
		&workflowRun.Input,
		&existingHash,
	)
	if err != nil {
		return WorkflowRun{}, fmt.Errorf("load idempotent workflow run: %w", err)
	}
	if existingHash == nil || *existingHash != requestHash {
		return WorkflowRun{}, ErrIdempotencyConflict
	}
	workflowRun.IdempotencyReused = true
	return workflowRun, nil
}

func (r *PostgresRepository) GetWorkflowRun(ctx context.Context, workflowID, workflowRunID string) (WorkflowRun, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowID == "" || workflowRunID == "" {
		return WorkflowRun{}, ErrWorkflowRunNotFound
	}

	var workflowRun WorkflowRun
	err := r.db.QueryRow(ctx, `
		SELECT id, workflow_id, status, input
		FROM workflow_runs
		WHERE workflow_id = $1
			AND id = $2
	`, workflowID, workflowRunID).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&workflowRun.Status,
		&workflowRun.Input,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkflowRun{}, ErrWorkflowRunNotFound
		}
		return WorkflowRun{}, fmt.Errorf("get workflow run: %w", err)
	}

	return workflowRun, nil
}

func (r *PostgresRepository) ListTaskRuns(ctx context.Context, workflowID, workflowRunID string) ([]TaskRun, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowID == "" || workflowRunID == "" {
		return nil, ErrWorkflowRunNotFound
	}

	if _, err := r.GetWorkflowRun(ctx, workflowID, workflowRunID); err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, `
		SELECT id, workflow_id, workflow_run_id, task_id, status
		FROM task_runs
		WHERE workflow_id = $1
			AND workflow_run_id = $2
		ORDER BY created_at, id
	`, workflowID, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("list task runs: %w", err)
	}
	defer rows.Close()

	var taskRuns []TaskRun
	for rows.Next() {
		var taskRun TaskRun
		if err := rows.Scan(&taskRun.ID, &taskRun.WorkflowID, &taskRun.WorkflowRunID, &taskRun.TaskID, &taskRun.Status); err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		taskRuns = append(taskRuns, taskRun)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task runs: %w", err)
	}

	return taskRuns, nil
}

func (r *PostgresRepository) ListTaskAttempts(ctx context.Context, workflowID, workflowRunID, taskRunID string) ([]TaskAttempt, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	taskRunID = strings.TrimSpace(taskRunID)
	if workflowID == "" || workflowRunID == "" {
		return nil, ErrWorkflowRunNotFound
	}
	if taskRunID == "" {
		return nil, ErrTaskRunNotFound
	}

	if _, err := r.GetWorkflowRun(ctx, workflowID, workflowRunID); err != nil {
		return nil, err
	}

	var taskRunExists bool
	err := r.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM task_runs
			WHERE workflow_id = $1
				AND workflow_run_id = $2
				AND id = $3
		)
	`, workflowID, workflowRunID, taskRunID).Scan(&taskRunExists)
	if err != nil {
		return nil, fmt.Errorf("check task run for attempts: %w", err)
	}
	if !taskRunExists {
		return nil, ErrTaskRunNotFound
	}

	rows, err := r.db.Query(ctx, `
		SELECT id, task_run_id, attempt_number, status, COALESCE(failure_reason, '')
		FROM task_attempts
		WHERE task_run_id = $1
		ORDER BY attempt_number
	`, taskRunID)
	if err != nil {
		return nil, fmt.Errorf("list task attempts: %w", err)
	}
	defer rows.Close()

	var attempts []TaskAttempt
	for rows.Next() {
		var attempt TaskAttempt
		if err := rows.Scan(&attempt.ID, &attempt.TaskRunID, &attempt.AttemptNumber, &attempt.Status, &attempt.FailureReason); err != nil {
			return nil, fmt.Errorf("scan task attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task attempts: %w", err)
	}

	return attempts, nil
}

func (r *PostgresRepository) LoadTaskRunStatus(ctx context.Context, input LoadTaskRunStatusInput) (TaskRunStatus, error) {
	taskRunID := strings.TrimSpace(input.TaskRunID)
	if taskRunID == "" {
		return "", ErrTaskRunNotFound
	}

	var status TaskRunStatus
	err := r.db.QueryRow(ctx, `
		SELECT status
		FROM task_runs
		WHERE id = $1
	`, taskRunID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrTaskRunNotFound
		}
		return "", fmt.Errorf("load task run status: %w", err)
	}

	return status, nil
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

func (r *PostgresRepository) QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) ([]TaskRun, error) {
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowRunID == "" {
		return []TaskRun{}, ErrWorkflowRunNotFound
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return []TaskRun{}, fmt.Errorf("begin queue runnable task run: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE task_runs tr
		SET status = $2
		WHERE tr.workflow_run_id = $1
			AND tr.status = $3
			AND NOT EXISTS (
				SELECT 1
				FROM task_dependencies d
				LEFT JOIN task_runs pred
				  ON pred.workflow_id = tr.workflow_id
				 AND pred.workflow_run_id = tr.workflow_run_id
				 AND pred.task_id = d.predecessor_task_id
				WHERE d.workflow_id = tr.workflow_id
				  AND d.successor_task_id = tr.task_id
				  AND (pred.id IS NULL OR pred.status <> $4))
		RETURNING id, workflow_id, workflow_run_id, task_id, status
	`, workflowRunID, TaskRunStatusQueued, TaskRunStatusPending, TaskRunStatusCompleted)
	if err != nil {
		return nil, fmt.Errorf("queue runnable task runs: %w", err)
	}
	defer rows.Close()

	var taskRuns []TaskRun
	for rows.Next() {
		var taskRun TaskRun
		if err := rows.Scan(
			&taskRun.ID,
			&taskRun.WorkflowID,
			&taskRun.WorkflowRunID,
			&taskRun.TaskID,
			&taskRun.Status,
		); err != nil {
			return nil, fmt.Errorf("scan queued runnable task run: %w", err)
		}
		taskRuns = append(taskRuns, taskRun)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued runnable task runs: %w", err)
	}
	rows.Close()

	for _, tr := range taskRuns {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), tr.WorkflowID, tr.WorkflowRunID, tr.TaskID, tr.ID)
		if err != nil {
			return nil, fmt.Errorf("create task outbox event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit queue runnable task runs: %w", err)
	}

	return taskRuns, nil
}

func (r *PostgresRepository) QueueDueRetryTaskRuns(ctx context.Context, now time.Time) ([]TaskRun, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return []TaskRun{}, fmt.Errorf("begin queue due retry task run: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE task_runs tr
		SET status = $1
		FROM tasks t
		WHERE t.workflow_id = tr.workflow_id
			AND t.id = tr.task_id
			AND tr.status = $2
			AND tr.next_retry_at <= $3
			AND tr.attempt_count < COALESCE((t.config->'retry'->>'max_attempts')::int, 1)
		RETURNING tr.id, tr.workflow_id, tr.workflow_run_id, tr.task_id, tr.status
	`, TaskRunStatusQueued, TaskRunStatusRetryWait, now)
	if err != nil {
		return nil, fmt.Errorf("queue due retry task runs: %w", err)
	}
	defer rows.Close()

	var taskRuns []TaskRun
	for rows.Next() {
		var taskRun TaskRun
		if err := rows.Scan(
			&taskRun.ID,
			&taskRun.WorkflowID,
			&taskRun.WorkflowRunID,
			&taskRun.TaskID,
			&taskRun.Status,
		); err != nil {
			return nil, fmt.Errorf("scan queued due retry task run: %w", err)
		}
		taskRuns = append(taskRuns, taskRun)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued due retry task runs: %w", err)
	}
	rows.Close()

	for _, tr := range taskRuns {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), tr.WorkflowID, tr.WorkflowRunID, tr.TaskID, tr.ID)
		if err != nil {
			return nil, fmt.Errorf("create retry task outbox event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit queue due retry task runs: %w", err)
	}

	return taskRuns, nil
}

func (r *PostgresRepository) FinalizeWorkflowRun(ctx context.Context, workflowRunID string) (WorkflowRun, bool, error) {
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowRunID == "" {
		return WorkflowRun{}, false, ErrWorkflowRunNotFound
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return WorkflowRun{}, false, fmt.Errorf("begin finalize workflow run: %w", err)
	}
	defer tx.Rollback(ctx)

	var workflowRun WorkflowRun
	var currentStatus WorkflowRunStatus
	err = tx.QueryRow(ctx, `
		SELECT id, workflow_id, status, input
		FROM workflow_runs
		WHERE id = $1
		FOR UPDATE
	`, workflowRunID).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&currentStatus,
		&workflowRun.Input,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkflowRun{}, false, ErrWorkflowRunNotFound
		}
		return WorkflowRun{}, false, fmt.Errorf("load workflow run for finalization: %w", err)
	}
	workflowRun.Status = string(currentStatus)

	if IsTerminalWorkflowRunStatus(currentStatus) {
		return workflowRun, false, nil
	}

	var deadLetterCount int
	var incompleteCount int
	err = tx.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = $2),
			COUNT(*) FILTER (WHERE status <> $3)
		FROM task_runs
		WHERE workflow_run_id = $1
	`, workflowRunID, TaskRunStatusDeadLetter, TaskRunStatusCompleted).Scan(&deadLetterCount, &incompleteCount)
	if err != nil {
		return WorkflowRun{}, false, fmt.Errorf("count task run finalization state: %w", err)
	}

	var nextStatus WorkflowRunStatus
	switch {
	case deadLetterCount > 0:
		nextStatus = WorkflowRunStatusFailed
	case incompleteCount == 0:
		nextStatus = WorkflowRunStatusCompleted
	default:
		return workflowRun, false, nil
	}

	if currentStatus == WorkflowRunStatusPending {
		if err := ValidateWorkflowRunTransition(WorkflowRunStatusPending, WorkflowRunStatusRunning); err != nil {
			return WorkflowRun{}, false, err
		}
		if err := ValidateWorkflowRunTransition(WorkflowRunStatusRunning, nextStatus); err != nil {
			return WorkflowRun{}, false, err
		}
	} else if err := ValidateWorkflowRunTransition(currentStatus, nextStatus); err != nil {
		return WorkflowRun{}, false, err
	}

	err = tx.QueryRow(ctx, `
		UPDATE workflow_runs
		SET status = $2,
			started_at = COALESCE(started_at, now()),
			completed_at = now()
		WHERE id = $1
		RETURNING id, workflow_id, status, input
	`, workflowRunID, nextStatus).Scan(
		&workflowRun.ID,
		&workflowRun.WorkflowID,
		&currentStatus,
		&workflowRun.Input,
	)
	if err != nil {
		return WorkflowRun{}, false, fmt.Errorf("update finalized workflow run: %w", err)
	}
	workflowRun.Status = string(currentStatus)

	if err := tx.Commit(ctx); err != nil {
		return WorkflowRun{}, false, fmt.Errorf("commit finalize workflow run: %w", err)
	}

	return workflowRun, true, nil
}
