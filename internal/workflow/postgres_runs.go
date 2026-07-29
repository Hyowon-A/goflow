package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
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
