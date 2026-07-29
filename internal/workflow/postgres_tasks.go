package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
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
