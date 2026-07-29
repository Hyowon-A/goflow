package workflow

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (r *PostgresRepository) CreateWorkflow(ctx context.Context, input CreateWorkflowInput) (Workflow, error) {
	workflowID := uuid.NewString()

	var workflow Workflow
	err := r.db.QueryRow(ctx, `
		INSERT INTO workflows (id, name, input_schema, output_schema)
		VALUES ($1, $2, $3, $4)
		RETURNING id, name, input_schema, output_schema
	`, workflowID, input.Name, input.InputSchema, input.OutputSchema).Scan(
		&workflow.ID,
		&workflow.Name,
		&workflow.InputSchema,
		&workflow.OutputSchema,
	)
	if err != nil {
		return Workflow{}, fmt.Errorf("create workflow: %w", err)
	}

	return workflow, nil
}
