package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *PostgresRepository) CreateDependency(ctx context.Context, workflowID string, input CreateDependencyInput) (Dependency, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Dependency{}, fmt.Errorf("begin create dependency: %w", err)
	}
	defer tx.Rollback(ctx)

	var lockedWorkflowID string
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM workflows
		WHERE id = $1
		FOR UPDATE
	`, workflowID).Scan(&lockedWorkflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Dependency{}, ErrWorkflowNotFound
		}
		return Dependency{}, fmt.Errorf("lock workflow for dependency creation: %w", err)
	}

	taskIDs, err := loadWorkflowTaskIDsInTx(ctx, tx, workflowID)
	if err != nil {
		return Dependency{}, err
	}

	edges, err := loadWorkflowDependencyEdgesInTx(ctx, tx, workflowID)
	if err != nil {
		return Dependency{}, err
	}

	edges = append(edges, DependencyEdge{
		PredecessorTaskID: input.PredecessorTaskID,
		SuccessorTaskID:   input.SuccessorTaskID,
	})
	if _, err := NewWorkflowGraph(taskIDs, edges); err != nil {
		return Dependency{}, err
	}

	var dependency Dependency
	err = tx.QueryRow(ctx, `
		INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id)
		VALUES ($1, $2, $3)
		RETURNING workflow_id, predecessor_task_id, successor_task_id
	`, workflowID, input.PredecessorTaskID, input.SuccessorTaskID).Scan(
		&dependency.WorkflowID,
		&dependency.PredecessorTaskID,
		&dependency.SuccessorTaskID,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.ConstraintName == "pk_task_dependencies" {
				return Dependency{}, ErrDuplicateDependency
			}
			if pgErr.ConstraintName == "fk_task_dependencies_predecessor" || pgErr.ConstraintName == "fk_task_dependencies_successor" {
				return Dependency{}, ErrInvalidTaskReference
			}
			if pgErr.ConstraintName == "chk_task_dependencies_not_self" {
				return Dependency{}, ErrSelfDependency
			}
		}

		return Dependency{}, fmt.Errorf("create dependency: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Dependency{}, fmt.Errorf("commit create dependency: %w", err)
	}

	return dependency, nil
}

func loadWorkflowTaskIDsInTx(ctx context.Context, tx pgx.Tx, workflowID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT id
		FROM tasks
		WHERE workflow_id = $1
		ORDER BY created_at, id
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("load workflow task ids: %w", err)
	}
	defer rows.Close()

	var taskIDs []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			return nil, fmt.Errorf("scan workflow task id: %w", err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow task ids: %w", err)
	}

	return taskIDs, nil
}

func loadWorkflowDependencyEdgesInTx(ctx context.Context, tx pgx.Tx, workflowID string) ([]DependencyEdge, error) {
	rows, err := tx.Query(ctx, `
		SELECT predecessor_task_id, successor_task_id
		FROM task_dependencies
		WHERE workflow_id = $1
		ORDER BY predecessor_task_id, successor_task_id
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("load workflow dependency edges: %w", err)
	}
	defer rows.Close()

	var edges []DependencyEdge
	for rows.Next() {
		var edge DependencyEdge
		if err := rows.Scan(&edge.PredecessorTaskID, &edge.SuccessorTaskID); err != nil {
			return nil, fmt.Errorf("scan workflow dependency edge: %w", err)
		}
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow dependency edges: %w", err)
	}

	return edges, nil
}
