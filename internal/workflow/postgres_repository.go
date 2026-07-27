package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{db: db}
}

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
