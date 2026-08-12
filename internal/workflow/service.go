package workflow

import (
	"context"
	"strings"
)

type Repository interface {
	CreateWorkflow(ctx context.Context, input CreateWorkflowInput) (Workflow, error)
	CreateTask(ctx context.Context, workflowID string, input CreateTaskInput) (Task, error)
	CreateDependency(ctx context.Context, workflowID string, input CreateDependencyInput) (Dependency, error)
	CreateWorkflowRun(ctx context.Context, workflowID string, input CreateWorkflowRunInput) (WorkflowRun, error)
	GetWorkflowRun(ctx context.Context, workflowID, workflowRunID string) (WorkflowRun, error)
	ListTaskRuns(ctx context.Context, workflowID, workflowRunID string) ([]TaskRun, error)
	ListTaskAttempts(ctx context.Context, workflowID, workflowRunID, taskRunID string) ([]TaskAttempt, error)
}

type Service struct {
	repo    Repository
	metrics metricsCounter
}

type metricsCounter interface {
	Inc(string)
}

func NewService(repo Repository) *Service {
	return NewServiceWithMetrics(repo, nil)
}

func NewServiceWithMetrics(repo Repository, metrics metricsCounter) *Service {
	return &Service{repo: repo, metrics: metrics}
}

func (s *Service) CreateWorkflow(ctx context.Context, input CreateWorkflowInput) (Workflow, error) {
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		return Workflow{}, ErrValidation
	}

	return s.repo.CreateWorkflow(ctx, input)
}

func (s *Service) CreateTask(ctx context.Context, workflowID string, input CreateTaskInput) (Task, error) {
	workflowID = strings.TrimSpace(workflowID)
	input.Name = strings.TrimSpace(input.Name)
	input.ExecutorType = strings.TrimSpace(input.ExecutorType)
	if workflowID == "" {
		return Task{}, ErrWorkflowNotFound
	}
	if input.Name == "" {
		return Task{}, ErrValidation
	}
	if input.ExecutorType == "" {
		return Task{}, ErrValidation
	}

	return s.repo.CreateTask(ctx, workflowID, input)
}

func (s *Service) CreateDependency(ctx context.Context, workflowID string, input CreateDependencyInput) (Dependency, error) {
	workflowID = strings.TrimSpace(workflowID)
	input.PredecessorTaskID = strings.TrimSpace(input.PredecessorTaskID)
	input.SuccessorTaskID = strings.TrimSpace(input.SuccessorTaskID)

	if workflowID == "" {
		return Dependency{}, ErrWorkflowNotFound
	}
	if input.PredecessorTaskID == "" || input.SuccessorTaskID == "" {
		return Dependency{}, ErrValidation
	}

	return s.repo.CreateDependency(ctx, workflowID, input)
}

func (s *Service) CreateWorkflowRun(ctx context.Context, workflowID string, input CreateWorkflowRunInput) (WorkflowRun, error) {
	workflowID = strings.TrimSpace(workflowID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)

	if workflowID == "" {
		return WorkflowRun{}, ErrWorkflowNotFound
	}

	workflowRun, err := s.repo.CreateWorkflowRun(ctx, workflowID, input)
	if err != nil {
		return WorkflowRun{}, err
	}
	if !workflowRun.IdempotencyReused && s.metrics != nil {
		s.metrics.Inc("goflow_workflow_runs_started_total")
	}
	return workflowRun, nil
}

func (s *Service) GetWorkflowRun(ctx context.Context, workflowID, workflowRunID string) (WorkflowRun, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowID == "" || workflowRunID == "" {
		return WorkflowRun{}, ErrWorkflowRunNotFound
	}

	return s.repo.GetWorkflowRun(ctx, workflowID, workflowRunID)
}

func (s *Service) ListTaskRuns(ctx context.Context, workflowID, workflowRunID string) ([]TaskRun, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowID == "" || workflowRunID == "" {
		return nil, ErrWorkflowRunNotFound
	}

	return s.repo.ListTaskRuns(ctx, workflowID, workflowRunID)
}

func (s *Service) ListTaskAttempts(ctx context.Context, workflowID, workflowRunID, taskRunID string) ([]TaskAttempt, error) {
	workflowID = strings.TrimSpace(workflowID)
	workflowRunID = strings.TrimSpace(workflowRunID)
	taskRunID = strings.TrimSpace(taskRunID)
	if workflowID == "" || workflowRunID == "" {
		return nil, ErrWorkflowRunNotFound
	}
	if taskRunID == "" {
		return nil, ErrTaskRunNotFound
	}

	return s.repo.ListTaskAttempts(ctx, workflowID, workflowRunID, taskRunID)
}
