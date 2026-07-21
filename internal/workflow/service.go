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
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
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

	if workflowID == "" {
		return Dependency{}, ErrWorkflowNotFound
	}

	return s.repo.CreateDependency(ctx, workflowID, input)
}

func (s *Service) CreateWorkflowRun(ctx context.Context, workflowID string, input CreateWorkflowRunInput) (WorkflowRun, error) {
	workflowID = strings.TrimSpace(workflowID)

	if workflowID == "" {
		return WorkflowRun{}, ErrWorkflowNotFound
	}

	return s.repo.CreateWorkflowRun(ctx, workflowID, input)
}
