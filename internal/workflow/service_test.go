package workflow

import (
	"context"
	"errors"
	"testing"
)

type fakeRepository struct {
	createDepErr error

	createdDependencies []CreateDependencyInput
}

func (r *fakeRepository) CreateWorkflow(context.Context, CreateWorkflowInput) (Workflow, error) {
	return Workflow{}, ErrNotImplemented
}

func (r *fakeRepository) CreateTask(context.Context, string, CreateTaskInput) (Task, error) {
	return Task{}, ErrNotImplemented
}

func (r *fakeRepository) CreateDependency(_ context.Context, workflowID string, input CreateDependencyInput) (Dependency, error) {
	if r.createDepErr != nil {
		return Dependency{}, r.createDepErr
	}

	r.createdDependencies = append(r.createdDependencies, input)
	return Dependency{
		WorkflowID:        workflowID,
		PredecessorTaskID: input.PredecessorTaskID,
		SuccessorTaskID:   input.SuccessorTaskID,
	}, nil
}

func (r *fakeRepository) CreateWorkflowRun(context.Context, string, CreateWorkflowRunInput) (WorkflowRun, error) {
	return WorkflowRun{}, ErrNotImplemented
}

func (r *fakeRepository) GetWorkflowRun(context.Context, string, string) (WorkflowRun, error) {
	return WorkflowRun{}, ErrNotImplemented
}

func (r *fakeRepository) ListTaskRuns(context.Context, string, string) ([]TaskRun, error) {
	return nil, ErrNotImplemented
}

func (r *fakeRepository) ListTaskAttempts(context.Context, string, string, string) ([]TaskAttempt, error) {
	return nil, ErrNotImplemented
}

func TestServiceCreateDependencyPreservesDependencyCycleError(t *testing.T) {
	repo := &fakeRepository{
		createDepErr: ErrDependencyCycle,
	}
	service := NewService(repo)

	_, err := service.CreateDependency(context.Background(), "workflow", CreateDependencyInput{
		PredecessorTaskID: "B",
		SuccessorTaskID:   "A",
	})
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle, got %v", err)
	}
}

func TestServiceCreateDependencyTrimsAndPersistsValidInput(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo)

	dependency, err := service.CreateDependency(context.Background(), " workflow ", CreateDependencyInput{
		PredecessorTaskID: " A ",
		SuccessorTaskID:   " B ",
	})
	if err != nil {
		t.Fatalf("create dependency: %v", err)
	}

	if dependency.WorkflowID != "workflow" {
		t.Fatalf("expected trimmed workflow ID, got %q", dependency.WorkflowID)
	}
	if dependency.PredecessorTaskID != "A" || dependency.SuccessorTaskID != "B" {
		t.Fatalf("expected trimmed dependency IDs, got %#v", dependency)
	}
	if len(repo.createdDependencies) != 1 {
		t.Fatalf("expected one persisted dependency, got %#v", repo.createdDependencies)
	}
}

func TestServiceCreateDependencyPreservesDuplicateDependencyError(t *testing.T) {
	repo := &fakeRepository{
		createDepErr: ErrDuplicateDependency,
	}
	service := NewService(repo)

	_, err := service.CreateDependency(context.Background(), "workflow", CreateDependencyInput{
		PredecessorTaskID: "A",
		SuccessorTaskID:   "B",
	})
	if !errors.Is(err, ErrDuplicateDependency) {
		t.Fatalf("expected ErrDuplicateDependency, got %v", err)
	}
}

func TestServiceCreateDependencyRejectsMissingTaskReference(t *testing.T) {
	repo := &fakeRepository{
		createDepErr: ErrInvalidTaskReference,
	}
	service := NewService(repo)

	_, err := service.CreateDependency(context.Background(), "workflow", CreateDependencyInput{
		PredecessorTaskID: "A",
		SuccessorTaskID:   "B",
	})
	if !errors.Is(err, ErrInvalidTaskReference) {
		t.Fatalf("expected ErrInvalidTaskReference, got %v", err)
	}
}

func TestServiceCreateDependencyRejectsCrossWorkflowTaskReference(t *testing.T) {
	repo := &fakeRepository{
		createDepErr: ErrInvalidTaskReference,
	}
	service := NewService(repo)

	_, err := service.CreateDependency(context.Background(), "workflow", CreateDependencyInput{
		PredecessorTaskID: "A",
		SuccessorTaskID:   "other-workflow-task",
	})
	if !errors.Is(err, ErrInvalidTaskReference) {
		t.Fatalf("expected ErrInvalidTaskReference, got %v", err)
	}
}
