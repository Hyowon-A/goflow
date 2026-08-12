package workflow

import (
	"context"
	"errors"
	"testing"
)

type fakeRepository struct {
	createDepErr error
	createRunErr error
	workflowRun  WorkflowRun

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
	if r.createRunErr != nil {
		return WorkflowRun{}, r.createRunErr
	}
	return r.workflowRun, nil
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

type fakeMetrics struct {
	counts map[string]int
}

func (m *fakeMetrics) Inc(name string) {
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[name]++
}

func TestServiceCreateWorkflowRunIncrementsStartedMetricForNewRun(t *testing.T) {
	repo := &fakeRepository{
		workflowRun: WorkflowRun{ID: "run-1"},
	}
	metrics := &fakeMetrics{}
	service := NewServiceWithMetrics(repo, metrics)

	if _, err := service.CreateWorkflowRun(context.Background(), "workflow", CreateWorkflowRunInput{}); err != nil {
		t.Fatalf("create workflow run: %v", err)
	}

	if got := metrics.counts["goflow_workflow_runs_started_total"]; got != 1 {
		t.Fatalf("expected started metric to increment once, got %d", got)
	}
}

func TestServiceCreateWorkflowRunDoesNotIncrementStartedMetricForIdempotencyReplay(t *testing.T) {
	repo := &fakeRepository{
		workflowRun: WorkflowRun{ID: "run-1", IdempotencyReused: true},
	}
	metrics := &fakeMetrics{}
	service := NewServiceWithMetrics(repo, metrics)

	if _, err := service.CreateWorkflowRun(context.Background(), "workflow", CreateWorkflowRunInput{}); err != nil {
		t.Fatalf("create workflow run: %v", err)
	}

	if got := metrics.counts["goflow_workflow_runs_started_total"]; got != 0 {
		t.Fatalf("expected started metric to stay zero, got %d", got)
	}
}
