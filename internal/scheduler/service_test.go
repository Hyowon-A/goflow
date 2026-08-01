package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestServiceQueueRunnableTaskRunsPublishesReturnedTaskRuns(t *testing.T) {
	repo := &fakeRepository{
		taskRuns: []workflow.TaskRun{
			{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1"},
			{ID: "task-run-2", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-2"},
		},
	}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run")
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}

	if !reflect.DeepEqual(repo.workflowRunIDs, []string{"workflow-run"}) {
		t.Fatalf("unexpected workflow run IDs: %#v", repo.workflowRunIDs)
	}
	want := []queue.TaskMessage{
		{WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
		{WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-2", TaskRunID: "task-run-2"},
	}
	if !reflect.DeepEqual(publisher.messages, want) {
		t.Fatalf("unexpected published messages: got %#v, want %#v", publisher.messages, want)
	}
}

func TestServiceQueueRunnableTaskRunsPublishesNothingWhenNoRowsChanged(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	publisher := &fakePublisher{}
	service := NewService(&fakeRepository{}, publisher)

	err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run")
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %#v", publisher.messages)
	}
	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"scheduler_noop"`,
		`"workflow_run_id":"workflow-run"`,
		`"reason":"no_runnable_task_runs"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
		}
	}
}

func TestServiceQueueRunnableTaskRunsPublishesOnlyRowsChangedAcrossDuplicateCalls(t *testing.T) {
	repo := &fakeRepository{
		batches: [][]workflow.TaskRun{
			{{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1"}},
			nil,
		},
	}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	if err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run"); err != nil {
		t.Fatalf("queue first runnable task runs: %v", err)
	}
	if err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run"); err != nil {
		t.Fatalf("queue duplicate runnable task runs: %v", err)
	}

	want := []queue.TaskMessage{
		{WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
	}
	if !reflect.DeepEqual(publisher.messages, want) {
		t.Fatalf("expected duplicate scheduler call to publish only changed rows, got %#v", publisher.messages)
	}
}

func TestServiceQueueRunnableTaskRunsReturnsErrors(t *testing.T) {
	tests := []struct {
		name          string
		repo          *fakeRepository
		publisher     *fakePublisher
		wantErr       error
		wantPublishes int
	}{
		{
			name:    "repository error",
			wantErr: errors.New("repo failed"),
		},
		{
			name: "publish error",
			repo: &fakeRepository{taskRuns: []workflow.TaskRun{
				{ID: "task-run", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task"},
			}},
			wantErr:       errors.New("publish failed"),
			wantPublishes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.repo == nil {
				tt.repo = &fakeRepository{err: tt.wantErr}
			}
			if tt.publisher == nil {
				tt.publisher = &fakePublisher{err: tt.wantErr}
			}
			service := NewService(tt.repo, tt.publisher)

			err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
			if len(tt.publisher.messages) != tt.wantPublishes {
				t.Fatalf("expected %d publish attempts, got %#v", tt.wantPublishes, tt.publisher.messages)
			}
		})
	}
}

type fakeRepository struct {
	taskRuns       []workflow.TaskRun
	batches        [][]workflow.TaskRun
	err            error
	workflowRunIDs []string
}

func (r *fakeRepository) QueueRunnableTaskRuns(_ context.Context, workflowRunID string) ([]workflow.TaskRun, error) {
	r.workflowRunIDs = append(r.workflowRunIDs, workflowRunID)
	if r.err != nil {
		return nil, r.err
	}
	if len(r.batches) > 0 {
		taskRuns := r.batches[0]
		r.batches = r.batches[1:]
		return taskRuns, nil
	}
	return r.taskRuns, nil
}

type fakePublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *fakePublisher) PublishTask(_ context.Context, message queue.TaskMessage) (string, error) {
	p.messages = append(p.messages, message)
	if p.err != nil {
		return "", p.err
	}
	return "message-id", nil
}
