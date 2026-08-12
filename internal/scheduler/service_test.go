package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestServiceQueueRunnableTaskRunsDispatchesOutboxEvents(t *testing.T) {
	repo := &fakeRepository{
		taskRuns: []workflow.TaskRun{
			{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1"},
			{ID: "task-run-2", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-2"},
		},
		outboxEvents: []workflow.TaskOutboxEvent{
			{ID: "event-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
			{ID: "event-2", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-2", TaskRunID: "task-run-2"},
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
	if len(repo.published) != 2 {
		t.Fatalf("expected two outbox events marked published, got %#v", repo.published)
	}
}

func TestServiceQueueRunnableTaskRunsDispatchesNothingWhenNoRowsChanged(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	repo := &fakeRepository{}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run")
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %#v", publisher.messages)
	}
	if repo.claims != 0 {
		t.Fatalf("expected no outbox claim when no rows changed, got %d", repo.claims)
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

func TestServiceQueueRunnableTaskRunsDispatchesOnlyRowsChangedAcrossDuplicateCalls(t *testing.T) {
	repo := &fakeRepository{
		batches: [][]workflow.TaskRun{
			{{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1"}},
			nil,
		},
		outboxBatches: [][]workflow.TaskOutboxEvent{
			{{ID: "event-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"}},
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
	if len(repo.published) != 1 {
		t.Fatalf("expected one outbox event marked published, got %#v", repo.published)
	}
}

func TestServiceQueueRunnableTaskRunsReturnsErrors(t *testing.T) {
	wantErr := errors.New("repo failed")
	service := NewService(&fakeRepository{err: wantErr}, &fakePublisher{})

	err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestServiceQueueRunnableTaskRunsLogsAndIgnoresDispatchErrorsAfterQueueCommit(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	repo := &fakeRepository{
		taskRuns: []workflow.TaskRun{
			{ID: "task-run", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task"},
		},
		claimErr: errors.New("claim failed"),
	}
	service := NewService(repo, &fakePublisher{})

	if err := service.QueueRunnableTaskRuns(context.Background(), "workflow-run"); err != nil {
		t.Fatalf("expected dispatch error to be logged, got %v", err)
	}
	for _, want := range []string{
		`"msg":"task_outbox_dispatch_failed"`,
		`"workflow_run_id":"workflow-run"`,
		`"error":"claim failed"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logs.String())
		}
	}
}

func TestServiceQueueDueRetryTaskRunsDispatchesOutboxEvents(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepository{
		retryTaskRuns: []workflow.TaskRun{
			{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1"},
		},
		outboxEvents: []workflow.TaskOutboxEvent{
			{ID: "event-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
		},
	}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	err := service.QueueDueRetryTaskRuns(context.Background(), now)
	if err != nil {
		t.Fatalf("queue due retry task runs: %v", err)
	}

	if !reflect.DeepEqual(repo.retryTimes, []time.Time{now}) {
		t.Fatalf("unexpected retry scheduler times: %#v", repo.retryTimes)
	}
	want := []queue.TaskMessage{
		{WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
	}
	if !reflect.DeepEqual(publisher.messages, want) {
		t.Fatalf("unexpected published messages: got %#v, want %#v", publisher.messages, want)
	}
	if len(repo.published) != 1 {
		t.Fatalf("expected one outbox event marked published, got %#v", repo.published)
	}
}

func TestServiceQueueDueRetryTaskRunsDispatchesNothingWhenNoRowsChanged(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	repo := &fakeRepository{}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	err := service.QueueDueRetryTaskRuns(context.Background(), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("queue due retry task runs: %v", err)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("expected no published messages, got %#v", publisher.messages)
	}
	if repo.claims != 0 {
		t.Fatalf("expected no outbox claim when no rows changed, got %d", repo.claims)
	}
	if !strings.Contains(logs.String(), `"reason":"no_due_retry_task_runs"`) {
		t.Fatalf("expected no-op retry scheduler log, got %s", logs.String())
	}
}

func TestServiceQueueDueRetryTaskRunsReturnsErrors(t *testing.T) {
	wantErr := errors.New("repo failed")
	service := NewService(&fakeRepository{retryErr: wantErr}, &fakePublisher{})

	err := service.QueueDueRetryTaskRuns(context.Background(), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestServiceRecoverExpiredRunningTaskRunsLogsAndDispatchesOutboxEvents(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	repo := &fakeRepository{
		recoveredTaskRuns: []workflow.TaskRun{
			{ID: "task-run-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", LockedBy: "worker-1"},
		},
		outboxEvents: []workflow.TaskOutboxEvent{
			{ID: "event-1", WorkflowID: "workflow", WorkflowRunID: "workflow-run", TaskID: "task-1", TaskRunID: "task-run-1"},
		},
	}
	publisher := &fakePublisher{}
	service := NewService(repo, publisher)

	if err := service.RecoverExpiredRunningTaskRuns(context.Background()); err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}

	if !reflect.DeepEqual(publisher.messages, []queue.TaskMessage{{
		WorkflowID:    "workflow",
		WorkflowRunID: "workflow-run",
		TaskID:        "task-1",
		TaskRunID:     "task-run-1",
	}}) {
		t.Fatalf("unexpected published messages: got %#v", publisher.messages)
	}
	if repo.claims != 1 {
		t.Fatalf("expected one outbox claim after recovery, got %d", repo.claims)
	}
	for _, want := range []string{
		`"msg":"expired_task_run_recovered"`,
		`"task_run_id":"task-run-1"`,
		`"previous_worker_id":"worker-1"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logs.String())
		}
	}
}

func TestServiceRecoverExpiredRunningTaskRunsSkipsOutboxWhenNoRowsChanged(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, &fakePublisher{})

	if err := service.RecoverExpiredRunningTaskRuns(context.Background()); err != nil {
		t.Fatalf("recover expired task runs: %v", err)
	}
	if repo.claims != 0 {
		t.Fatalf("expected no outbox claim when no rows recovered, got %d", repo.claims)
	}
}

type fakeRepository struct {
	taskRuns          []workflow.TaskRun
	retryTaskRuns     []workflow.TaskRun
	recoveredTaskRuns []workflow.TaskRun
	batches           [][]workflow.TaskRun
	retryBatches      [][]workflow.TaskRun
	outboxEvents      []workflow.TaskOutboxEvent
	outboxBatches     [][]workflow.TaskOutboxEvent
	err               error
	retryErr          error
	recoveryErr       error
	claimErr          error
	workflowRunIDs    []string
	retryTimes        []time.Time
	claims            int
	published         []workflow.MarkTaskOutboxEventPublishedInput
	failures          []workflow.RecordTaskOutboxEventFailureInput
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

func (r *fakeRepository) QueueDueRetryTaskRuns(_ context.Context, now time.Time) ([]workflow.TaskRun, error) {
	r.retryTimes = append(r.retryTimes, now)
	if r.retryErr != nil {
		return nil, r.retryErr
	}
	if len(r.retryBatches) > 0 {
		taskRuns := r.retryBatches[0]
		r.retryBatches = r.retryBatches[1:]
		return taskRuns, nil
	}
	return r.retryTaskRuns, nil
}

func (r *fakeRepository) RecoverExpiredRunningTaskRuns(context.Context) ([]workflow.TaskRun, error) {
	if r.recoveryErr != nil {
		return nil, r.recoveryErr
	}
	return r.recoveredTaskRuns, nil
}

func (r *fakeRepository) ClaimPendingTaskOutboxEvents(context.Context) ([]workflow.TaskOutboxEvent, error) {
	r.claims++
	if r.claimErr != nil {
		return nil, r.claimErr
	}
	if len(r.outboxBatches) > 0 {
		events := r.outboxBatches[0]
		r.outboxBatches = r.outboxBatches[1:]
		return events, nil
	}
	return r.outboxEvents, nil
}

func (r *fakeRepository) MarkTaskOutboxEventPublished(_ context.Context, input workflow.MarkTaskOutboxEventPublishedInput) error {
	r.published = append(r.published, input)
	return nil
}

func (r *fakeRepository) RecordTaskOutboxEventFailure(_ context.Context, input workflow.RecordTaskOutboxEventFailureInput) error {
	r.failures = append(r.failures, input)
	return nil
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
