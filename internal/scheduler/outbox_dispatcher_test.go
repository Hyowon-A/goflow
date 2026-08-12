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

func TestOutboxDispatcherPublishesClaimedEventAndMarksPublished(t *testing.T) {
	repo := &fakeOutboxRepository{
		events: []workflow.TaskOutboxEvent{{
			ID:            "event-1",
			WorkflowID:    "workflow",
			WorkflowRunID: "workflow-run",
			TaskID:        "task",
			TaskRunID:     "task-run",
		}},
	}
	publisher := &fakePublisher{}
	metrics := &fakeMetrics{}
	dispatcher := NewOutboxDispatcherWithMetrics(repo, publisher, metrics)

	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch pending task outbox events: %v", err)
	}

	wantMessage := []queue.TaskMessage{{
		WorkflowID:    "workflow",
		WorkflowRunID: "workflow-run",
		TaskID:        "task",
		TaskRunID:     "task-run",
	}}
	if !reflect.DeepEqual(publisher.messages, wantMessage) {
		t.Fatalf("unexpected published messages: got %#v, want %#v", publisher.messages, wantMessage)
	}
	wantPublished := []workflow.MarkTaskOutboxEventPublishedInput{{
		EventID:        "event-1",
		RedisMessageID: "message-id",
	}}
	if !reflect.DeepEqual(repo.published, wantPublished) {
		t.Fatalf("unexpected published marks: got %#v, want %#v", repo.published, wantPublished)
	}
	if len(repo.failures) != 0 {
		t.Fatalf("expected no failure records, got %#v", repo.failures)
	}
	if got := metrics.counts["goflow_outbox_published_total"]; got != 1 {
		t.Fatalf("expected one published metric, got %d", got)
	}
}

func TestOutboxDispatcherRecordsPublishFailure(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	publishErr := errors.New("redis unavailable")
	repo := &fakeOutboxRepository{
		events: []workflow.TaskOutboxEvent{{
			ID:            "event-1",
			WorkflowID:    "workflow",
			WorkflowRunID: "workflow-run",
			TaskID:        "task",
			TaskRunID:     "task-run",
		}},
	}
	publisher := &fakePublisher{err: publishErr}
	metrics := &fakeMetrics{}
	dispatcher := NewOutboxDispatcherWithMetrics(repo, publisher, metrics)

	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch pending task outbox events: %v", err)
	}

	wantFailures := []workflow.RecordTaskOutboxEventFailureInput{{
		EventID:   "event-1",
		LastError: publishErr.Error(),
	}}
	if !reflect.DeepEqual(repo.failures, wantFailures) {
		t.Fatalf("unexpected failure records: got %#v, want %#v", repo.failures, wantFailures)
	}
	if len(repo.published) != 0 {
		t.Fatalf("expected no published marks, got %#v", repo.published)
	}
	if got := metrics.counts["goflow_outbox_publish_failures_total"]; got != 1 {
		t.Fatalf("expected one publish failure metric, got %d", got)
	}
	for _, want := range []string{
		`"msg":"task_outbox_publish_failed"`,
		`"outbox_event_id":"event-1"`,
		`"workflow_id":"workflow"`,
		`"workflow_run_id":"workflow-run"`,
		`"task_id":"task"`,
		`"task_run_id":"task-run"`,
		`"error":"redis unavailable"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logs.String())
		}
	}
}

func TestOutboxDispatcherSecondRunDoesNotRepublishAlreadyPublishedEvent(t *testing.T) {
	repo := &fakeOutboxRepository{
		batches: [][]workflow.TaskOutboxEvent{
			{{
				ID:            "event-1",
				WorkflowID:    "workflow",
				WorkflowRunID: "workflow-run",
				TaskID:        "task",
				TaskRunID:     "task-run",
			}},
			nil,
		},
	}
	publisher := &fakePublisher{}
	dispatcher := NewOutboxDispatcher(repo, publisher)

	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch first outbox pass: %v", err)
	}
	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch second outbox pass: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf("expected one publish, got %#v", publisher.messages)
	}
	if len(repo.published) != 1 {
		t.Fatalf("expected one published mark, got %#v", repo.published)
	}
}

type fakeOutboxRepository struct {
	events    []workflow.TaskOutboxEvent
	batches   [][]workflow.TaskOutboxEvent
	err       error
	published []workflow.MarkTaskOutboxEventPublishedInput
	failures  []workflow.RecordTaskOutboxEventFailureInput
}

func (r *fakeOutboxRepository) ClaimPendingTaskOutboxEvents(context.Context) ([]workflow.TaskOutboxEvent, error) {
	if r.err != nil {
		return nil, r.err
	}
	if len(r.batches) > 0 {
		events := r.batches[0]
		r.batches = r.batches[1:]
		return events, nil
	}
	return r.events, nil
}

func (r *fakeOutboxRepository) MarkTaskOutboxEventPublished(_ context.Context, input workflow.MarkTaskOutboxEventPublishedInput) error {
	r.published = append(r.published, input)
	return nil
}

func (r *fakeOutboxRepository) RecordTaskOutboxEventFailure(_ context.Context, input workflow.RecordTaskOutboxEventFailureInput) error {
	r.failures = append(r.failures, input)
	return nil
}
