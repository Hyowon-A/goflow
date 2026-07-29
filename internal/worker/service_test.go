package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestServiceProcessOneReceivesClaimsAndAcknowledgesTask(t *testing.T) {
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    "workflow-id",
				WorkflowRunID: "workflow-run-id",
				TaskID:        "task-id",
				TaskRunID:     "task-run-id",
			},
		},
	}
	claimer := &fakeTaskRunClaimer{
		claimed: workflow.TaskRun{
			ID:     "task-run-id",
			Status: workflow.TaskRunStatusRunning,
		},
	}
	service := NewService(ServiceConfig{WorkerID: "worker-1"}, consumer, claimer)

	err := service.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one task: %v", err)
	}

	wantClaims := []workflow.ClaimTaskRunInput{
		{TaskRunID: "task-run-id", WorkerID: "worker-1"},
	}
	if !reflect.DeepEqual(claimer.claims, wantClaims) {
		t.Fatalf("unexpected claim inputs: got %#v, want %#v", claimer.claims, wantClaims)
	}

	if !reflect.DeepEqual(consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acked, got %#v", consumer.acks)
	}
}

func TestServiceProcessOneDoesNotAckWhenClaimFails(t *testing.T) {
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    "workflow-id",
				WorkflowRunID: "workflow-run-id",
				TaskID:        "task-id",
				TaskRunID:     "task-run-id",
			},
		},
	}
	claimer := &fakeTaskRunClaimer{
		claimErr: workflow.ErrTaskRunNotClaimable,
	}
	service := NewService(ServiceConfig{WorkerID: "worker-1"}, consumer, claimer)

	err := service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskRunNotClaimable) {
		t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
	}

	if len(consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements after claim failure, got %#v", consumer.acks)
	}
}

func TestServiceProcessOneDoesNotClaimWhenQueueReadTimesOut(t *testing.T) {
	consumer := &fakeConsumer{
		receiveErr: queue.ErrNoMessage,
	}
	claimer := &fakeTaskRunClaimer{}
	service := NewService(ServiceConfig{WorkerID: "worker-1"}, consumer, claimer)

	err := service.ProcessOne(context.Background())
	if !errors.Is(err, queue.ErrNoMessage) {
		t.Fatalf("expected ErrNoMessage, got %v", err)
	}

	if len(claimer.claims) != 0 {
		t.Fatalf("expected no claim attempts when no message is received, got %#v", claimer.claims)
	}
	if len(consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements when no message is received, got %#v", consumer.acks)
	}
}

func TestServiceProcessOneReturnsAckErrorAfterSuccessfulClaim(t *testing.T) {
	ackErr := errors.New("ack failed")
	consumer := &fakeConsumer{
		received: queue.ReceivedTaskMessage{
			MessageID: "redis-message-id",
			TaskMessage: queue.TaskMessage{
				WorkflowID:    "workflow-id",
				WorkflowRunID: "workflow-run-id",
				TaskID:        "task-id",
				TaskRunID:     "task-run-id",
			},
		},
		ackErr: ackErr,
	}
	claimer := &fakeTaskRunClaimer{
		claimed: workflow.TaskRun{
			ID:     "task-run-id",
			Status: workflow.TaskRunStatusRunning,
		},
	}
	service := NewService(ServiceConfig{WorkerID: "worker-1"}, consumer, claimer)

	err := service.ProcessOne(context.Background())
	if !errors.Is(err, ackErr) {
		t.Fatalf("expected ack error, got %v", err)
	}

	if len(claimer.claims) != 1 {
		t.Fatalf("expected one successful claim before ack failure, got %#v", claimer.claims)
	}
}

type fakeConsumer struct {
	received   queue.ReceivedTaskMessage
	receiveErr error
	acks       []string
	ackErr     error
}

func (c *fakeConsumer) ReceiveTask(context.Context) (queue.ReceivedTaskMessage, error) {
	if c.receiveErr != nil {
		return queue.ReceivedTaskMessage{}, c.receiveErr
	}
	return c.received, nil
}

func (c *fakeConsumer) AckTask(_ context.Context, messageID string) error {
	c.acks = append(c.acks, messageID)
	return c.ackErr
}

func (c *fakeConsumer) Close() error {
	return nil
}

type fakeTaskRunClaimer struct {
	claimed  workflow.TaskRun
	claimErr error
	claims   []workflow.ClaimTaskRunInput
}

func (c *fakeTaskRunClaimer) ClaimTaskRun(_ context.Context, input workflow.ClaimTaskRunInput) (workflow.TaskRun, error) {
	c.claims = append(c.claims, input)
	if c.claimErr != nil {
		return workflow.TaskRun{}, c.claimErr
	}
	return c.claimed, nil
}
