package scheduler

import (
	"context"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type outboxRepository interface {
	ClaimPendingTaskOutboxEvents(ctx context.Context) ([]workflow.TaskOutboxEvent, error)
	MarkTaskOutboxEventPublished(ctx context.Context, input workflow.MarkTaskOutboxEventPublishedInput) error
	RecordTaskOutboxEventFailure(ctx context.Context, input workflow.RecordTaskOutboxEventFailureInput) error
}

type OutboxDispatcher struct {
	repo      outboxRepository
	publisher queue.TaskPublisher
}

func NewOutboxDispatcher(repo outboxRepository, publisher queue.TaskPublisher) *OutboxDispatcher {
	return &OutboxDispatcher{repo: repo, publisher: publisher}
}

func (d *OutboxDispatcher) DispatchPendingTaskOutboxEvents(ctx context.Context) error {
	events, err := d.repo.ClaimPendingTaskOutboxEvents(ctx)
	if err != nil {
		return err
	}

	for _, event := range events {
		redisID, err := d.publisher.PublishTask(ctx, queue.TaskMessage{
			WorkflowID:    event.WorkflowID,
			WorkflowRunID: event.WorkflowRunID,
			TaskID:        event.TaskID,
			TaskRunID:     event.TaskRunID,
		})
		if err != nil {
			if recordErr := d.repo.RecordTaskOutboxEventFailure(ctx, workflow.RecordTaskOutboxEventFailureInput{
				EventID:   event.ID,
				LastError: err.Error(),
			}); recordErr != nil {
				return recordErr
			}
			continue
		}

		if err := d.repo.MarkTaskOutboxEventPublished(ctx, workflow.MarkTaskOutboxEventPublishedInput{
			EventID:        event.ID,
			RedisMessageID: redisID,
		}); err != nil {
			return err
		}
	}

	return nil
}
