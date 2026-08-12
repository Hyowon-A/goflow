package scheduler

import (
	"context"
	"log/slog"

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
	metrics   Metrics
}

func NewOutboxDispatcher(repo outboxRepository, publisher queue.TaskPublisher) *OutboxDispatcher {
	return NewOutboxDispatcherWithMetrics(repo, publisher, nil)
}

func NewOutboxDispatcherWithMetrics(repo outboxRepository, publisher queue.TaskPublisher, metrics Metrics) *OutboxDispatcher {
	return &OutboxDispatcher{repo: repo, publisher: publisher, metrics: metrics}
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
			if d.metrics != nil {
				d.metrics.Inc("goflow_outbox_publish_failures_total")
			}
			slog.WarnContext(ctx, "task_outbox_publish_failed",
				slog.String("outbox_event_id", event.ID),
				slog.String("workflow_id", event.WorkflowID),
				slog.String("workflow_run_id", event.WorkflowRunID),
				slog.String("task_id", event.TaskID),
				slog.String("task_run_id", event.TaskRunID),
				slog.String("error", err.Error()),
			)
			continue
		}

		if err := d.repo.MarkTaskOutboxEventPublished(ctx, workflow.MarkTaskOutboxEventPublishedInput{
			EventID:        event.ID,
			RedisMessageID: redisID,
		}); err != nil {
			return err
		}
		if d.metrics != nil {
			d.metrics.Inc("goflow_outbox_published_total")
		}
	}

	return nil
}
