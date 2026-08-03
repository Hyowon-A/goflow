package scheduler

import (
	"context"
	"log/slog"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type repository interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) ([]workflow.TaskRun, error)
	ClaimPendingTaskOutboxEvents(ctx context.Context) ([]workflow.TaskOutboxEvent, error)
	MarkTaskOutboxEventPublished(ctx context.Context, input workflow.MarkTaskOutboxEventPublishedInput) error
	RecordTaskOutboxEventFailure(ctx context.Context, input workflow.RecordTaskOutboxEventFailureInput) error
}

type Service struct {
	repo             repository
	outboxDispatcher *OutboxDispatcher
}

func NewService(repo repository, publisher queue.TaskPublisher) *Service {
	return &Service{repo: repo, outboxDispatcher: NewOutboxDispatcher(repo, publisher)}
}

func (s *Service) QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) error {
	taskRuns, err := s.repo.QueueRunnableTaskRuns(ctx, workflowRunID)
	if err != nil {
		return err
	}
	if len(taskRuns) == 0 {
		slog.InfoContext(ctx, "scheduler_noop",
			slog.String("workflow_run_id", workflowRunID),
			slog.String("reason", "no_runnable_task_runs"),
		)
		return nil
	}

	if err := s.outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx); err != nil {
		slog.WarnContext(ctx, "task_outbox_dispatch_failed",
			slog.String("workflow_run_id", workflowRunID),
			slog.String("error", err.Error()),
		)
	}

	return nil
}
