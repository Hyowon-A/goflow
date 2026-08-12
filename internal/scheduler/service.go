package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type repository interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) ([]workflow.TaskRun, error)
	QueueDueRetryTaskRuns(ctx context.Context, now time.Time) ([]workflow.TaskRun, error)
	ClaimPendingTaskOutboxEvents(ctx context.Context) ([]workflow.TaskOutboxEvent, error)
	MarkTaskOutboxEventPublished(ctx context.Context, input workflow.MarkTaskOutboxEventPublishedInput) error
	RecordTaskOutboxEventFailure(ctx context.Context, input workflow.RecordTaskOutboxEventFailureInput) error
	RecoverExpiredRunningTaskRuns(ctx context.Context) ([]workflow.TaskRun, error)
}

type Metrics interface {
	Inc(string)
	Add(string, uint64)
}

type Service struct {
	repo             repository
	outboxDispatcher *OutboxDispatcher
	metrics          Metrics
}

func NewService(repo repository, publisher queue.TaskPublisher) *Service {
	return NewServiceWithMetrics(repo, publisher, nil)
}

func NewServiceWithMetrics(repo repository, publisher queue.TaskPublisher, metrics Metrics) *Service {
	return &Service{
		repo:             repo,
		outboxDispatcher: NewOutboxDispatcherWithMetrics(repo, publisher, metrics),
		metrics:          metrics,
	}
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
	s.addMetric("goflow_task_runs_queued_total", len(taskRuns))

	if err := s.outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx); err != nil {
		slog.WarnContext(ctx, "task_outbox_dispatch_failed",
			slog.String("workflow_run_id", workflowRunID),
			slog.String("error", err.Error()),
		)
	}

	return nil
}

func (s *Service) QueueDueRetryTaskRuns(ctx context.Context, now time.Time) error {
	taskRuns, err := s.repo.QueueDueRetryTaskRuns(ctx, now)
	if err != nil {
		return err
	}
	if len(taskRuns) == 0 {
		slog.InfoContext(ctx, "scheduler_noop",
			slog.String("reason", "no_due_retry_task_runs"),
		)
		return nil
	}
	s.addMetric("goflow_task_runs_queued_total", len(taskRuns))

	for _, taskRun := range taskRuns {
		slog.InfoContext(ctx, "retry_task_run_queued",
			slog.String("workflow_id", taskRun.WorkflowID),
			slog.String("workflow_run_id", taskRun.WorkflowRunID),
			slog.String("task_id", taskRun.TaskID),
			slog.String("task_run_id", taskRun.ID),
			slog.String("next_retry_at", now.Format(time.RFC3339Nano)),
		)
	}

	if err := s.outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx); err != nil {
		slog.WarnContext(ctx, "task_outbox_dispatch_failed",
			slog.String("error", err.Error()),
		)
	}

	return nil
}

func (s *Service) RecoverExpiredRunningTaskRuns(ctx context.Context) error {
	taskRuns, err := s.repo.RecoverExpiredRunningTaskRuns(ctx)
	if err != nil {
		return err
	}
	if len(taskRuns) == 0 {
		return nil
	}
	s.addMetric("goflow_task_runs_queued_total", len(taskRuns))
	s.addMetric("goflow_worker_lease_recoveries_total", len(taskRuns))
	for _, taskRun := range taskRuns {
		slog.InfoContext(ctx, "expired_task_run_recovered",
			slog.String("workflow_id", taskRun.WorkflowID),
			slog.String("workflow_run_id", taskRun.WorkflowRunID),
			slog.String("task_id", taskRun.TaskID),
			slog.String("task_run_id", taskRun.ID),
			slog.String("previous_worker_id", taskRun.LockedBy),
		)
	}

	if err := s.outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx); err != nil {
		slog.WarnContext(ctx, "task_outbox_dispatch_failed",
			slog.String("error", err.Error()),
		)
	}
	return nil
}

func (s *Service) addMetric(name string, count int) {
	if s.metrics != nil && count > 0 {
		s.metrics.Add(name, uint64(count))
	}
}
