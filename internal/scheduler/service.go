package scheduler

import (
	"context"
	"log/slog"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type repository interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) ([]workflow.TaskRun, error)
}

type Service struct {
	repo      repository
	publisher queue.TaskPublisher
}

func NewService(repo repository, publisher queue.TaskPublisher) *Service {
	return &Service{repo: repo, publisher: publisher}
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

	for _, taskRun := range taskRuns {
		if _, err := s.publisher.PublishTask(ctx, queue.TaskMessage{
			WorkflowID:    taskRun.WorkflowID,
			WorkflowRunID: taskRun.WorkflowRunID,
			TaskID:        taskRun.TaskID,
			TaskRunID:     taskRun.ID,
		}); err != nil {
			return err
		}
	}

	return nil
}
