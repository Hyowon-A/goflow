package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type ServiceConfig struct {
	WorkerID string
}

type TaskRunClaimer interface {
	ClaimTaskRun(ctx context.Context, input workflow.ClaimTaskRunInput) (workflow.TaskRun, error)
}

type Repository interface {
	LoadTaskRunStatus(ctx context.Context, input workflow.LoadTaskRunStatusInput) (workflow.TaskRunStatus, error)
	LoadTaskRunExecution(ctx context.Context, input workflow.LoadTaskRunExecutionInput) (workflow.TaskRunExecution, error)
	CreateTaskAttempt(ctx context.Context, taskRunID string) (workflow.TaskAttempt, error)
	CompleteTaskAttempt(ctx context.Context, input workflow.CompleteTaskAttemptInput) (workflow.CompleteTaskAttemptResult, error)
}

type Scheduler interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) error
}

type Service struct {
	config    ServiceConfig
	consumer  queue.TaskConsumer
	claimer   TaskRunClaimer
	repo      Repository
	executors ExecutorRegistry
	scheduler Scheduler
}

func NewService(
	config ServiceConfig,
	consumer queue.TaskConsumer,
	claimer TaskRunClaimer,
	repo Repository,
	executors ExecutorRegistry,
	scheduler ...Scheduler,
) *Service {
	var schedulerInstance Scheduler
	if len(scheduler) > 0 {
		schedulerInstance = scheduler[0]
	}

	return &Service{
		config:    config,
		consumer:  consumer,
		claimer:   claimer,
		repo:      repo,
		executors: executors,
		scheduler: schedulerInstance,
	}
}

func (s *Service) ProcessOne(ctx context.Context) error {
	if s == nil || s.consumer == nil || s.claimer == nil || s.repo == nil || s.executors == nil || strings.TrimSpace(s.config.WorkerID) == "" {
		return fmt.Errorf("invalid worker service config")
	}

	message, err := s.consumer.ReceiveTask(ctx)
	if err != nil {
		return err
	}

	_, err = s.claimer.ClaimTaskRun(ctx, workflow.ClaimTaskRunInput{
		TaskRunID: message.TaskRunID,
		WorkerID:  s.config.WorkerID,
	})
	if err != nil {
		if errors.Is(err, workflow.ErrTaskRunNotClaimable) {
			return s.handleUnclaimableTaskRun(ctx, message)
		}
		return err
	}

	taskRunExecution, err := s.repo.LoadTaskRunExecution(ctx, workflow.LoadTaskRunExecutionInput{
		TaskRunID:     message.TaskRunID,
		WorkflowID:    message.WorkflowID,
		WorkflowRunID: message.WorkflowRunID,
		TaskID:        message.TaskID,
	})
	if err != nil {
		return err
	}

	taskAttempt, err := s.repo.CreateTaskAttempt(ctx, message.TaskRunID)
	if err != nil {
		return err
	}

	executionInput := ExecutionInput{
		WorkflowID:    taskRunExecution.WorkflowID,
		WorkflowRunID: taskRunExecution.WorkflowRunID,
		TaskID:        taskRunExecution.TaskID,
		TaskRunID:     taskRunExecution.TaskRunID,
		ExecutorType:  taskRunExecution.ExecutorType,
		Config:        taskRunExecution.Config,
		TaskRunInput:  taskRunExecution.TaskRunInput,
	}

	executor, err := s.executors.Resolve(taskRunExecution.ExecutorType)
	if err != nil || executor == nil {
		reason := ErrUnknownExecutorType.Error()
		if err != nil {
			reason = err.Error()
		}
		if completeErr := s.completeFailedAttempt(ctx, taskAttempt, message.TaskRunID, reason); completeErr != nil {
			return completeErr
		}
		return s.consumer.AckTask(ctx, message.MessageID)
	}

	result, executeErr := executor.Execute(ctx, executionInput)
	completeInput, err := completeAttemptInput(taskAttempt.ID, message.TaskRunID, result, executeErr)
	if err != nil {
		if completeErr := s.completeFailedAttempt(ctx, taskAttempt, message.TaskRunID, err.Error()); completeErr != nil {
			return completeErr
		}
		return s.consumer.AckTask(ctx, message.MessageID)
	}

	if _, err := s.repo.CompleteTaskAttempt(ctx, completeInput); err != nil {
		return err
	}

	if s.scheduler != nil {
		if err = s.scheduler.QueueRunnableTaskRuns(ctx, message.WorkflowRunID); err != nil {
			return err
		}
	}

	if err := s.consumer.AckTask(ctx, message.MessageID); err != nil {
		return err
	}

	return nil
}

func (s *Service) handleUnclaimableTaskRun(ctx context.Context, message queue.ReceivedTaskMessage) error {
	status, err := s.repo.LoadTaskRunStatus(ctx, workflow.LoadTaskRunStatusInput{TaskRunID: message.TaskRunID})
	if err != nil {
		return err
	}

	switch status {
	case workflow.TaskRunStatusRunning, workflow.TaskRunStatusCompleted, workflow.TaskRunStatusFailed, workflow.TaskRunStatusDeadLetter:
		slog.InfoContext(ctx, "duplicate_task_message",
			slog.String("workflow_id", message.WorkflowID),
			slog.String("workflow_run_id", message.WorkflowRunID),
			slog.String("task_id", message.TaskID),
			slog.String("task_run_id", message.TaskRunID),
			slog.String("redis_message_id", message.MessageID),
			slog.String("worker_id", s.config.WorkerID),
			slog.String("status", string(status)),
			slog.String("reason", "not_claimable"),
		)
		return s.consumer.AckTask(ctx, message.MessageID)
	default:
		return workflow.ErrTaskRunNotClaimable
	}
}

func (s *Service) completeFailedAttempt(ctx context.Context, attempt workflow.TaskAttempt, taskRunID, reason string) error {
	_, err := s.repo.CompleteTaskAttempt(ctx, workflow.CompleteTaskAttemptInput{
		TaskAttemptID: attempt.ID,
		TaskRunID:     taskRunID,
		Success:       false,
		FailureReason: reason,
	})
	return err
}

func completeAttemptInput(
	taskAttemptID string,
	taskRunID string,
	result ExecutionResult,
	executeErr error,
) (workflow.CompleteTaskAttemptInput, error) {
	if executeErr != nil {
		return workflow.CompleteTaskAttemptInput{
			TaskAttemptID: taskAttemptID,
			TaskRunID:     taskRunID,
			Success:       false,
			FailureReason: executeErr.Error(),
		}, nil
	}

	if strings.TrimSpace(result.FailureReason) != "" {
		return workflow.CompleteTaskAttemptInput{
			TaskAttemptID: taskAttemptID,
			TaskRunID:     taskRunID,
			Success:       false,
			FailureReason: result.FailureReason,
		}, nil
	}

	return workflow.CompleteTaskAttemptInput{
		TaskAttemptID: taskAttemptID,
		TaskRunID:     taskRunID,
		Success:       true,
		Output:        result.Output,
	}, nil
}
