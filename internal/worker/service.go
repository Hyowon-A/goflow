package worker

import (
	"context"
	"fmt"
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
	LoadTaskRunExecution(ctx context.Context, input workflow.LoadTaskRunExecutionInput) (workflow.TaskRunExecution, error)
	CreateTaskAttempt(ctx context.Context, taskRunID string) (workflow.TaskAttempt, error)
	CompleteTaskAttempt(ctx context.Context, input workflow.CompleteTaskAttemptInput) (workflow.CompleteTaskAttemptResult, error)
}

type Service struct {
	config    ServiceConfig
	consumer  queue.TaskConsumer
	claimer   TaskRunClaimer
	repo      Repository
	executors ExecutorRegistry
}

func NewService(
	config ServiceConfig,
	consumer queue.TaskConsumer,
	claimer TaskRunClaimer,
	repo Repository,
	executors ExecutorRegistry,
) *Service {
	return &Service{
		config:    config,
		consumer:  consumer,
		claimer:   claimer,
		repo:      repo,
		executors: executors,
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

	if err := s.consumer.AckTask(ctx, message.MessageID); err != nil {
		return err
	}

	return nil
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
