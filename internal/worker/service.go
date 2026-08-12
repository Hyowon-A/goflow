package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type ServiceConfig struct {
	WorkerID          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type TaskRunClaimer interface {
	ClaimTaskRun(ctx context.Context, input workflow.ClaimTaskRunInput) (workflow.TaskRun, error)
}

type Repository interface {
	LoadTaskRunStatus(ctx context.Context, input workflow.LoadTaskRunStatusInput) (workflow.TaskRunStatus, error)
	LoadTaskRunExecution(ctx context.Context, input workflow.LoadTaskRunExecutionInput) (workflow.TaskRunExecution, error)
	CreateTaskAttempt(ctx context.Context, taskRunID string) (workflow.TaskAttempt, error)
	CompleteTaskAttempt(ctx context.Context, input workflow.CompleteTaskAttemptInput) (workflow.CompleteTaskAttemptResult, error)
	FinalizeWorkflowRun(ctx context.Context, workflowRunID string) (workflow.WorkflowRun, bool, error)
	ExtendTaskRunLease(ctx context.Context, input workflow.ExtendTaskRunLeaseInput) (workflow.TaskRun, error)
}

type Scheduler interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) error
}

type Metrics interface {
	Inc(string)
}

type Service struct {
	config    ServiceConfig
	consumer  queue.TaskConsumer
	claimer   TaskRunClaimer
	repo      Repository
	executors ExecutorRegistry
	scheduler Scheduler
	metrics   Metrics
}

func NewService(
	config ServiceConfig,
	consumer queue.TaskConsumer,
	claimer TaskRunClaimer,
	repo Repository,
	executors ExecutorRegistry,
	scheduler ...Scheduler,
) *Service {
	return NewServiceWithMetrics(config, consumer, claimer, repo, executors, nil, scheduler...)
}

func NewServiceWithMetrics(
	config ServiceConfig,
	consumer queue.TaskConsumer,
	claimer TaskRunClaimer,
	repo Repository,
	executors ExecutorRegistry,
	metrics Metrics,
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
		metrics:   metrics,
	}
}

func (s *Service) ProcessOne(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("invalid worker service config")
	}
	heartbeatInterval := s.heartbeatInterval()
	if s.consumer == nil || s.claimer == nil || s.repo == nil || s.executors == nil || strings.TrimSpace(s.config.WorkerID) == "" || s.config.LeaseDuration <= 0 || heartbeatInterval <= 0 || heartbeatInterval >= s.config.LeaseDuration {
		return fmt.Errorf("invalid worker service config")
	}

	message, err := s.consumer.ReceiveTask(ctx)
	if err != nil {
		return err
	}

	_, err = s.claimer.ClaimTaskRun(ctx, workflow.ClaimTaskRunInput{
		TaskRunID:     message.TaskRunID,
		WorkerID:      s.config.WorkerID,
		LeaseDuration: s.config.LeaseDuration,
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

	policy, err := workflow.ParseRetryPolicy(taskRunExecution.Config)
	if err != nil {
		if _, completeErr := s.completeFailedAttempt(ctx, taskAttempt, message.TaskRunID, err.Error()); completeErr != nil {
			return completeErr
		}
		if err := s.finalizeWorkflowRun(ctx, message.WorkflowRunID); err != nil {
			return err
		}
		return s.ackTask(ctx, message.MessageID)
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
		if _, completeErr := s.completeFailedAttempt(ctx, taskAttempt, message.TaskRunID, reason); completeErr != nil {
			return completeErr
		}
		if err := s.finalizeWorkflowRun(ctx, message.WorkflowRunID); err != nil {
			return err
		}
		return s.ackTask(ctx, message.MessageID)
	}

	result, executeErr, heartbeatErr := s.executeWithHeartbeat(ctx, executor, executionInput, message.TaskRunID, heartbeatInterval)
	if heartbeatErr != nil {
		return s.leavePending(heartbeatErr)
	}

	completeInput, err := completeAttemptInput(taskAttempt.ID, message.TaskRunID, s.config.WorkerID, result, executeErr)
	if err != nil {
		if _, completeErr := s.completeFailedAttempt(ctx, taskAttempt, message.TaskRunID, err.Error()); completeErr != nil {
			return completeErr
		}
		if err := s.finalizeWorkflowRun(ctx, message.WorkflowRunID); err != nil {
			return err
		}
		return s.ackTask(ctx, message.MessageID)
	}
	decision := workflow.DecideRetry(time.Now(), taskAttempt.AttemptNumber, policy, result.Retryable, completeInput.FailureReason)
	completeInput.Retry = decision.Retry
	completeInput.NextRetryAt = decision.NextRetryAt

	completeResult, err := s.completeTaskAttempt(ctx, completeInput)
	if err != nil {
		return err
	}

	if err := s.finalizeWorkflowRun(ctx, message.WorkflowRunID); err != nil {
		return err
	}

	if s.scheduler != nil && completeResult.TaskRun.Status == workflow.TaskRunStatusCompleted {
		if err = s.scheduler.QueueRunnableTaskRuns(ctx, message.WorkflowRunID); err != nil {
			return err
		}
	}

	if err := s.ackTask(ctx, message.MessageID); err != nil {
		return err
	}

	return nil
}

func (s *Service) heartbeatInterval() time.Duration {
	if s == nil {
		return 0
	}
	if s.config.HeartbeatInterval > 0 {
		return s.config.HeartbeatInterval
	}
	return s.config.LeaseDuration / 2
}

func (s *Service) executeWithHeartbeat(
	ctx context.Context,
	executor Executor,
	input ExecutionInput,
	taskRunID string,
	interval time.Duration,
) (ExecutionResult, error, error) {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	heartbeatDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_, err := s.repo.ExtendTaskRunLease(ctx, workflow.ExtendTaskRunLeaseInput{
					TaskRunID:     taskRunID,
					WorkerID:      s.config.WorkerID,
					LeaseDuration: s.config.LeaseDuration,
				})
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						s.incMetric("goflow_worker_lease_heartbeat_failures_total")
					}
					cancel()
					heartbeatDone <- err
					return
				}
				s.incMetric("goflow_worker_lease_heartbeats_total")
			case <-done:
				heartbeatDone <- nil
				return
			case <-ctx.Done():
				heartbeatDone <- nil
				return
			}
		}
	}()

	result, executeErr := executor.Execute(heartbeatCtx, input)
	close(done)
	if err := <-heartbeatDone; err != nil {
		return result, executeErr, err
	}

	return result, executeErr, nil
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
		return s.ackTask(ctx, message.MessageID)
	default:
		return workflow.ErrTaskRunNotClaimable
	}
}

func (s *Service) ackTask(ctx context.Context, messageID string) error {
	if err := s.consumer.AckTask(ctx, messageID); err != nil {
		return err
	}
	s.incMetric("goflow_worker_messages_acknowledged_total")
	return nil
}

func (s *Service) leavePending(err error) error {
	if errors.Is(err, workflow.ErrTaskAttemptNotCompletable) || errors.Is(err, workflow.ErrTaskRunLeaseNotExtensible) {
		s.incMetric("goflow_worker_messages_left_pending_total")
	}
	return err
}

func (s *Service) incMetric(name string) {
	if s.metrics != nil {
		s.metrics.Inc(name)
	}
}

func (s *Service) completeFailedAttempt(ctx context.Context, attempt workflow.TaskAttempt, taskRunID, reason string) (workflow.CompleteTaskAttemptResult, error) {
	return s.completeTaskAttempt(ctx, workflow.CompleteTaskAttemptInput{
		TaskAttemptID: attempt.ID,
		TaskRunID:     taskRunID,
		WorkerID:      s.config.WorkerID,
		Success:       false,
		FailureReason: reason,
	})
}

func (s *Service) completeTaskAttempt(ctx context.Context, input workflow.CompleteTaskAttemptInput) (workflow.CompleteTaskAttemptResult, error) {
	result, err := s.repo.CompleteTaskAttempt(ctx, input)
	if err != nil {
		if errors.Is(err, workflow.ErrTaskAttemptNotCompletable) {
			s.incMetric("goflow_worker_late_completions_rejected_total")
			s.incMetric("goflow_worker_messages_left_pending_total")
		}
		return workflow.CompleteTaskAttemptResult{}, err
	}
	if s.metrics != nil {
		switch result.TaskAttempt.Status {
		case workflow.TaskAttemptStatusCompleted:
			s.metrics.Inc("goflow_task_attempts_completed_total")
		case workflow.TaskAttemptStatusFailed:
			s.metrics.Inc("goflow_task_attempts_failed_total")
		}
		switch result.TaskRun.Status {
		case workflow.TaskRunStatusCompleted:
			s.metrics.Inc("goflow_task_runs_completed_total")
		case workflow.TaskRunStatusDeadLetter:
			s.metrics.Inc("goflow_task_runs_dead_lettered_total")
		}
	}
	return result, nil
}

func (s *Service) finalizeWorkflowRun(ctx context.Context, workflowRunID string) error {
	workflowRun, changed, err := s.repo.FinalizeWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return err
	}
	if changed {
		if s.metrics != nil {
			switch workflow.WorkflowRunStatus(workflowRun.Status) {
			case workflow.WorkflowRunStatusCompleted:
				s.metrics.Inc("goflow_workflow_runs_completed_total")
			case workflow.WorkflowRunStatusFailed:
				s.metrics.Inc("goflow_workflow_runs_failed_total")
			}
		}
		slog.InfoContext(ctx, "workflow_run_finalized",
			slog.String("workflow_run_id", workflowRun.ID),
			slog.String("workflow_id", workflowRun.WorkflowID),
			slog.String("status", workflowRun.Status),
		)
	}
	return nil
}

func completeAttemptInput(
	taskAttemptID string,
	taskRunID string,
	workerID string,
	result ExecutionResult,
	executeErr error,
) (workflow.CompleteTaskAttemptInput, error) {
	if executeErr != nil {
		return workflow.CompleteTaskAttemptInput{
			TaskAttemptID: taskAttemptID,
			TaskRunID:     taskRunID,
			WorkerID:      workerID,
			Success:       false,
			FailureReason: executeErr.Error(),
		}, nil
	}

	if strings.TrimSpace(result.FailureReason) != "" {
		return workflow.CompleteTaskAttemptInput{
			TaskAttemptID: taskAttemptID,
			TaskRunID:     taskRunID,
			WorkerID:      workerID,
			Success:       false,
			FailureReason: result.FailureReason,
		}, nil
	}

	return workflow.CompleteTaskAttemptInput{
		TaskAttemptID: taskAttemptID,
		TaskRunID:     taskRunID,
		WorkerID:      workerID,
		Success:       true,
		Output:        result.Output,
	}, nil
}
