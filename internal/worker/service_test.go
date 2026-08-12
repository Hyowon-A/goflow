package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestServiceProcessOneRunsExecutorCompletesAttemptAndAcknowledgesTask(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed", "message": "ok"}}

	err := fixture.service.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(fixture.claimer.claims, []workflow.ClaimTaskRunInput{{TaskRunID: "task-run-id", WorkerID: "worker-1", LeaseDuration: 30 * time.Second}}) {
		t.Fatalf("unexpected claim inputs: got %#v", fixture.claimer.claims)
	}
	if !reflect.DeepEqual(fixture.repo.loads, []workflow.LoadTaskRunExecutionInput{{
		TaskRunID:     "task-run-id",
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
	}}) {
		t.Fatalf("unexpected load inputs: got %#v", fixture.repo.loads)
	}
	if !reflect.DeepEqual(fixture.executor.inputs, []ExecutionInput{{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
		ExecutorType:  ExecutorTypeLog,
		Config:        map[string]any{"message": "from config"},
		TaskRunInput:  map[string]any{"message": "from input"},
	}}) {
		t.Fatalf("unexpected executor inputs: got %#v", fixture.executor.inputs)
	}
	if !reflect.DeepEqual(fixture.repo.completions, []workflow.CompleteTaskAttemptInput{{
		TaskAttemptID: "attempt-id",
		TaskRunID:     "task-run-id",
		WorkerID:      "worker-1",
		Success:       true,
		Output:        map[string]any{"status": "completed", "message": "ok"},
	}}) {
		t.Fatalf("unexpected completions: got %#v", fixture.repo.completions)
	}
	if !reflect.DeepEqual(fixture.repo.finalized, []string{"workflow-run-id"}) {
		t.Fatalf("unexpected finalized workflow run IDs: %#v", fixture.repo.finalized)
	}
	if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acked, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneLogsWorkflowRunFinalization(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	fixture.repo.finalizeChanged = true
	fixture.repo.finalizeWorkflowRun = workflow.WorkflowRun{
		ID:         "workflow-run-id",
		WorkflowID: "workflow-id",
		Status:     string(workflow.WorkflowRunStatusCompleted),
	}

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"workflow_run_finalized"`,
		`"workflow_id":"workflow-id"`,
		`"workflow_run_id":"workflow-run-id"`,
		`"status":"completed"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
		}
	}
}

func TestServiceFinalizeWorkflowRunIncrementsTerminalMetrics(t *testing.T) {
	tests := []struct {
		name       string
		status     workflow.WorkflowRunStatus
		metricName string
	}{
		{
			name:       "completed",
			status:     workflow.WorkflowRunStatusCompleted,
			metricName: "goflow_workflow_runs_completed_total",
		},
		{
			name:       "failed",
			status:     workflow.WorkflowRunStatusFailed,
			metricName: "goflow_workflow_runs_failed_total",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newServiceTestFixture()
			metrics := &fakeMetrics{}
			fixture.service.metrics = metrics
			fixture.repo.finalizeChanged = true
			fixture.repo.finalizeWorkflowRun = workflow.WorkflowRun{
				ID:         "workflow-run-id",
				WorkflowID: "workflow-id",
				Status:     string(tt.status),
			}

			if err := fixture.service.finalizeWorkflowRun(context.Background(), "workflow-run-id"); err != nil {
				t.Fatalf("finalize workflow run: %v", err)
			}

			if got := metrics.counts[tt.metricName]; got != 1 {
				t.Fatalf("expected %s to increment once, got %d", tt.metricName, got)
			}
		})
	}
}

func TestServiceFinalizeWorkflowRunDoesNotIncrementMetricsWhenUnchanged(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.repo.finalizeChanged = false
	fixture.repo.finalizeWorkflowRun = workflow.WorkflowRun{
		ID:         "workflow-run-id",
		WorkflowID: "workflow-id",
		Status:     string(workflow.WorkflowRunStatusCompleted),
	}

	if err := fixture.service.finalizeWorkflowRun(context.Background(), "workflow-run-id"); err != nil {
		t.Fatalf("finalize workflow run: %v", err)
	}

	if len(metrics.counts) != 0 {
		t.Fatalf("expected no metrics for unchanged finalization, got %#v", metrics.counts)
	}
}

func TestServiceProcessOneIncrementsTaskCompletionMetrics(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	for name, want := range map[string]int{
		"goflow_task_attempts_completed_total": 1,
		"goflow_task_runs_completed_total":     1,
	} {
		if got := metrics.counts[name]; got != want {
			t.Fatalf("expected %s=%d, got %d", name, want, got)
		}
	}
}

func TestServiceProcessOneIncrementsTaskFailureMetrics(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.executor.result = ExecutionResult{FailureReason: "failed"}

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	for name, want := range map[string]int{
		"goflow_task_attempts_failed_total":    1,
		"goflow_task_runs_dead_lettered_total": 1,
	} {
		if got := metrics.counts[name]; got != want {
			t.Fatalf("expected %s=%d, got %d", name, want, got)
		}
	}
}

func TestServiceProcessOneIncrementsAcknowledgedMetric(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if got := metrics.counts["goflow_worker_messages_acknowledged_total"]; got != 1 {
		t.Fatalf("expected acknowledged metric once, got %d", got)
	}
}

func TestServiceProcessOneDoesNotAckWhenQueueReadTimesOut(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.consumer.receiveErr = queue.ErrNoMessage

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, queue.ErrNoMessage) {
		t.Fatalf("expected ErrNoMessage, got %v", err)
	}
	if len(fixture.claimer.claims) != 0 {
		t.Fatalf("expected no claim attempts when no message is received, got %#v", fixture.claimer.claims)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements when no message is received, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneDoesNotAckWhenClaimFailsForNonDuplicateTask(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.claimer.claimErr = workflow.ErrTaskRunNotClaimable
	fixture.repo.taskRunStatus = workflow.TaskRunStatusPending

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskRunNotClaimable) {
		t.Fatalf("expected ErrTaskRunNotClaimable, got %v", err)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements after claim failure, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneAcknowledgesDuplicateMessagesForKnownNonQueuedStates(t *testing.T) {
	statuses := []workflow.TaskRunStatus{
		workflow.TaskRunStatusRunning,
		workflow.TaskRunStatusCompleted,
		workflow.TaskRunStatusFailed,
		workflow.TaskRunStatusDeadLetter,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })

			fixture := newServiceTestFixture()
			fixture.claimer.claimErr = workflow.ErrTaskRunNotClaimable
			fixture.repo.taskRunStatus = status

			err := fixture.service.ProcessOne(context.Background())
			if err != nil {
				t.Fatalf("process duplicate message: %v", err)
			}
			if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
				t.Fatalf("expected duplicate message to be acked, got %#v", fixture.consumer.acks)
			}
			if len(fixture.repo.loads) != 0 || len(fixture.repo.createdAttempts) != 0 || len(fixture.repo.completions) != 0 {
				t.Fatalf("expected duplicate message to skip execution, got loads=%#v attempts=%#v completions=%#v", fixture.repo.loads, fixture.repo.createdAttempts, fixture.repo.completions)
			}
			logOutput := logs.String()
			for _, want := range []string{
				`"msg":"duplicate_task_message"`,
				`"task_run_id":"task-run-id"`,
				`"redis_message_id":"redis-message-id"`,
				`"reason":"not_claimable"`,
			} {
				if !strings.Contains(logOutput, want) {
					t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
				}
			}
		})
	}
}

func TestServiceProcessOneIncrementsAcknowledgedMetricForDuplicateMessage(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.claimer.claimErr = workflow.ErrTaskRunNotClaimable
	fixture.repo.taskRunStatus = workflow.TaskRunStatusCompleted

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process duplicate message: %v", err)
	}

	if got := metrics.counts["goflow_worker_messages_acknowledged_total"]; got != 1 {
		t.Fatalf("expected acknowledged metric once, got %d", got)
	}
}

func TestServiceLogsDoNotIncludeExecutorPayloads(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	duplicate := newServiceTestFixture()
	duplicate.claimer.claimErr = workflow.ErrTaskRunNotClaimable
	duplicate.repo.taskRunStatus = workflow.TaskRunStatusCompleted
	duplicate.repo.execution.Config = map[string]any{"token": "secret-config"}
	duplicate.repo.execution.TaskRunInput = map[string]any{"document": "secret-input"}
	if err := duplicate.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process duplicate message: %v", err)
	}

	failed := newServiceTestFixture()
	failed.repo.execution.Config = map[string]any{"token": "secret-config"}
	failed.repo.execution.TaskRunInput = map[string]any{"document": "secret-input"}
	failed.executor.result = ExecutionResult{
		Output:        map[string]any{"document": "secret-output"},
		FailureReason: "secret-failure",
	}
	failed.repo.finalizeChanged = true
	failed.repo.finalizeWorkflowRun = workflow.WorkflowRun{
		ID:         "workflow-run-id",
		WorkflowID: "workflow-id",
		Status:     string(workflow.WorkflowRunStatusFailed),
	}
	if err := failed.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process failed task: %v", err)
	}

	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"duplicate_task_message"`,
		`"msg":"workflow_run_finalized"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
		}
	}
	for _, secret := range []string{"secret-config", "secret-input", "secret-output", "secret-failure"} {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("expected log output not to contain %q, got %s", secret, logOutput)
		}
	}
}

func TestServiceProcessOneDoesNotAckWhenDuplicateStatusIsUnknown(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.claimer.claimErr = workflow.ErrTaskRunNotClaimable
	fixture.repo.statusErr = workflow.ErrTaskRunNotFound

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskRunNotFound) {
		t.Fatalf("expected ErrTaskRunNotFound, got %v", err)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected unknown task run message to remain pending, got acks %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOnePersistsExecutorFailureAndAcknowledgesTask(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{FailureReason: "random failure"}

	err := fixture.service.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(fixture.repo.completions, []workflow.CompleteTaskAttemptInput{{
		TaskAttemptID: "attempt-id",
		TaskRunID:     "task-run-id",
		WorkerID:      "worker-1",
		Success:       false,
		FailureReason: "random failure",
	}}) {
		t.Fatalf("unexpected completions: got %#v", fixture.repo.completions)
	}
	if !reflect.DeepEqual(fixture.repo.finalized, []string{"workflow-run-id"}) {
		t.Fatalf("unexpected finalized workflow run IDs: %#v", fixture.repo.finalized)
	}
	if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acked, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOnePersistsUnknownExecutorFailureAndAcknowledgesTask(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.repo.execution.ExecutorType = "unknown"

	err := fixture.service.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(fixture.repo.completions, []workflow.CompleteTaskAttemptInput{{
		TaskAttemptID: "attempt-id",
		TaskRunID:     "task-run-id",
		WorkerID:      "worker-1",
		Success:       false,
		FailureReason: ErrUnknownExecutorType.Error(),
	}}) {
		t.Fatalf("unexpected completions: got %#v", fixture.repo.completions)
	}
	if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acked, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneDoesNotAckWhenCompletionPersistenceFails(t *testing.T) {
	completeErr := errors.New("complete failed")
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	fixture.repo.completeErr = completeErr

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, completeErr) {
		t.Fatalf("expected completion error, got %v", err)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements after completion failure, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneDoesNotAckWhenLateCompletionIsRejected(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	fixture.repo.completeErr = workflow.ErrTaskAttemptNotCompletable

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskAttemptNotCompletable) {
		t.Fatalf("expected ErrTaskAttemptNotCompletable, got %v", err)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected no acknowledgements after stale completion, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneIncrementsLateCompletionPendingMetrics(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	fixture.repo.completeErr = workflow.ErrTaskAttemptNotCompletable

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskAttemptNotCompletable) {
		t.Fatalf("expected ErrTaskAttemptNotCompletable, got %v", err)
	}

	for name, want := range map[string]int{
		"goflow_worker_late_completions_rejected_total": 1,
		"goflow_worker_messages_left_pending_total":     1,
	} {
		if got := metrics.counts[name]; got != want {
			t.Fatalf("expected %s=%d, got %d", name, want, got)
		}
	}
	if got := metrics.counts["goflow_worker_messages_acknowledged_total"]; got != 0 {
		t.Fatalf("expected no acknowledged metric, got %d", got)
	}
}

func TestServiceProcessOneDoesNotCompleteOrAckWhenHeartbeatLosesLease(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.service.config.HeartbeatInterval = time.Millisecond
	fixture.repo.extendErr = workflow.ErrTaskRunLeaseNotExtensible
	fixture.executor.waitForCancel = true

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskRunLeaseNotExtensible) {
		t.Fatalf("expected ErrTaskRunLeaseNotExtensible, got %v", err)
	}
	if len(fixture.repo.extensions) == 0 {
		t.Fatal("expected heartbeat lease extension attempt")
	}
	if len(fixture.repo.completions) != 0 {
		t.Fatalf("expected no completion after lost lease, got %#v", fixture.repo.completions)
	}
	if len(fixture.consumer.acks) != 0 {
		t.Fatalf("expected no ack after lost lease, got %#v", fixture.consumer.acks)
	}
}

func TestServiceProcessOneIncrementsHeartbeatFailureAndPendingMetrics(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.service.config.HeartbeatInterval = time.Millisecond
	fixture.repo.extendErr = workflow.ErrTaskRunLeaseNotExtensible
	fixture.executor.waitForCancel = true

	err := fixture.service.ProcessOne(context.Background())
	if !errors.Is(err, workflow.ErrTaskRunLeaseNotExtensible) {
		t.Fatalf("expected ErrTaskRunLeaseNotExtensible, got %v", err)
	}

	for name, want := range map[string]int{
		"goflow_worker_lease_heartbeat_failures_total": 1,
		"goflow_worker_messages_left_pending_total":    1,
	} {
		if got := metrics.counts[name]; got != want {
			t.Fatalf("expected %s=%d, got %d", name, want, got)
		}
	}
	if got := metrics.counts["goflow_worker_messages_acknowledged_total"]; got != 0 {
		t.Fatalf("expected no acknowledged metric, got %d", got)
	}
}

func TestServiceProcessOneIncrementsSuccessfulHeartbeatMetric(t *testing.T) {
	fixture := newServiceTestFixture()
	metrics := &fakeMetrics{}
	fixture.service.metrics = metrics
	fixture.service.config.HeartbeatInterval = time.Millisecond
	fixture.executor.sleep = 5 * time.Millisecond
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if got := metrics.counts["goflow_worker_lease_heartbeats_total"]; got == 0 {
		t.Fatal("expected at least one successful heartbeat metric")
	}
}

func TestServiceProcessOneAcksOnlyAfterCompletionPersistence(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	var events []string
	fixture.repo.events = &events
	fixture.consumer.events = &events

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(events, []string{"complete", "finalize", "ack"}) {
		t.Fatalf("expected completion and finalization before ack, got %#v", events)
	}
}

func TestServiceProcessOneSchedulesSuccessorsAfterCompletionBeforeAck(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{Output: map[string]any{"status": "completed"}}
	var events []string
	fixture.repo.events = &events
	fixture.consumer.events = &events
	scheduler := &fakeScheduler{events: &events}
	fixture.service.scheduler = scheduler

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if !reflect.DeepEqual(scheduler.workflowRunIDs, []string{"workflow-run-id"}) {
		t.Fatalf("unexpected scheduler workflow run IDs: %#v", scheduler.workflowRunIDs)
	}
	if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected ack after scheduling, got %#v", fixture.consumer.acks)
	}
	if !reflect.DeepEqual(events, []string{"complete", "finalize", "schedule", "ack"}) {
		t.Fatalf("expected completion, schedule, ack order, got %#v", events)
	}
}

func TestServiceProcessOneDoesNotScheduleSuccessorsAfterDeadLetter(t *testing.T) {
	fixture := newServiceTestFixture()
	fixture.executor.result = ExecutionResult{FailureReason: "permanent failure"}
	scheduler := &fakeScheduler{}
	fixture.service.scheduler = scheduler

	if err := fixture.service.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process one task: %v", err)
	}

	if len(scheduler.workflowRunIDs) != 0 {
		t.Fatalf("expected no successor scheduling after dead-letter, got %#v", scheduler.workflowRunIDs)
	}
	if !reflect.DeepEqual(fixture.repo.finalized, []string{"workflow-run-id"}) {
		t.Fatalf("unexpected finalized workflow run IDs: %#v", fixture.repo.finalized)
	}
	if !reflect.DeepEqual(fixture.consumer.acks, []string{"redis-message-id"}) {
		t.Fatalf("expected redis-message-id to be acked, got %#v", fixture.consumer.acks)
	}
}

type serviceTestFixture struct {
	consumer *fakeConsumer
	claimer  *fakeTaskRunClaimer
	repo     *fakeExecutionRepository
	executor *fakeExecutionExecutor
	service  *Service
}

func newServiceTestFixture() serviceTestFixture {
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
		claimed: workflow.TaskRun{ID: "task-run-id", Status: workflow.TaskRunStatusRunning},
	}
	repo := &fakeExecutionRepository{
		taskRunStatus: workflow.TaskRunStatusRunning,
		execution: workflow.TaskRunExecution{
			WorkflowID:    "workflow-id",
			WorkflowRunID: "workflow-run-id",
			TaskID:        "task-id",
			TaskRunID:     "task-run-id",
			ExecutorType:  ExecutorTypeLog,
			Config:        map[string]any{"message": "from config"},
			TaskRunInput:  map[string]any{"message": "from input"},
		},
		attempt: workflow.TaskAttempt{
			ID:            "attempt-id",
			TaskRunID:     "task-run-id",
			AttemptNumber: 1,
			Status:        workflow.TaskAttemptStatusRunning,
		},
	}
	executor := &fakeExecutionExecutor{}

	return serviceTestFixture{
		consumer: consumer,
		claimer:  claimer,
		repo:     repo,
		executor: executor,
		service: NewService(
			ServiceConfig{WorkerID: "worker-1", LeaseDuration: 30 * time.Second},
			consumer,
			claimer,
			repo,
			NewExecutorRegistry(map[string]Executor{ExecutorTypeLog: executor}),
		),
	}
}

type fakeConsumer struct {
	received   queue.ReceivedTaskMessage
	receiveErr error
	acks       []string
	ackErr     error
	events     *[]string
}

func (c *fakeConsumer) ReceiveTask(context.Context) (queue.ReceivedTaskMessage, error) {
	if c.receiveErr != nil {
		return queue.ReceivedTaskMessage{}, c.receiveErr
	}
	return c.received, nil
}

func (c *fakeConsumer) AckTask(_ context.Context, messageID string) error {
	c.acks = append(c.acks, messageID)
	if c.events != nil {
		*c.events = append(*c.events, "ack")
	}
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

type fakeExecutionRepository struct {
	taskRunStatus       workflow.TaskRunStatus
	statusErr           error
	execution           workflow.TaskRunExecution
	loadErr             error
	attempt             workflow.TaskAttempt
	createErr           error
	completeErr         error
	extendErr           error
	finalizeErr         error
	finalizeChanged     bool
	finalizeWorkflowRun workflow.WorkflowRun
	loads               []workflow.LoadTaskRunExecutionInput
	createdAttempts     []string
	completions         []workflow.CompleteTaskAttemptInput
	extensions          []workflow.ExtendTaskRunLeaseInput
	finalized           []string
	events              *[]string
}

func (r *fakeExecutionRepository) LoadTaskRunStatus(_ context.Context, input workflow.LoadTaskRunStatusInput) (workflow.TaskRunStatus, error) {
	if r.statusErr != nil {
		return "", r.statusErr
	}
	return r.taskRunStatus, nil
}

func (r *fakeExecutionRepository) LoadTaskRunExecution(_ context.Context, input workflow.LoadTaskRunExecutionInput) (workflow.TaskRunExecution, error) {
	r.loads = append(r.loads, input)
	if r.loadErr != nil {
		return workflow.TaskRunExecution{}, r.loadErr
	}
	return r.execution, nil
}

func (r *fakeExecutionRepository) CreateTaskAttempt(_ context.Context, taskRunID string) (workflow.TaskAttempt, error) {
	r.createdAttempts = append(r.createdAttempts, taskRunID)
	if r.createErr != nil {
		return workflow.TaskAttempt{}, r.createErr
	}
	return r.attempt, nil
}

func (r *fakeExecutionRepository) CompleteTaskAttempt(_ context.Context, input workflow.CompleteTaskAttemptInput) (workflow.CompleteTaskAttemptResult, error) {
	r.completions = append(r.completions, input)
	if r.completeErr != nil {
		return workflow.CompleteTaskAttemptResult{}, r.completeErr
	}
	if r.events != nil {
		*r.events = append(*r.events, "complete")
	}
	return workflow.CompleteTaskAttemptResult{
		TaskAttempt: workflow.TaskAttempt{ID: input.TaskAttemptID, TaskRunID: input.TaskRunID, Status: completedAttemptStatus(input)},
		TaskRun:     workflow.TaskRun{ID: input.TaskRunID, Status: completedTaskRunStatus(input)},
	}, nil
}

func (r *fakeExecutionRepository) ExtendTaskRunLease(_ context.Context, input workflow.ExtendTaskRunLeaseInput) (workflow.TaskRun, error) {
	r.extensions = append(r.extensions, input)
	if r.extendErr != nil {
		return workflow.TaskRun{}, r.extendErr
	}
	return workflow.TaskRun{
		ID:             input.TaskRunID,
		Status:         workflow.TaskRunStatusRunning,
		LeaseExpiresAt: time.Now().Add(input.LeaseDuration),
	}, nil
}

func (r *fakeExecutionRepository) FinalizeWorkflowRun(_ context.Context, workflowRunID string) (workflow.WorkflowRun, bool, error) {
	r.finalized = append(r.finalized, workflowRunID)
	if r.finalizeErr != nil {
		return workflow.WorkflowRun{}, false, r.finalizeErr
	}
	if r.events != nil {
		*r.events = append(*r.events, "finalize")
	}
	if r.finalizeWorkflowRun.ID == "" {
		r.finalizeWorkflowRun = workflow.WorkflowRun{ID: workflowRunID, Status: string(workflow.WorkflowRunStatusRunning)}
	}
	return r.finalizeWorkflowRun, r.finalizeChanged, nil
}

func completedTaskRunStatus(input workflow.CompleteTaskAttemptInput) workflow.TaskRunStatus {
	if input.Success {
		return workflow.TaskRunStatusCompleted
	}
	if input.Retry {
		return workflow.TaskRunStatusRetryWait
	}
	return workflow.TaskRunStatusDeadLetter
}

func completedAttemptStatus(input workflow.CompleteTaskAttemptInput) workflow.TaskAttemptStatus {
	if input.Success {
		return workflow.TaskAttemptStatusCompleted
	}
	return workflow.TaskAttemptStatusFailed
}

type fakeExecutionExecutor struct {
	result        ExecutionResult
	err           error
	inputs        []ExecutionInput
	waitForCancel bool
	sleep         time.Duration
}

func (e *fakeExecutionExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	e.inputs = append(e.inputs, input)
	if e.waitForCancel {
		<-ctx.Done()
		return ExecutionResult{}, ctx.Err()
	}
	if e.sleep > 0 {
		select {
		case <-time.After(e.sleep):
		case <-ctx.Done():
			return ExecutionResult{}, ctx.Err()
		}
	}
	return e.result, e.err
}

type fakeScheduler struct {
	workflowRunIDs []string
	err            error
	events         *[]string
}

func (s *fakeScheduler) QueueRunnableTaskRuns(_ context.Context, workflowRunID string) error {
	s.workflowRunIDs = append(s.workflowRunIDs, workflowRunID)
	if s.events != nil {
		*s.events = append(*s.events, "schedule")
	}
	return s.err
}

type fakeMetrics struct {
	counts map[string]int
}

func (m *fakeMetrics) Inc(name string) {
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[name]++
}
