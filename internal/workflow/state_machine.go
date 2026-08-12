package workflow

import "fmt"

type stateStatus interface {
	~string
}

type stateMachine[S stateStatus] struct {
	transitions map[S]map[S]struct{}
	unknownErr  error
}

func (m stateMachine[S]) isKnown(status S) bool {
	_, ok := m.transitions[status]
	return ok
}

func (m stateMachine[S]) isTerminal(status S) bool {
	nextStates, ok := m.transitions[status]
	return ok && len(nextStates) == 0
}

func (m stateMachine[S]) validateTransition(from, to S) error {
	if !m.isKnown(from) {
		return fmt.Errorf("%w: from %q", m.unknownErr, from)
	}
	if !m.isKnown(to) {
		return fmt.Errorf("%w: to %q", m.unknownErr, to)
	}

	nextStates := m.transitions[from]
	if _, ok := nextStates[to]; !ok {
		return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, from, to)
	}

	return nil
}

type WorkflowRunStatus string

const (
	WorkflowRunStatusPending   WorkflowRunStatus = "pending"
	WorkflowRunStatusRunning   WorkflowRunStatus = "running"
	WorkflowRunStatusCompleted WorkflowRunStatus = "completed"
	WorkflowRunStatusFailed    WorkflowRunStatus = "failed"
)

type TaskRunStatus string

const (
	TaskRunStatusPending    TaskRunStatus = "pending"
	TaskRunStatusQueued     TaskRunStatus = "queued"
	TaskRunStatusRunning    TaskRunStatus = "running"
	TaskRunStatusCompleted  TaskRunStatus = "completed"
	TaskRunStatusFailed     TaskRunStatus = "failed"
	TaskRunStatusRetryWait  TaskRunStatus = "retry_wait"
	TaskRunStatusDeadLetter TaskRunStatus = "dead_letter"
)

type TaskAttemptStatus string

const (
	TaskAttemptStatusRunning   TaskAttemptStatus = "running"
	TaskAttemptStatusCompleted TaskAttemptStatus = "completed"
	TaskAttemptStatusFailed    TaskAttemptStatus = "failed"
)

var workflowRunStateMachine = stateMachine[WorkflowRunStatus]{
	unknownErr: ErrUnknownWorkflowRunStatus,
	transitions: map[WorkflowRunStatus]map[WorkflowRunStatus]struct{}{
		WorkflowRunStatusPending: {
			WorkflowRunStatusRunning: {},
		},
		WorkflowRunStatusRunning: {
			WorkflowRunStatusCompleted: {},
			WorkflowRunStatusFailed:    {},
		},
		WorkflowRunStatusCompleted: {},
		WorkflowRunStatusFailed:    {},
	},
}

var taskRunStateMachine = stateMachine[TaskRunStatus]{
	unknownErr: ErrUnknownTaskRunStatus,
	transitions: map[TaskRunStatus]map[TaskRunStatus]struct{}{
		TaskRunStatusPending: {
			TaskRunStatusQueued: {},
		},
		TaskRunStatusQueued: {
			TaskRunStatusRunning: {},
		},
		TaskRunStatusRunning: {
			TaskRunStatusQueued:     {},
			TaskRunStatusCompleted:  {},
			TaskRunStatusRetryWait:  {},
			TaskRunStatusDeadLetter: {},
		},
		TaskRunStatusFailed: {
			TaskRunStatusRetryWait:  {},
			TaskRunStatusDeadLetter: {},
		},
		TaskRunStatusRetryWait: {
			TaskRunStatusQueued: {},
		},
		TaskRunStatusCompleted:  {},
		TaskRunStatusDeadLetter: {},
	},
}

var taskAttemptStateMachine = stateMachine[TaskAttemptStatus]{
	unknownErr: ErrUnknownTaskAttemptStatus,
	transitions: map[TaskAttemptStatus]map[TaskAttemptStatus]struct{}{
		TaskAttemptStatusRunning: {
			TaskAttemptStatusCompleted: {},
			TaskAttemptStatusFailed:    {},
		},
		TaskAttemptStatusCompleted: {},
		TaskAttemptStatusFailed:    {},
	},
}

func IsKnownWorkflowRunStatus(status WorkflowRunStatus) bool {
	return workflowRunStateMachine.isKnown(status)
}

func IsTerminalWorkflowRunStatus(status WorkflowRunStatus) bool {
	return workflowRunStateMachine.isTerminal(status)
}

func ValidateWorkflowRunTransition(from, to WorkflowRunStatus) error {
	return workflowRunStateMachine.validateTransition(from, to)
}

func IsKnownTaskRunStatus(status TaskRunStatus) bool {
	return taskRunStateMachine.isKnown(status)
}

func IsTerminalTaskRunStatus(status TaskRunStatus) bool {
	return taskRunStateMachine.isTerminal(status)
}

func ValidateTaskRunTransition(from, to TaskRunStatus) error {
	return taskRunStateMachine.validateTransition(from, to)
}

func IsKnownTaskAttemptStatus(status TaskAttemptStatus) bool {
	return taskAttemptStateMachine.isKnown(status)
}

func IsTerminalTaskAttemptStatus(status TaskAttemptStatus) bool {
	return taskAttemptStateMachine.isTerminal(status)
}

func ValidateTaskAttemptTransition(from, to TaskAttemptStatus) error {
	return taskAttemptStateMachine.validateTransition(from, to)
}
