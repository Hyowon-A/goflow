package workflow

import (
	"errors"
	"os"
	"regexp"
	"slices"
	"testing"
)

func TestValidateWorkflowRunTransitionAcceptsValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from WorkflowRunStatus
		to   WorkflowRunStatus
	}{
		{
			name: "pending to running",
			from: WorkflowRunStatusPending,
			to:   WorkflowRunStatusRunning,
		},
		{
			name: "running to completed",
			from: WorkflowRunStatusRunning,
			to:   WorkflowRunStatusCompleted,
		},
		{
			name: "running to failed",
			from: WorkflowRunStatusRunning,
			to:   WorkflowRunStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateWorkflowRunTransition(tt.from, tt.to); err != nil {
				t.Fatalf("expected transition %q -> %q to be valid, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateWorkflowRunTransitionRejectsInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from WorkflowRunStatus
		to   WorkflowRunStatus
	}{
		{
			name: "pending cannot complete before running",
			from: WorkflowRunStatusPending,
			to:   WorkflowRunStatusCompleted,
		},
		{
			name: "pending cannot fail before running",
			from: WorkflowRunStatusPending,
			to:   WorkflowRunStatusFailed,
		},
		{
			name: "running cannot go back to pending",
			from: WorkflowRunStatusRunning,
			to:   WorkflowRunStatusPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWorkflowRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition for %q -> %q, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateWorkflowRunTransitionRejectsTerminalStateTransitions(t *testing.T) {
	tests := []struct {
		from WorkflowRunStatus
		to   WorkflowRunStatus
	}{
		{from: WorkflowRunStatusCompleted, to: WorkflowRunStatusRunning},
		{from: WorkflowRunStatusCompleted, to: WorkflowRunStatusFailed},
		{from: WorkflowRunStatusFailed, to: WorkflowRunStatusRunning},
		{from: WorkflowRunStatusFailed, to: WorkflowRunStatusCompleted},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+" to "+string(tt.to), func(t *testing.T) {
			if !IsTerminalWorkflowRunStatus(tt.from) {
				t.Fatalf("expected %q to be terminal", tt.from)
			}

			err := ValidateWorkflowRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected terminal transition to return ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestValidateWorkflowRunTransitionRejectsSameStateTransitions(t *testing.T) {
	statuses := []WorkflowRunStatus{
		WorkflowRunStatusPending,
		WorkflowRunStatusRunning,
		WorkflowRunStatusCompleted,
		WorkflowRunStatusFailed,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			err := ValidateWorkflowRunTransition(status, status)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected same-state transition for %q to return ErrInvalidTransition, got %v", status, err)
			}
		})
	}
}

func TestValidateWorkflowRunTransitionRejectsUnknownStates(t *testing.T) {
	tests := []struct {
		name string
		from WorkflowRunStatus
		to   WorkflowRunStatus
	}{
		{
			name: "unknown source",
			from: WorkflowRunStatus("queued"),
			to:   WorkflowRunStatusRunning,
		},
		{
			name: "unknown destination",
			from: WorkflowRunStatusPending,
			to:   WorkflowRunStatus("dead_letter"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateWorkflowRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrUnknownWorkflowRunStatus) {
				t.Fatalf("expected ErrUnknownWorkflowRunStatus, got %v", err)
			}
		})
	}
}

func TestValidateTaskRunTransitionAcceptsValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskRunStatus
		to   TaskRunStatus
	}{
		{
			name: "pending to queued",
			from: TaskRunStatusPending,
			to:   TaskRunStatusQueued,
		},
		{
			name: "queued to running",
			from: TaskRunStatusQueued,
			to:   TaskRunStatusRunning,
		},
		{
			name: "running to completed",
			from: TaskRunStatusRunning,
			to:   TaskRunStatusCompleted,
		},
		{
			name: "running to dead letter",
			from: TaskRunStatusRunning,
			to:   TaskRunStatusDeadLetter,
		},
		{
			name: "running to retry wait",
			from: TaskRunStatusRunning,
			to:   TaskRunStatusRetryWait,
		},
		{
			name: "failed to retry wait",
			from: TaskRunStatusFailed,
			to:   TaskRunStatusRetryWait,
		},
		{
			name: "failed to dead letter",
			from: TaskRunStatusFailed,
			to:   TaskRunStatusDeadLetter,
		},
		{
			name: "retry wait to queued",
			from: TaskRunStatusRetryWait,
			to:   TaskRunStatusQueued,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateTaskRunTransition(tt.from, tt.to); err != nil {
				t.Fatalf("expected transition %q -> %q to be valid, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateTaskRunTransitionRejectsInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskRunStatus
		to   TaskRunStatus
	}{
		{
			name: "pending cannot run before queue",
			from: TaskRunStatusPending,
			to:   TaskRunStatusRunning,
		},
		{
			name: "queued cannot complete before running",
			from: TaskRunStatusQueued,
			to:   TaskRunStatusCompleted,
		},
		{
			name: "running permanent failure must dead letter",
			from: TaskRunStatusRunning,
			to:   TaskRunStatusFailed,
		},
		{
			name: "retry wait cannot run before requeue",
			from: TaskRunStatusRetryWait,
			to:   TaskRunStatusRunning,
		},
		{
			name: "failed cannot go directly back to running",
			from: TaskRunStatusFailed,
			to:   TaskRunStatusRunning,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTaskRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition for %q -> %q, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateTaskRunTransitionRejectsTerminalStateTransitions(t *testing.T) {
	tests := []struct {
		from TaskRunStatus
		to   TaskRunStatus
	}{
		{from: TaskRunStatusCompleted, to: TaskRunStatusQueued},
		{from: TaskRunStatusCompleted, to: TaskRunStatusFailed},
		{from: TaskRunStatusDeadLetter, to: TaskRunStatusQueued},
		{from: TaskRunStatusDeadLetter, to: TaskRunStatusRunning},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+" to "+string(tt.to), func(t *testing.T) {
			if !IsTerminalTaskRunStatus(tt.from) {
				t.Fatalf("expected %q to be terminal", tt.from)
			}

			err := ValidateTaskRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected terminal transition to return ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestValidateTaskRunTransitionRejectsSameStateTransitions(t *testing.T) {
	statuses := []TaskRunStatus{
		TaskRunStatusPending,
		TaskRunStatusQueued,
		TaskRunStatusRunning,
		TaskRunStatusCompleted,
		TaskRunStatusFailed,
		TaskRunStatusRetryWait,
		TaskRunStatusDeadLetter,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			err := ValidateTaskRunTransition(status, status)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected same-state transition for %q to return ErrInvalidTransition, got %v", status, err)
			}
		})
	}
}

func TestValidateTaskRunTransitionRejectsUnknownStates(t *testing.T) {
	tests := []struct {
		name string
		from TaskRunStatus
		to   TaskRunStatus
	}{
		{
			name: "unknown source",
			from: TaskRunStatus("waiting"),
			to:   TaskRunStatusQueued,
		},
		{
			name: "unknown destination",
			from: TaskRunStatusPending,
			to:   TaskRunStatus("success"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTaskRunTransition(tt.from, tt.to)
			if !errors.Is(err, ErrUnknownTaskRunStatus) {
				t.Fatalf("expected ErrUnknownTaskRunStatus, got %v", err)
			}
		})
	}
}

func TestValidateTaskAttemptTransitionAcceptsValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskAttemptStatus
		to   TaskAttemptStatus
	}{
		{
			name: "running to completed",
			from: TaskAttemptStatusRunning,
			to:   TaskAttemptStatusCompleted,
		},
		{
			name: "running to failed",
			from: TaskAttemptStatusRunning,
			to:   TaskAttemptStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateTaskAttemptTransition(tt.from, tt.to); err != nil {
				t.Fatalf("expected transition %q -> %q to be valid, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateTaskAttemptTransitionRejectsInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskAttemptStatus
		to   TaskAttemptStatus
	}{
		{
			name: "completed cannot return to running",
			from: TaskAttemptStatusCompleted,
			to:   TaskAttemptStatusRunning,
		},
		{
			name: "failed cannot return to running",
			from: TaskAttemptStatusFailed,
			to:   TaskAttemptStatusRunning,
		},
		{
			name: "completed cannot become failed",
			from: TaskAttemptStatusCompleted,
			to:   TaskAttemptStatusFailed,
		},
		{
			name: "failed cannot become completed",
			from: TaskAttemptStatusFailed,
			to:   TaskAttemptStatusCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTaskAttemptTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition for %q -> %q, got %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestValidateTaskAttemptTransitionRejectsTerminalStateTransitions(t *testing.T) {
	tests := []struct {
		from TaskAttemptStatus
		to   TaskAttemptStatus
	}{
		{from: TaskAttemptStatusCompleted, to: TaskAttemptStatusRunning},
		{from: TaskAttemptStatusCompleted, to: TaskAttemptStatusFailed},
		{from: TaskAttemptStatusFailed, to: TaskAttemptStatusRunning},
		{from: TaskAttemptStatusFailed, to: TaskAttemptStatusCompleted},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+" to "+string(tt.to), func(t *testing.T) {
			if !IsTerminalTaskAttemptStatus(tt.from) {
				t.Fatalf("expected %q to be terminal", tt.from)
			}

			err := ValidateTaskAttemptTransition(tt.from, tt.to)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected terminal transition to return ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestValidateTaskAttemptTransitionRejectsSameStateTransitions(t *testing.T) {
	statuses := []TaskAttemptStatus{
		TaskAttemptStatusRunning,
		TaskAttemptStatusCompleted,
		TaskAttemptStatusFailed,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			err := ValidateTaskAttemptTransition(status, status)
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected same-state transition for %q to return ErrInvalidTransition, got %v", status, err)
			}
		})
	}
}

func TestValidateTaskAttemptTransitionRejectsUnknownStates(t *testing.T) {
	tests := []struct {
		name string
		from TaskAttemptStatus
		to   TaskAttemptStatus
	}{
		{
			name: "unknown source",
			from: TaskAttemptStatus("pending"),
			to:   TaskAttemptStatusRunning,
		},
		{
			name: "unknown destination",
			from: TaskAttemptStatusRunning,
			to:   TaskAttemptStatus("dead_letter"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTaskAttemptTransition(tt.from, tt.to)
			if !errors.Is(err, ErrUnknownTaskAttemptStatus) {
				t.Fatalf("expected ErrUnknownTaskAttemptStatus, got %v", err)
			}
		})
	}
}

func TestWorkflowRunStatusConstantsAlignWithDatabaseEnum(t *testing.T) {
	migration, err := os.ReadFile("../../migrations/001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	enumValues := extractPostgresEnumValues(t, string(migration), "workflow_run_status")
	constantValues := []string{
		string(WorkflowRunStatusPending),
		string(WorkflowRunStatusRunning),
		string(WorkflowRunStatusCompleted),
		string(WorkflowRunStatusFailed),
	}

	slices.Sort(enumValues)
	slices.Sort(constantValues)

	if !slices.Equal(enumValues, constantValues) {
		t.Fatalf("expected workflow run status constants %v to match database enum %v", constantValues, enumValues)
	}
}

func TestTaskRunStatusConstantsAlignWithDatabaseEnum(t *testing.T) {
	migration, err := os.ReadFile("../../migrations/001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	enumValues := extractPostgresEnumValues(t, string(migration), "task_run_status")
	constantValues := []string{
		string(TaskRunStatusPending),
		string(TaskRunStatusQueued),
		string(TaskRunStatusRunning),
		string(TaskRunStatusRetryWait),
		string(TaskRunStatusCompleted),
		string(TaskRunStatusFailed),
		string(TaskRunStatusDeadLetter),
	}

	slices.Sort(enumValues)
	slices.Sort(constantValues)

	if !slices.Equal(enumValues, constantValues) {
		t.Fatalf("expected task run status constants %v to match database enum %v", constantValues, enumValues)
	}
}

func TestTaskAttemptStatusConstantsAlignWithDatabaseEnum(t *testing.T) {
	migration, err := os.ReadFile("../../migrations/001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	enumValues := extractPostgresEnumValues(t, string(migration), "task_attempt_status")
	constantValues := []string{
		string(TaskAttemptStatusRunning),
		string(TaskAttemptStatusCompleted),
		string(TaskAttemptStatusFailed),
	}

	slices.Sort(enumValues)
	slices.Sort(constantValues)

	if !slices.Equal(enumValues, constantValues) {
		t.Fatalf("expected task attempt status constants %v to match database enum %v", constantValues, enumValues)
	}
}

func TestWorkflowAndAttemptStatusesAreSeparateLifecycles(t *testing.T) {
	if string(WorkflowRunStatusPending) != string(TaskRunStatusPending) {
		t.Fatalf("expected shared pending spelling for workflow and task run statuses")
	}

	if string(WorkflowRunStatusCompleted) != string(TaskAttemptStatusCompleted) {
		t.Fatalf("expected shared completed spelling for workflow and task attempt statuses")
	}

	if IsKnownTaskRunStatus(TaskRunStatus(WorkflowRunStatusFailed)) != true {
		t.Fatalf("expected failed to be a valid task run status")
	}
}

func extractPostgresEnumValues(t *testing.T, migration string, enumName string) []string {
	t.Helper()

	enumPattern := regexp.MustCompile(`(?s)CREATE TYPE\s+` + regexp.QuoteMeta(enumName) + `\s+AS ENUM\s*\((.*?)\);`)
	enumMatch := enumPattern.FindStringSubmatch(migration)
	if enumMatch == nil {
		t.Fatalf("missing database enum %q", enumName)
	}

	valuePattern := regexp.MustCompile(`'([^']+)'`)
	matches := valuePattern.FindAllStringSubmatch(enumMatch[1], -1)
	if len(matches) == 0 {
		t.Fatalf("database enum %q has no values", enumName)
	}

	values := make([]string, 0, len(matches))
	for _, match := range matches {
		values = append(values, match[1])
	}

	return values
}
