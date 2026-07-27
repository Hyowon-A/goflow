package queue

import (
	"errors"
	"reflect"
	"testing"
)

func TestTaskMessageFieldsAreStable(t *testing.T) {
	message := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	}

	fields, err := message.Fields()
	if err != nil {
		t.Fatalf("build message fields: %v", err)
	}

	want := []Field{
		{Name: "schema_version", Value: TaskMessageSchemaVersion},
		{Name: "workflow_id", Value: "workflow-id"},
		{Name: "workflow_run_id", Value: "workflow-run-id"},
		{Name: "task_id", Value: "task-id"},
		{Name: "task_run_id", Value: "task-run-id"},
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("unexpected fields: got %#v, want %#v", fields, want)
	}
}

func TestTaskMessageValidateRejectsMissingRequiredFields(t *testing.T) {
	valid := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	}

	tests := []struct {
		name    string
		message TaskMessage
	}{
		{
			name:    "missing workflow id",
			message: TaskMessage{WorkflowRunID: valid.WorkflowRunID, TaskID: valid.TaskID, TaskRunID: valid.TaskRunID},
		},
		{
			name:    "missing workflow run id",
			message: TaskMessage{WorkflowID: valid.WorkflowID, TaskID: valid.TaskID, TaskRunID: valid.TaskRunID},
		},
		{
			name:    "missing task id",
			message: TaskMessage{WorkflowID: valid.WorkflowID, WorkflowRunID: valid.WorkflowRunID, TaskRunID: valid.TaskRunID},
		},
		{
			name:    "missing task run id",
			message: TaskMessage{WorkflowID: valid.WorkflowID, WorkflowRunID: valid.WorkflowRunID, TaskID: valid.TaskID},
		},
		{
			name: "blank fields are invalid",
			message: TaskMessage{
				WorkflowID:    " ",
				WorkflowRunID: valid.WorkflowRunID,
				TaskID:        valid.TaskID,
				TaskRunID:     valid.TaskRunID,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.message.Validate()
			if !errors.Is(err, ErrInvalidTaskMessage) {
				t.Fatalf("expected ErrInvalidTaskMessage, got %v", err)
			}
		})
	}
}

func TestTaskMessageFieldsRejectsInvalidMessage(t *testing.T) {
	_, err := (TaskMessage{}).Fields()
	if !errors.Is(err, ErrInvalidTaskMessage) {
		t.Fatalf("expected ErrInvalidTaskMessage, got %v", err)
	}
}
