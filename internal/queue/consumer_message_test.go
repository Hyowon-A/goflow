package queue

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseTaskMessageFields(t *testing.T) {
	fields := map[string]string{
		"schema_version":  TaskMessageSchemaVersion,
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id",
	}

	message, err := ParseTaskMessageFields(fields)
	if err != nil {
		t.Fatalf("parse task message fields: %v", err)
	}

	want := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	}
	if !reflect.DeepEqual(message, want) {
		t.Fatalf("unexpected task message: got %#v, want %#v", message, want)
	}
}

func TestParseTaskMessageFieldsIgnoresOutboxStorageFields(t *testing.T) {
	fields := map[string]string{
		"schema_version":   TaskMessageSchemaVersion,
		"workflow_id":      "workflow-id",
		"workflow_run_id":  "workflow-run-id",
		"task_id":          "task-id",
		"task_run_id":      "task-run-id",
		"outbox_event_id":  "event-id",
		"outbox_status":    "published",
		"redis_message_id": "1700000000000-0",
	}

	message, err := ParseTaskMessageFields(fields)
	if err != nil {
		t.Fatalf("parse task message fields: %v", err)
	}

	want := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	}
	if !reflect.DeepEqual(message, want) {
		t.Fatalf("unexpected task message: got %#v, want %#v", message, want)
	}
}

func TestParseTaskMessageFieldsRejectsMissingRequiredFields(t *testing.T) {
	valid := map[string]string{
		"schema_version":  TaskMessageSchemaVersion,
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id",
	}

	requiredFields := []string{
		"workflow_id",
		"workflow_run_id",
		"task_id",
		"task_run_id",
	}

	for _, field := range requiredFields {
		t.Run(field, func(t *testing.T) {
			fields := cloneFields(valid)
			delete(fields, field)

			_, err := ParseTaskMessageFields(fields)
			if !errors.Is(err, ErrInvalidTaskMessage) {
				t.Fatalf("expected ErrInvalidTaskMessage, got %v", err)
			}
		})
	}
}

func TestParseTaskMessageFieldsRejectsUnsupportedSchemaVersion(t *testing.T) {
	fields := map[string]string{
		"schema_version":  "999",
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id",
	}

	_, err := ParseTaskMessageFields(fields)
	if !errors.Is(err, ErrUnsupportedMessageVersion) {
		t.Fatalf("expected ErrUnsupportedMessageVersion, got %v", err)
	}
}

func TestReceivedTaskMessagePreservesRedisMessageID(t *testing.T) {
	fields := map[string]string{
		"schema_version":  TaskMessageSchemaVersion,
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id",
	}

	received, err := NewReceivedTaskMessage("1700000000000-0", fields)
	if err != nil {
		t.Fatalf("build received task message: %v", err)
	}

	if received.MessageID != "1700000000000-0" {
		t.Fatalf("expected Redis message ID 1700000000000-0, got %q", received.MessageID)
	}
	if received.TaskRunID != "task-run-id" {
		t.Fatalf("expected embedded task run ID task-run-id, got %q", received.TaskRunID)
	}
}

func cloneFields(fields map[string]string) map[string]string {
	cloned := make(map[string]string, len(fields))
	for key, value := range fields {
		cloned[key] = value
	}
	return cloned
}
