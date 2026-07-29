package queue

import "strings"

func ParseTaskMessageFields(fields map[string]string) (TaskMessage, error) {
	if fields == nil {
		return TaskMessage{}, ErrInvalidTaskMessage
	}

	schemaVersion := strings.TrimSpace(fields["schema_version"])
	if schemaVersion == "" {
		return TaskMessage{}, ErrInvalidTaskMessage
	}
	if schemaVersion != TaskMessageSchemaVersion {
		return TaskMessage{}, ErrUnsupportedMessageVersion
	}

	message := TaskMessage{
		WorkflowID:    fields["workflow_id"],
		WorkflowRunID: fields["workflow_run_id"],
		TaskID:        fields["task_id"],
		TaskRunID:     fields["task_run_id"],
	}
	if err := message.Validate(); err != nil {
		return TaskMessage{}, err
	}

	return message, nil
}

func NewReceivedTaskMessage(messageID string, fields map[string]string) (ReceivedTaskMessage, error) {
	if strings.TrimSpace(messageID) == "" {
		return ReceivedTaskMessage{}, ErrInvalidTaskMessage
	}

	message, err := ParseTaskMessageFields(fields)
	if err != nil {
		return ReceivedTaskMessage{}, err
	}

	return ReceivedTaskMessage{
		MessageID:   messageID,
		TaskMessage: message,
	}, nil
}
