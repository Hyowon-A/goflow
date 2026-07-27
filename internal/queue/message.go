package queue

import "strings"

const TaskMessageSchemaVersion = "1"

type Field struct {
	Name  string
	Value string
}

type TaskMessage struct {
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	TaskRunID     string
}

func (m TaskMessage) Validate() error {
	if strings.TrimSpace(m.WorkflowID) == "" {
		return ErrInvalidTaskMessage
	}
	if strings.TrimSpace(m.WorkflowRunID) == "" {
		return ErrInvalidTaskMessage
	}
	if strings.TrimSpace(m.TaskID) == "" {
		return ErrInvalidTaskMessage
	}
	if strings.TrimSpace(m.TaskRunID) == "" {
		return ErrInvalidTaskMessage
	}

	return nil
}

func (m TaskMessage) Fields() ([]Field, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	return []Field{
		{Name: "schema_version", Value: TaskMessageSchemaVersion},
		{Name: "workflow_id", Value: m.WorkflowID},
		{Name: "workflow_run_id", Value: m.WorkflowRunID},
		{Name: "task_id", Value: m.TaskID},
		{Name: "task_run_id", Value: m.TaskRunID},
	}, nil
}
