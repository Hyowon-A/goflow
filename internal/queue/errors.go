package queue

import "errors"

var (
	ErrInvalidConfig      = errors.New("invalid_queue_config")
	ErrInvalidTaskMessage = errors.New("invalid_task_message")
)
