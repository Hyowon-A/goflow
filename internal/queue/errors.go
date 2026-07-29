package queue

import "errors"

var (
	ErrInvalidConfig             = errors.New("invalid_queue_config")
	ErrInvalidTaskMessage        = errors.New("invalid_task_message")
	ErrUnsupportedMessageVersion = errors.New("unsupported_message_version")
	ErrNoMessage                 = errors.New("no_message")
)
