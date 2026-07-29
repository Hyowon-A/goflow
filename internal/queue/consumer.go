package queue

import "context"

type TaskConsumer interface {
	ReceiveTask(ctx context.Context) (ReceivedTaskMessage, error)
	AckTask(ctx context.Context, messageID string) error
	Close() error
}

type ReceivedTaskMessage struct {
	MessageID string
	TaskMessage
}
