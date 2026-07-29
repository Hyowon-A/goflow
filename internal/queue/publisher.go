package queue

import "context"

type TaskPublisher interface {
	PublishTask(ctx context.Context, message TaskMessage) (string, error)
}
