package queue

import (
	"context"
	"errors"
	"testing"
)

func TestRedisStreamPublisherImplementsTaskPublisher(t *testing.T) {
	var _ TaskPublisher = (*RedisStreamPublisher)(nil)
}

func TestNewRedisStreamPublisherRejectsInvalidConfig(t *testing.T) {
	_, err := NewRedisStreamPublisher(Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestPublishTaskRejectsInvalidMessage(t *testing.T) {
	publisher := &RedisStreamPublisher{}

	_, err := publisher.PublishTask(context.Background(), TaskMessage{})
	if !errors.Is(err, ErrInvalidTaskMessage) {
		t.Fatalf("expected ErrInvalidTaskMessage, got %v", err)
	}
}
