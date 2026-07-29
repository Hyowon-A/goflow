package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisStreamPublisher struct {
	config Config
	client *redis.Client
}

func NewRedisStreamPublisher(config Config) (*RedisStreamPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr: config.Addr,
	})

	return &RedisStreamPublisher{
		config: config,
		client: client,
	}, nil
}

func (p *RedisStreamPublisher) PublishTask(ctx context.Context, message TaskMessage) (string, error) {
	fields, err := message.Fields()
	if err != nil {
		return "", err
	}
	if p == nil || p.client == nil {
		return "", ErrInvalidConfig
	}

	values := make([]string, 0, len(fields)*2)
	for _, field := range fields {
		values = append(values, field.Name, field.Value)
	}

	messageID, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.config.StreamName,
		ID:     "*",
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("publish task message: %w", err)
	}

	return messageID, nil
}

func (p *RedisStreamPublisher) Close() error {
	if p == nil || p.client == nil {
		return nil
	}

	return p.client.Close()
}
