package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisStreamConsumer struct {
	config ConsumerConfig
	client *redis.Client
}

func NewRedisStreamConsumer(config ConsumerConfig) (*RedisStreamConsumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr: config.Addr,
	})

	if err := ensureRedisStreamConsumerGroup(context.Background(), client, config); err != nil {
		_ = client.Close()
		return nil, err
	}

	return &RedisStreamConsumer{
		config: config,
		client: client,
	}, nil
}

func ensureRedisStreamConsumerGroup(ctx context.Context, client *redis.Client, config ConsumerConfig) error {
	err := client.XGroupCreateMkStream(ctx, config.StreamName, config.GroupName, "$").Err()
	if err == nil {
		return nil
	}
	if isRedisConsumerGroupExistsError(err) {
		return nil
	}

	return fmt.Errorf("create redis stream consumer group: %w", err)
}

func isRedisConsumerGroupExistsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func (c *RedisStreamConsumer) ReceiveTask(ctx context.Context) (ReceivedTaskMessage, error) {
	if c == nil || c.client == nil {
		return ReceivedTaskMessage{}, ErrInvalidConfig
	}

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.config.GroupName,
		Consumer: c.config.ConsumerName,
		Streams:  []string{c.config.StreamName, ">"},
		Count:    int64(c.config.Count),
		Block:    c.config.BlockTimeout,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ReceivedTaskMessage{}, ErrNoMessage
		}
		return ReceivedTaskMessage{}, fmt.Errorf("receive task message: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return ReceivedTaskMessage{}, ErrNoMessage
	}

	message := streams[0].Messages[0]
	fields, err := redisMessageFields(message.Values)
	if err != nil {
		return ReceivedTaskMessage{}, err
	}

	return NewReceivedTaskMessage(message.ID, fields)
}

func redisMessageFields(values map[string]interface{}) (map[string]string, error) {
	fields := make(map[string]string, len(values))
	for key, value := range values {
		switch typedValue := value.(type) {
		case string:
			fields[key] = typedValue
		case []byte:
			fields[key] = string(typedValue)
		default:
			fields[key] = fmt.Sprint(typedValue)
		}
	}

	return fields, nil
}

func (c *RedisStreamConsumer) AckTask(ctx context.Context, messageID string) error {
	if c == nil || c.client == nil {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(messageID) == "" {
		return ErrInvalidTaskMessage
	}

	if err := c.client.XAck(ctx, c.config.StreamName, c.config.GroupName, messageID).Err(); err != nil {
		return fmt.Errorf("ack task message: %w", err)
	}

	return nil
}

func (c *RedisStreamConsumer) Close() error {
	if c == nil || c.client == nil {
		return nil
	}

	return c.client.Close()
}
