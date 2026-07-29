package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestRedisStreamConsumerCreatesGroupIdempotently(t *testing.T) {
	cfg := DefaultConsumerConfig()
	cfg.StreamName = fmt.Sprintf("goflow:consumer-test:%d", time.Now().UnixNano())
	cfg.GroupName = "goflow-workers"
	cfg.ConsumerName = "worker-1"
	cfg.BlockTimeout = 10 * time.Millisecond
	skipIfRedisUnavailable(t, cfg.Addr)
	t.Cleanup(func() {
		_, _ = redisCommand(cfg.Addr, "DEL", cfg.StreamName)
	})

	first, err := NewRedisStreamConsumer(cfg)
	if err != nil {
		t.Fatalf("create first consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Fatalf("close first consumer: %v", err)
		}
	})

	second, err := NewRedisStreamConsumer(cfg)
	if err != nil {
		t.Fatalf("create second consumer for existing group: %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Fatalf("close second consumer: %v", err)
		}
	})
}

func TestRedisStreamConsumerReceivesPublishedTaskMessage(t *testing.T) {
	publisherCfg := DefaultConfig()
	publisherCfg.StreamName = fmt.Sprintf("goflow:consumer-test:%d", time.Now().UnixNano())
	skipIfRedisUnavailable(t, publisherCfg.Addr)
	t.Cleanup(func() {
		_, _ = redisCommand(publisherCfg.Addr, "DEL", publisherCfg.StreamName)
	})

	publisher, err := NewRedisStreamPublisher(publisherCfg)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Fatalf("close publisher: %v", err)
		}
	})

	consumerCfg := DefaultConsumerConfig()
	consumerCfg.StreamName = publisherCfg.StreamName
	consumerCfg.GroupName = "goflow-workers"
	consumerCfg.ConsumerName = "worker-1"
	consumerCfg.BlockTimeout = time.Second
	consumer, err := NewRedisStreamConsumer(consumerCfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Fatalf("close consumer: %v", err)
		}
	})

	message := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	}
	messageID, err := publisher.PublishTask(context.Background(), message)
	if err != nil {
		t.Fatalf("publish task: %v", err)
	}

	received, err := consumer.ReceiveTask(context.Background())
	if err != nil {
		t.Fatalf("receive task: %v", err)
	}
	if received.MessageID != messageID {
		t.Fatalf("expected Redis message ID %q, got %q", messageID, received.MessageID)
	}
	if received.TaskMessage != message {
		t.Fatalf("unexpected task message: got %#v, want %#v", received.TaskMessage, message)
	}
}

func TestRedisStreamConsumerGroupDeliversNewMessageToOneConsumer(t *testing.T) {
	publisherCfg := DefaultConfig()
	publisherCfg.StreamName = fmt.Sprintf("goflow:consumer-test:%d", time.Now().UnixNano())
	skipIfRedisUnavailable(t, publisherCfg.Addr)
	t.Cleanup(func() {
		_, _ = redisCommand(publisherCfg.Addr, "DEL", publisherCfg.StreamName)
	})

	publisher, err := NewRedisStreamPublisher(publisherCfg)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Fatalf("close publisher: %v", err)
		}
	})

	firstCfg := DefaultConsumerConfig()
	firstCfg.StreamName = publisherCfg.StreamName
	firstCfg.GroupName = "goflow-workers"
	firstCfg.ConsumerName = "worker-1"
	firstCfg.BlockTimeout = 20 * time.Millisecond
	firstConsumer, err := NewRedisStreamConsumer(firstCfg)
	if err != nil {
		t.Fatalf("create first consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := firstConsumer.Close(); err != nil {
			t.Fatalf("close first consumer: %v", err)
		}
	})

	secondCfg := firstCfg
	secondCfg.ConsumerName = "worker-2"
	secondConsumer, err := NewRedisStreamConsumer(secondCfg)
	if err != nil {
		t.Fatalf("create second consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := secondConsumer.Close(); err != nil {
			t.Fatalf("close second consumer: %v", err)
		}
	})

	_, err = publisher.PublishTask(context.Background(), TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	})
	if err != nil {
		t.Fatalf("publish task: %v", err)
	}

	firstReceived, firstErr := firstConsumer.ReceiveTask(context.Background())
	secondReceived, secondErr := secondConsumer.ReceiveTask(context.Background())

	gotCount := 0
	for _, result := range []struct {
		received ReceivedTaskMessage
		err      error
	}{
		{received: firstReceived, err: firstErr},
		{received: secondReceived, err: secondErr},
	} {
		if result.err == nil {
			gotCount++
			continue
		}
		if !errors.Is(result.err, ErrNoMessage) {
			t.Fatalf("expected ErrNoMessage for empty consumer read, got %v", result.err)
		}
	}

	if gotCount != 1 {
		t.Fatalf("expected exactly one consumer to receive the new message, got %d", gotCount)
	}
}

func TestRedisStreamConsumerAckRemovesMessageFromPending(t *testing.T) {
	publisherCfg := DefaultConfig()
	publisherCfg.StreamName = fmt.Sprintf("goflow:consumer-test:%d", time.Now().UnixNano())
	skipIfRedisUnavailable(t, publisherCfg.Addr)
	t.Cleanup(func() {
		_, _ = redisCommand(publisherCfg.Addr, "DEL", publisherCfg.StreamName)
	})

	publisher, err := NewRedisStreamPublisher(publisherCfg)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Fatalf("close publisher: %v", err)
		}
	})

	consumerCfg := DefaultConsumerConfig()
	consumerCfg.StreamName = publisherCfg.StreamName
	consumerCfg.GroupName = "goflow-workers"
	consumerCfg.ConsumerName = "worker-1"
	consumerCfg.BlockTimeout = time.Second
	consumer, err := NewRedisStreamConsumer(consumerCfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Fatalf("close consumer: %v", err)
		}
	})

	_, err = publisher.PublishTask(context.Background(), TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
	})
	if err != nil {
		t.Fatalf("publish task: %v", err)
	}

	received, err := consumer.ReceiveTask(context.Background())
	if err != nil {
		t.Fatalf("receive task: %v", err)
	}

	pendingBeforeAck := redisPendingCount(t, consumerCfg.Addr, consumerCfg.StreamName, consumerCfg.GroupName)
	if pendingBeforeAck != 1 {
		t.Fatalf("expected one pending message before ack, got %d", pendingBeforeAck)
	}

	if err := consumer.AckTask(context.Background(), received.MessageID); err != nil {
		t.Fatalf("ack task: %v", err)
	}

	pendingAfterAck := redisPendingCount(t, consumerCfg.Addr, consumerCfg.StreamName, consumerCfg.GroupName)
	if pendingAfterAck != 0 {
		t.Fatalf("expected no pending messages after ack, got %d", pendingAfterAck)
	}
}

func redisPendingCount(t *testing.T, addr, streamName, groupName string) int64 {
	t.Helper()

	value, err := redisCommand(addr, "XPENDING", streamName, groupName)
	if err != nil {
		t.Fatalf("read Redis pending count: %v", err)
	}
	if len(value.array) == 0 {
		return 0
	}
	count, err := strconv.ParseInt(value.array[0].str, 10, 64)
	if err != nil {
		t.Fatalf("parse Redis pending count %q: %v", value.array[0].str, err)
	}
	return count
}
