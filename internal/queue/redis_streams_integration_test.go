package queue

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisStreamPublisherPublishesTaskMessage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamName = fmt.Sprintf("goflow:test:%d", time.Now().UnixNano())
	skipIfRedisUnavailable(t, cfg.Addr)
	t.Cleanup(func() {
		_, _ = redisCommand(cfg.Addr, "DEL", cfg.StreamName)
	})

	publisher, err := NewRedisStreamPublisher(cfg)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Fatalf("close publisher: %v", err)
		}
	})

	firstMessage := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id-1",
	}
	firstMessageID, err := publisher.PublishTask(context.Background(), firstMessage)
	if err != nil {
		t.Fatalf("publish first task: %v", err)
	}
	if firstMessageID == "" {
		t.Fatal("expected first Redis stream message ID")
	}

	secondMessage := TaskMessage{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id-2",
	}
	secondMessageID, err := publisher.PublishTask(context.Background(), secondMessage)
	if err != nil {
		t.Fatalf("publish second task: %v", err)
	}
	if secondMessageID == "" {
		t.Fatal("expected second Redis stream message ID")
	}
	if secondMessageID == firstMessageID {
		t.Fatalf("expected unique Redis stream message IDs, got %q", firstMessageID)
	}

	entries := redisStreamEntries(t, cfg.Addr, cfg.StreamName)
	if len(entries) != 2 {
		t.Fatalf("expected two Redis stream entries, got %#v", entries)
	}

	wantFirstFields := map[string]string{
		"schema_version":  TaskMessageSchemaVersion,
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id-1",
	}
	if !reflect.DeepEqual(entries[0].fields, wantFirstFields) {
		t.Fatalf("unexpected first Redis stream fields: got %#v, want %#v", entries[0].fields, wantFirstFields)
	}

	wantSecondFields := map[string]string{
		"schema_version":  TaskMessageSchemaVersion,
		"workflow_id":     "workflow-id",
		"workflow_run_id": "workflow-run-id",
		"task_id":         "task-id",
		"task_run_id":     "task-run-id-2",
	}
	if !reflect.DeepEqual(entries[1].fields, wantSecondFields) {
		t.Fatalf("unexpected second Redis stream fields: got %#v, want %#v", entries[1].fields, wantSecondFields)
	}
}

func skipIfRedisUnavailable(t *testing.T, addr string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	_ = conn.Close()
}

type redisStreamEntry struct {
	id     string
	fields map[string]string
}

func redisStreamEntries(t *testing.T, addr, streamName string) []redisStreamEntry {
	t.Helper()

	value, err := redisCommand(addr, "XRANGE", streamName, "-", "+")
	if err != nil {
		t.Fatalf("read Redis stream entries: %v", err)
	}

	entries := make([]redisStreamEntry, 0, len(value.array))
	for _, entryValue := range value.array {
		if len(entryValue.array) != 2 {
			t.Fatalf("unexpected Redis stream entry shape: %#v", entryValue)
		}

		fieldValues := entryValue.array[1].array
		if len(fieldValues)%2 != 0 {
			t.Fatalf("expected even Redis field/value count, got %#v", fieldValues)
		}

		fields := make(map[string]string, len(fieldValues)/2)
		for i := 0; i < len(fieldValues); i += 2 {
			fields[fieldValues[i].str] = fieldValues[i+1].str
		}

		entries = append(entries, redisStreamEntry{
			id:     entryValue.array[0].str,
			fields: fields,
		})
	}

	return entries
}

type redisValue struct {
	kind  byte
	str   string
	array []redisValue
}

func redisCommand(addr string, args ...string) (redisValue, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return redisValue{}, err
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "*%d\r\n", len(args)); err != nil {
		return redisValue{}, err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return redisValue{}, err
		}
	}

	value, err := readRedisValue(bufio.NewReader(conn))
	if err != nil {
		return redisValue{}, err
	}
	if value.kind == '-' {
		return redisValue{}, fmt.Errorf("redis error: %s", value.str)
	}

	return value, nil
}

func readRedisValue(reader *bufio.Reader) (redisValue, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return redisValue{}, err
	}

	switch prefix {
	case '+', '-', ':':
		line, err := readRedisLine(reader)
		if err != nil {
			return redisValue{}, err
		}
		return redisValue{kind: prefix, str: line}, nil
	case '$':
		line, err := readRedisLine(reader)
		if err != nil {
			return redisValue{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return redisValue{}, err
		}
		if length < 0 {
			return redisValue{kind: prefix}, nil
		}

		data := make([]byte, length+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return redisValue{}, err
		}
		return redisValue{kind: prefix, str: string(data[:length])}, nil
	case '*':
		line, err := readRedisLine(reader)
		if err != nil {
			return redisValue{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return redisValue{}, err
		}
		values := make([]redisValue, 0, length)
		for i := 0; i < length; i++ {
			value, err := readRedisValue(reader)
			if err != nil {
				return redisValue{}, err
			}
			values = append(values, value)
		}
		return redisValue{kind: prefix, array: values}, nil
	default:
		return redisValue{}, fmt.Errorf("unknown redis response prefix %q", prefix)
	}
}

func readRedisLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
