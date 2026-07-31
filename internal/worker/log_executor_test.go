package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"testing"
)

func TestLogExecutorUsesConfigMessageAndWritesStructuredOutput(t *testing.T) {
	var logs bytes.Buffer
	executor := NewLogExecutor(log.New(&logs, "", 0))

	result, err := executor.Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"message": "hello"},
	})
	if err != nil {
		t.Fatalf("execute log: %v", err)
	}

	if result.Output["status"] != "completed" || result.Output["message"] != "hello" {
		t.Fatalf("unexpected log output: %#v", result.Output)
	}
	encoded, err := json.Marshal(result.Output)
	if err != nil {
		t.Fatalf("encode log output: %v", err)
	}
	if logs.String() != string(encoded)+"\n" {
		t.Fatalf("expected structured output to be logged, got %q", logs.String())
	}
}

func TestLogExecutorFallsBackToTaskRunInput(t *testing.T) {
	executor := NewLogExecutor(log.New(io.Discard, "", 0))
	result, err := executor.Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{"message": "from input"},
	})
	if err != nil {
		t.Fatalf("execute log from task input: %v", err)
	}

	if result.Output["message"] != "from input" {
		t.Fatalf("expected task-run input message, got %q", result.Output["message"])
	}
}

func TestLogExecutorRejectsMissingMessage(t *testing.T) {
	_, err := (LogExecutor{}).Execute(context.Background(), ExecutionInput{
		Config:       map[string]any{"message": "   "},
		TaskRunInput: map[string]any{},
	})
	if !errors.Is(err, ErrMissingMessage) {
		t.Fatalf("expected ErrMissingMessage, got %v", err)
	}
}
