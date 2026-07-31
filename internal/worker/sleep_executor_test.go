package worker

import (
	"context"
	"errors"
	"testing"
)

func TestSleepExecutorCompletesWithValidDuration(t *testing.T) {
	result, err := (SleepExecutor{}).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"duration": "1ms"},
	})
	if err != nil {
		t.Fatalf("execute sleep: %v", err)
	}

	if result.Output["status"] != "completed" {
		t.Fatalf("expected completed status, got %q", result.Output["status"])
	}
	if result.FailureReason != "" {
		t.Fatalf("expected no failure reason, got %q", result.FailureReason)
	}
	if result.Retryable {
		t.Fatal("expected successful sleep to be non-retryable")
	}
}

func TestSleepExecutorRejectsInvalidDurations(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
	}{
		{name: "missing", config: map[string]any{}},
		{name: "non-string duration", config: map[string]any{"duration": 1}},
		{name: "invalid duration", config: map[string]any{"duration": "not-a-duration"}},
		{name: "negative duration", config: map[string]any{"duration": "-1s"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := (SleepExecutor{}).Execute(context.Background(), ExecutionInput{
				Config: tt.config,
			})
			if !errors.Is(err, ErrInvalidSleepDuration) {
				t.Fatalf("expected ErrInvalidSleepDuration, got %v", err)
			}
		})
	}
}

func TestSleepExecutorStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := (SleepExecutor{}).Execute(ctx, ExecutionInput{
		Config: map[string]any{"duration": "1h"},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
