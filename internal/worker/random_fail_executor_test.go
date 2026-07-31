package worker

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestRandomFailExecutorCompletesWhenProbabilityIsZero(t *testing.T) {
	result, err := NewRandomFailExecutor(func() float64 { return 0 }).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"failure_probability": float64(0)},
	})
	if err != nil {
		t.Fatalf("execute random fail: %v", err)
	}

	if result.Output["status"] != "completed" || result.Output["success"] != true {
		t.Fatalf("unexpected random fail output: %#v", result.Output)
	}
	if result.FailureReason != "" {
		t.Fatalf("expected no failure reason, got %q", result.FailureReason)
	}
}

func TestRandomFailExecutorFailsWhenProbabilityIsOne(t *testing.T) {
	result, err := NewRandomFailExecutor(func() float64 { return 0 }).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"failure_probability": float64(1)},
	})
	if err != nil {
		t.Fatalf("execute random fail: %v", err)
	}
	if result.FailureReason == "" {
		t.Fatal("expected failure reason")
	}
	if result.Output != nil {
		t.Fatalf("expected no output on failure, got %q", result.Output)
	}
}

func TestRandomFailExecutorUsesRandomDraw(t *testing.T) {
	tests := []struct {
		name        string
		draw        float64
		wantFailure bool
	}{
		{name: "draw below probability fails", draw: 0.49, wantFailure: true},
		{name: "draw equal to probability succeeds", draw: 0.5, wantFailure: false},
		{name: "draw above probability succeeds", draw: 0.51, wantFailure: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewRandomFailExecutor(func() float64 { return tt.draw }).Execute(context.Background(), ExecutionInput{
				Config: map[string]any{"failure_probability": 0.5},
			})
			if err != nil {
				t.Fatalf("execute random fail: %v", err)
			}

			gotFailure := result.FailureReason != ""
			if gotFailure != tt.wantFailure {
				t.Fatalf("failure mismatch: got %v, want %v, result %#v", gotFailure, tt.wantFailure, result)
			}
		})
	}
}

func TestRandomFailExecutorFallsBackToTaskRunInput(t *testing.T) {
	result, err := NewRandomFailExecutor(func() float64 { return 0.75 }).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{"failure_probability": 0.5},
	})
	if err != nil {
		t.Fatalf("execute random fail: %v", err)
	}
	if result.FailureReason != "" {
		t.Fatalf("expected success, got failure reason %q", result.FailureReason)
	}
}

func TestRandomFailExecutorRejectsInvalidFailureProbability(t *testing.T) {
	tests := []struct {
		name  string
		input ExecutionInput
	}{
		{name: "missing", input: ExecutionInput{Config: map[string]any{}}},
		{name: "non-float", input: ExecutionInput{Config: map[string]any{"failure_probability": "0.5"}}},
		{name: "negative", input: ExecutionInput{Config: map[string]any{"failure_probability": -0.1}}},
		{name: "greater than one", input: ExecutionInput{Config: map[string]any{"failure_probability": 1.1}}},
		{name: "nan", input: ExecutionInput{Config: map[string]any{"failure_probability": math.NaN()}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRandomFailExecutor(func() float64 { return 0 }).Execute(context.Background(), tt.input)
			if !errors.Is(err, ErrInvalidFailureProbability) {
				t.Fatalf("expected ErrInvalidFailureProbability, got %v", err)
			}
		})
	}
}

func TestRandomFailExecutorRejectsInvalidRandomDraw(t *testing.T) {
	tests := []struct {
		name string
		draw float64
	}{
		{name: "negative", draw: -0.1},
		{name: "one", draw: 1},
		{name: "greater than one", draw: 1.1},
		{name: "nan", draw: math.NaN()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRandomFailExecutor(func() float64 { return tt.draw }).Execute(context.Background(), ExecutionInput{
				Config: map[string]any{"failure_probability": 0.5},
			})
			if !errors.Is(err, ErrInvalidRandomDraw) {
				t.Fatalf("expected ErrInvalidRandomDraw, got %v", err)
			}
		})
	}
}

func TestRandomFailExecutorZeroValueUsesDefaultRandom(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("zero-value executor panicked: %v", recovered)
		}
	}()

	result, err := (RandomFailExecutor{}).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"failure_probability": 0.5},
	})
	if err != nil {
		t.Fatalf("execute zero-value random fail: %v", err)
	}
	if result.Output == nil && result.FailureReason == "" {
		t.Fatalf("expected success output or failure reason, got %#v", result)
	}
}

func TestRandomFailExecutorStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewRandomFailExecutor(func() float64 { return 0 }).Execute(ctx, ExecutionInput{
		Config: map[string]any{"failure_probability": 0.5},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
