package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestExecutorRegistryResolvesKnownTypesThroughExecutorContract(t *testing.T) {
	fakes := map[string]*fakeExecutor{
		ExecutorTypeSleep:      {output: map[string]any{"type": "sleep"}},
		ExecutorTypeLog:        {output: map[string]any{"type": "log"}},
		ExecutorTypeRandomFail: {output: map[string]any{"type": "random_fail"}},
	}
	registered := map[string]Executor{
		ExecutorTypeSleep:      fakes[ExecutorTypeSleep],
		ExecutorTypeLog:        fakes[ExecutorTypeLog],
		ExecutorTypeRandomFail: fakes[ExecutorTypeRandomFail],
	}
	registry := NewExecutorRegistry(registered)

	input := ExecutionInput{
		WorkflowID:    "workflow-id",
		WorkflowRunID: "workflow-run-id",
		TaskID:        "task-id",
		TaskRunID:     "task-run-id",
		ExecutorType:  ExecutorTypeSleep,
		Config:        map[string]any{"duration": "1s"},
		TaskRunInput:  map[string]any{"value": "input"},
	}

	for _, executorType := range []string{
		ExecutorTypeSleep,
		ExecutorTypeLog,
		ExecutorTypeRandomFail,
	} {
		t.Run(executorType, func(t *testing.T) {
			executor, err := registry.Resolve("  " + executorType + "  ")
			if err != nil {
				t.Fatalf("resolve %q: %v", executorType, err)
			}
			if executor == nil {
				t.Fatal("expected a non-nil executor")
			}

			input.ExecutorType = executorType
			result, err := executor.Execute(context.Background(), input)
			if err != nil {
				t.Fatalf("execute %q: %v", executorType, err)
			}
			if result.Output == nil {
				t.Fatal("expected executor output")
			}
			if !reflect.DeepEqual(fakes[executorType].inputs, []ExecutionInput{input}) {
				t.Fatalf("unexpected execution input: got %#v, want %#v", fakes[executorType].inputs, []ExecutionInput{input})
			}
		})
	}
}

func TestExecutorRegistryRejectsUnknownTypesWithStableError(t *testing.T) {
	registry := NewExecutorRegistry(map[string]Executor{
		ExecutorTypeSleep: &fakeExecutor{},
	})

	for _, executorType := range []string{"missing", "", "   "} {
		t.Run(executorType, func(t *testing.T) {
			executor, err := registry.Resolve(executorType)
			if !errors.Is(err, ErrUnknownExecutorType) {
				t.Fatalf("expected ErrUnknownExecutorType, got %v", err)
			}
			if executor != nil {
				t.Fatalf("expected no executor for %q, got %T", executorType, executor)
			}
		})
	}
}

type fakeExecutor struct {
	output map[string]any
	inputs []ExecutionInput
}

func (f *fakeExecutor) Execute(_ context.Context, input ExecutionInput) (ExecutionResult, error) {
	f.inputs = append(f.inputs, input)
	return ExecutionResult{Output: f.output}, nil
}
