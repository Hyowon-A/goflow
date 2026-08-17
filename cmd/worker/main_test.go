package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Hyowon-A/goflow/internal/recallify"
	"github.com/Hyowon-A/goflow/internal/worker"
)

func TestNewExecutorRegistryResolvesRecallifyExecutors(t *testing.T) {
	registry := newExecutorRegistry()

	for _, executorType := range []string{
		recallify.ExecutorTypeValidateRequest,
		recallify.ExecutorTypeCleanText,
		recallify.ExecutorTypeGenerateMCQs,
		recallify.ExecutorTypeValidateMCQs,
		recallify.ExecutorTypeMergeStudySet,
		recallify.ExecutorTypeNotifyCallback,
	} {
		if _, err := registry.Resolve(executorType); err != nil {
			t.Fatalf("resolve %s: %v", executorType, err)
		}
	}
}

func TestRecallifyConfigIsRequiredWhenGenerateTaskRuns(t *testing.T) {
	executor, err := newExecutorRegistry().Resolve(recallify.ExecutorTypeGenerateMCQs)
	if err != nil {
		t.Fatalf("resolve generate executor: %v", err)
	}

	_, err = executor.Execute(context.Background(), worker.ExecutionInput{
		TaskRunInput: map[string]any{
			"clean_text": "study notes",
			"mcq_count":  1,
			"level":      "medium",
		},
	})
	if !errors.Is(err, recallify.ErrInvalidRecallifyGenerateMCQsConfig) {
		t.Fatalf("expected ErrInvalidRecallifyGenerateMCQsConfig, got %v", err)
	}
}
