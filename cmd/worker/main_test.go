package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Hyowon-A/goflow/internal/metrics"
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

func TestWorkerMetricsHandlerHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	workerMetricsHandler(metrics.NewRegistry()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected health 200, got %d", rec.Code)
	}
}

func TestWorkerMetricsHandlerMetrics(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	workerMetricsHandler(metrics.NewRegistry()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected metrics 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "goflow_outbox_pending") {
		t.Fatalf("expected metrics body to include outbox gauge, got:\n%s", rec.Body.String())
	}
}
