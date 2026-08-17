package recallify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecallifyGenerateMCQsExecutorSendsRequestAndReturnsMetadata(t *testing.T) {
	var request struct {
		Text  string `json:"text"`
		Count string `json:"count"`
		Level string `json:"level"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ai/generateMcqs" {
			t.Fatalf("expected /ai/generateMcqs, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected bearer token, got %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{"mcqs":"[{\"question\":\"Q?\"}]"}`))
	}))
	defer server.Close()

	result, err := NewRecallifyGenerateMCQsExecutor(RecallifyClient{}).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{
			"base_url":     server.URL,
			"bearer_token": "secret",
			"timeout":      "2s",
		},
		TaskRunInput: map[string]any{
			"predecessors": map[string]any{
				"clean_text": map[string]any{
					"clean_text": " clean study notes ",
					"mcq_count":  2,
					"level":      "hard",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if request.Text != "clean study notes" || request.Count != "2" || request.Level != "hard" {
		t.Fatalf("unexpected request: %+v", request)
	}
	if result.FailureReason != "" || result.Retryable {
		t.Fatalf("expected success, got %+v", result)
	}
	if result.Output["kind"] != "mcq" {
		t.Fatalf("expected kind mcq, got %#v", result.Output["kind"])
	}
	if result.Output["requested_count"] != 2 {
		t.Fatalf("expected requested_count 2, got %#v", result.Output["requested_count"])
	}
	if result.Output["raw_json"] != `[{"question":"Q?"}]` {
		t.Fatalf("unexpected raw_json: %#v", result.Output["raw_json"])
	}
}

func TestRecallifyGenerateMCQsExecutorMapsTransientFailureToRetryableResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	result, err := NewRecallifyGenerateMCQsExecutor(RecallifyClient{}).Execute(context.Background(), recallifyGenerateMCQsExecutionInput(server.URL))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.FailureReason == "" {
		t.Fatal("expected failure reason")
	}
	if !result.Retryable {
		t.Fatal("expected retryable failure")
	}
}

func TestRecallifyGenerateMCQsExecutorMapsPermanentFailureToNonRetryableResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	result, err := NewRecallifyGenerateMCQsExecutor(RecallifyClient{}).Execute(context.Background(), recallifyGenerateMCQsExecutionInput(server.URL))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.FailureReason == "" {
		t.Fatal("expected failure reason")
	}
	if result.Retryable {
		t.Fatal("expected non-retryable failure")
	}
}

func TestRecallifyGenerateMCQsExecutorRejectsBadConfigBeforeRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer server.Close()

	_, err := NewRecallifyGenerateMCQsExecutor(RecallifyClient{}).Execute(context.Background(), ExecutionInput{
		Config:       map[string]any{"base_url": " "},
		TaskRunInput: recallifyGenerateMCQsExecutionInput(server.URL).TaskRunInput,
	})
	if !errors.Is(err, ErrInvalidRecallifyGenerateMCQsConfig) {
		t.Fatalf("expected ErrInvalidRecallifyGenerateMCQsConfig, got %v", err)
	}
	if called {
		t.Fatal("expected no HTTP request")
	}
}

func TestRecallifyGenerateMCQsExecutorRejectsBadInputBeforeRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer server.Close()

	_, err := NewRecallifyGenerateMCQsExecutor(RecallifyClient{}).Execute(context.Background(), ExecutionInput{
		Config: map[string]any{"base_url": server.URL},
		TaskRunInput: map[string]any{
			"predecessors": map[string]any{"clean_text": map[string]any{"clean_text": ""}},
		},
	})
	if !errors.Is(err, ErrInvalidRecallifyGenerateMCQsInput) {
		t.Fatalf("expected ErrInvalidRecallifyGenerateMCQsInput, got %v", err)
	}
	if called {
		t.Fatal("expected no HTTP request")
	}
}

func TestRecallifyGenerateMCQsExecutorStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := (RecallifyGenerateMCQsExecutor{}).Execute(ctx, ExecutionInput{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func recallifyGenerateMCQsExecutionInput(baseURL string) ExecutionInput {
	return ExecutionInput{
		Config: map[string]any{"base_url": baseURL},
		TaskRunInput: map[string]any{
			"clean_text": "notes",
			"mcq_count":  1,
			"level":      "medium",
		},
	}
}
