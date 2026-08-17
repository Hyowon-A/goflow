package recallify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecallifyNotifyCallbackExecutorSkipsBlankCallback(t *testing.T) {
	result, err := (RecallifyNotifyCallbackExecutor{}).Execute(context.Background(), recallifyCallbackInput(""))
	if err != nil {
		t.Fatalf("notify callback: %v", err)
	}
	if result.Output["skipped"] != true {
		t.Fatalf("expected skipped callback, got %#v", result.Output)
	}
}

func TestRecallifyNotifyCallbackExecutorPostsSummary(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode callback: %v", err)
		}
	}))
	defer server.Close()

	result, err := (RecallifyNotifyCallbackExecutor{}).Execute(context.Background(), recallifyCallbackInput(server.URL))
	if err != nil {
		t.Fatalf("notify callback: %v", err)
	}
	if result.Output["posted"] != true {
		t.Fatalf("expected posted callback, got %#v", result.Output)
	}
	if got["status"] != "completed" || got["external_request_id"] != "req-1" {
		t.Fatalf("unexpected callback payload: %#v", got)
	}
	summary, ok := got["summary"].(map[string]any)
	if !ok || summary["title"] != "GoFlow Basics" || summary["level"] != "medium" || summary["mcq_count"] != float64(1) {
		t.Fatalf("unexpected callback summary: %#v", got["summary"])
	}
}

func TestRecallifyNotifyCallbackExecutorClassifiesStatuses(t *testing.T) {
	for status, retryable := range map[int]bool{
		http.StatusInternalServerError: true,
		http.StatusServiceUnavailable:  true,
		http.StatusBadRequest:          false,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		}))
		result, err := (RecallifyNotifyCallbackExecutor{}).Execute(context.Background(), recallifyCallbackInput(server.URL))
		server.Close()
		if err != nil {
			t.Fatalf("status %d callback: %v", status, err)
		}
		if result.FailureReason == "" || result.Retryable != retryable {
			t.Fatalf("status %d retryable=%v result=%#v", status, retryable, result)
		}
	}
}

func TestRecallifyNotifyCallbackExecutorFailsWithoutMergedOutput(t *testing.T) {
	_, err := (RecallifyNotifyCallbackExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"workflow_input": recallifyWorkflowInput("http://localhost/callback"),
		},
	})
	if !errors.Is(err, ErrInvalidRecallifyCallbackInput) {
		t.Fatalf("expected ErrInvalidRecallifyCallbackInput, got %v", err)
	}
}

func recallifyCallbackInput(callbackURL string) ExecutionInput {
	return ExecutionInput{TaskRunInput: map[string]any{
		"workflow_input": recallifyWorkflowInput(callbackURL),
		"predecessors": map[string]any{
			"merge_study_set": map[string]any{
				"title": "GoFlow Basics",
				"level": "medium",
				"counts": map[string]any{
					"mcqs": 1,
				},
				"external_request_id": "req-1",
			},
		},
	}}
}

func recallifyWorkflowInput(callbackURL string) map[string]any {
	return map[string]any{
		"document_text":       "notes",
		"title":               "GoFlow Basics",
		"level":               "medium",
		"mcq_count":           1,
		"callback_url":        callbackURL,
		"external_request_id": "req-1",
	}
}
