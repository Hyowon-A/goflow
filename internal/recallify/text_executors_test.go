package recallify

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRecallifyValidateRequestExecutorReturnsNormalizedDefaults(t *testing.T) {
	result, err := (RecallifyValidateRequestExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{"document_text": " lecture notes "},
	})
	if err != nil {
		t.Fatalf("execute validate request: %v", err)
	}

	want := map[string]any{
		"document_text": "lecture notes",
		"title":         DefaultRecallifyTitle,
		"level":         DefaultRecallifyLevel,
		"mcq_count":     DefaultRecallifyCount,
	}
	if !reflect.DeepEqual(result.Output, want) {
		t.Fatalf("unexpected output:\n got %#v\nwant %#v", result.Output, want)
	}
}

func TestRecallifyValidateRequestExecutorRejectsInvalidInput(t *testing.T) {
	_, err := (RecallifyValidateRequestExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{"document_text": " "},
	})
	if !errors.Is(err, ErrMissingRecallifyDocumentText) {
		t.Fatalf("expected ErrMissingRecallifyDocumentText, got %v", err)
	}
}

func TestRecallifyCleanTextExecutorCollapsesWhitespace(t *testing.T) {
	result, err := (RecallifyCleanTextExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"predecessors": map[string]any{
				"validate_request": map[string]any{
					"document_text":       " hello\x00\n\n\tworld ",
					"title":               "OS",
					"level":               "easy",
					"mcq_count":           3,
					"external_request_id": "req-1",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("execute clean text: %v", err)
	}

	want := map[string]any{
		"clean_text":          "hello world",
		"title":               "OS",
		"level":               "easy",
		"mcq_count":           3,
		"external_request_id": "req-1",
	}
	if !reflect.DeepEqual(result.Output, want) {
		t.Fatalf("unexpected output:\n got %#v\nwant %#v", result.Output, want)
	}
}

func TestRecallifyCleanTextExecutorRejectsOversizedTextWithoutRetry(t *testing.T) {
	result, err := (RecallifyCleanTextExecutor{}).Execute(context.Background(), ExecutionInput{
		Config:       map[string]any{"max_text_bytes": 5},
		TaskRunInput: map[string]any{"document_text": "too large"},
	})
	if err != nil {
		t.Fatalf("execute clean text: %v", err)
	}
	if result.FailureReason != recallifyTextTooLargeFailure {
		t.Fatalf("expected oversized failure, got %#v", result)
	}
	if result.Retryable {
		t.Fatal("expected oversized input to be non-retryable")
	}
}

func TestRecallifyCleanTextExecutorRejectsInvalidMaxTextBytes(t *testing.T) {
	_, err := (RecallifyCleanTextExecutor{}).Execute(context.Background(), ExecutionInput{
		Config:       map[string]any{"max_text_bytes": 0},
		TaskRunInput: map[string]any{"document_text": "lecture notes"},
	})
	if !errors.Is(err, ErrInvalidRecallifyMaxTextBytes) {
		t.Fatalf("expected ErrInvalidRecallifyMaxTextBytes, got %v", err)
	}
}
