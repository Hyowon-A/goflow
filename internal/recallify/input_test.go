package recallify

import (
	"errors"
	"reflect"
	"testing"
)

func TestNormalizeRecallifyRunInputAcceptsValidInput(t *testing.T) {
	got, err := NormalizeRecallifyRunInput(map[string]any{
		"document_text":       " lecture notes ",
		"title":               "Operating Systems Week 3",
		"level":               "hard",
		"mcq_count":           float64(7),
		"callback_url":        " http://localhost:3000/api/goflow/callback ",
		"external_request_id": " recallify-request-123 ",
	})
	if err != nil {
		t.Fatalf("normalize recallify input: %v", err)
	}

	want := RecallifyRunInput{
		DocumentText:      "lecture notes",
		Title:             "Operating Systems Week 3",
		Level:             "hard",
		MCQCount:          7,
		CallbackURL:       "http://localhost:3000/api/goflow/callback",
		ExternalRequestID: "recallify-request-123",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized input mismatch:\n got %#v\nwant %#v", got, want)
	}
}

func TestNormalizeRecallifyRunInputRejectsBlankDocumentText(t *testing.T) {
	_, err := NormalizeRecallifyRunInput(map[string]any{"document_text": "  \n\t "})
	if !errors.Is(err, ErrMissingRecallifyDocumentText) {
		t.Fatalf("expected ErrMissingRecallifyDocumentText, got %v", err)
	}
}

func TestNormalizeRecallifyRunInputRejectsInvalidLevel(t *testing.T) {
	for _, level := range []any{"expert", "   "} {
		_, err := NormalizeRecallifyRunInput(map[string]any{
			"document_text": "lecture notes",
			"level":         level,
		})
		if !errors.Is(err, ErrInvalidRecallifyLevel) {
			t.Fatalf("expected ErrInvalidRecallifyLevel, got %v", err)
		}
	}
}

func TestNormalizeRecallifyRunInputRejectsZeroRequestedItems(t *testing.T) {
	_, err := NormalizeRecallifyRunInput(map[string]any{
		"document_text": "lecture notes",
		"mcq_count":     0,
	})
	if !errors.Is(err, ErrInvalidRecallifyCount) {
		t.Fatalf("expected ErrInvalidRecallifyCount, got %v", err)
	}
}

func TestNormalizeRecallifyRunInputDefaultsOptionalFields(t *testing.T) {
	got, err := NormalizeRecallifyRunInput(map[string]any{"document_text": "lecture notes"})
	if err != nil {
		t.Fatalf("normalize recallify input: %v", err)
	}

	want := RecallifyRunInput{
		DocumentText: "lecture notes",
		Title:        DefaultRecallifyTitle,
		Level:        DefaultRecallifyLevel,
		MCQCount:     DefaultRecallifyCount,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized input mismatch:\n got %#v\nwant %#v", got, want)
	}
}

func TestNormalizeRecallifyRunInputRejectsNullTitle(t *testing.T) {
	_, err := NormalizeRecallifyRunInput(map[string]any{
		"document_text": "lecture notes",
		"title":         nil,
	})
	if !errors.Is(err, ErrInvalidRecallifyField) {
		t.Fatalf("expected ErrInvalidRecallifyField, got %v", err)
	}
}

func TestNormalizeRecallifyRunInputRejectsInvalidCounts(t *testing.T) {
	tests := []struct {
		name  string
		count any
	}{
		{name: "negative", count: -1},
		{name: "fractional", count: 1.5},
		{name: "string", count: "10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NormalizeRecallifyRunInput(map[string]any{
				"document_text": "lecture notes",
				"mcq_count":     tt.count,
			})
			if !errors.Is(err, ErrInvalidRecallifyCount) {
				t.Fatalf("expected ErrInvalidRecallifyCount, got %v", err)
			}
		})
	}
}
