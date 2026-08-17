package recallify

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestValidateRecallifyMCQsNormalizesValidItems(t *testing.T) {
	mcqs, err := ValidateRecallifyMCQs(`[{
		"question":"  What   is GoFlow? ",
		"option1":" Workflow   engine ",
		"explanation1":" It runs durable jobs. ",
		"option2":"Database",
		"explanation2":"No.",
		"option3":"Web framework",
		"explanation3":"No.",
		"option4":"Queue only",
		"explanation4":"No.",
		"answer":1
	}]`, 1)
	if err != nil {
		t.Fatalf("validate mcqs: %v", err)
	}

	want := []map[string]any{{
		"question":     "What is GoFlow?",
		"option1":      "Workflow engine",
		"explanation1": "It runs durable jobs.",
		"option2":      "Database",
		"explanation2": "No.",
		"option3":      "Web framework",
		"explanation3": "No.",
		"option4":      "Queue only",
		"explanation4": "No.",
		"answer":       1,
	}}
	if !reflect.DeepEqual(mcqs, want) {
		t.Fatalf("unexpected mcqs:\n got %#v\nwant %#v", mcqs, want)
	}
}

func TestValidateRecallifyMCQsRejectsInvalidOutput(t *testing.T) {
	tests := map[string]string{
		"non array":         `{"question":"Q?"}`,
		"missing question":  `[{"option1":"A","explanation1":"A","option2":"B","explanation2":"B","option3":"C","explanation3":"C","option4":"D","explanation4":"D","answer":1}]`,
		"duplicate options": `[{"question":"Q?","option1":"A","explanation1":"A","option2":" a ","explanation2":"B","option3":"C","explanation3":"C","option4":"D","explanation4":"D","answer":1}]`,
		"bad answer":        `[{"question":"Q?","option1":"A","explanation1":"A","option2":"B","explanation2":"B","option3":"C","explanation3":"C","option4":"D","explanation4":"D","answer":5}]`,
		"missing expl":      `[{"question":"Q?","option1":"A","option2":"B","explanation2":"B","option3":"C","explanation3":"C","option4":"D","explanation4":"D","answer":1}]`,
		"wrong count":       `[{"question":"Q?","option1":"A","explanation1":"A","option2":"B","explanation2":"B","option3":"C","explanation3":"C","option4":"D","explanation4":"D","answer":1}]`,
	}

	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			requestedCount := 1
			if name == "wrong count" {
				requestedCount = 2
			}
			if _, err := ValidateRecallifyMCQs(raw, requestedCount); !errors.Is(err, ErrInvalidRecallifyMCQs) {
				t.Fatalf("expected ErrInvalidRecallifyMCQs, got %v", err)
			}
		})
	}
}

func TestValidateRecallifyMCQsRejectsDuplicateQuestions(t *testing.T) {
	raw := `[` + validRecallifyMCQJSON("Q?") + `,` + validRecallifyMCQJSON(" q? ") + `]`
	if _, err := ValidateRecallifyMCQs(raw, 2); !errors.Is(err, ErrInvalidRecallifyMCQs) {
		t.Fatalf("expected ErrInvalidRecallifyMCQs, got %v", err)
	}
}

func TestRecallifyValidateMCQsExecutorParsesAndEmitsSummary(t *testing.T) {
	result, err := (RecallifyValidateMCQsExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"predecessors": map[string]any{
				"generate_mcqs": map[string]any{
					"raw_json":        `[` + validRecallifyMCQJSON("Q?") + `]`,
					"requested_count": 1,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("execute validate mcqs: %v", err)
	}
	if result.Output["kind"] != "mcq" || result.Output["requested_count"] != 1 || result.Output["count"] != 1 {
		t.Fatalf("unexpected output metadata: %#v", result.Output)
	}
	if mcqs, ok := result.Output["mcqs"].([]map[string]any); !ok || len(mcqs) != 1 {
		t.Fatalf("expected one mcq, got %#v", result.Output["mcqs"])
	}
}

func TestRecallifyValidateMCQsExecutorFailsInvalidJSONWithoutRetry(t *testing.T) {
	result, err := (RecallifyValidateMCQsExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"raw_json":        `{"question":"Q?"}`,
			"requested_count": 1,
		},
	})
	if err != nil {
		t.Fatalf("execute validate mcqs: %v", err)
	}
	if result.FailureReason != ErrInvalidRecallifyMCQs.Error() {
		t.Fatalf("expected validation failure, got %#v", result)
	}
	if result.Retryable {
		t.Fatal("expected non-retryable validation failure")
	}
}

func TestRecallifyMergeStudySetExecutorEmitsPayload(t *testing.T) {
	mcqs, err := ValidateRecallifyMCQs(`[`+validRecallifyMCQJSON("Q?")+`]`, 1)
	if err != nil {
		t.Fatalf("validate fixture mcq: %v", err)
	}

	result, err := (RecallifyMergeStudySetExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"workflow_input": map[string]any{
				"document_text":        "notes",
				"title":                " Biology ",
				"level":                "easy",
				"mcq_count":            1,
				"external_request_id":  "req-1",
				"ignored_product_data": "ignored",
			},
			"predecessors": map[string]any{
				"validate_mcqs": map[string]any{"mcqs": mcqs},
			},
		},
	})
	if err != nil {
		t.Fatalf("execute merge: %v", err)
	}

	want := map[string]any{
		"title":               "Biology",
		"level":               "easy",
		"mcqs":                mcqs,
		"counts":              map[string]any{"mcqs": 1},
		"external_request_id": "req-1",
	}
	if !reflect.DeepEqual(result.Output, want) {
		t.Fatalf("unexpected output:\n got %#v\nwant %#v", result.Output, want)
	}
}

func TestRecallifyMergeStudySetExecutorFailsWithoutValidatedMCQs(t *testing.T) {
	_, err := (RecallifyMergeStudySetExecutor{}).Execute(context.Background(), ExecutionInput{
		TaskRunInput: map[string]any{
			"workflow_input": map[string]any{"document_text": "notes"},
		},
	})
	if !errors.Is(err, ErrInvalidRecallifyMergeInput) {
		t.Fatalf("expected ErrInvalidRecallifyMergeInput, got %v", err)
	}
}

func validRecallifyMCQJSON(question string) string {
	return strings.ReplaceAll(`{
		"question":"QUESTION",
		"option1":"A",
		"explanation1":"Because A.",
		"option2":"B",
		"explanation2":"Because B.",
		"option3":"C",
		"explanation3":"Because C.",
		"option4":"D",
		"explanation4":"Because D.",
		"answer":1
	}`, "QUESTION", question)
}
