package recallify

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrInvalidRecallifyMCQsInput  = errors.New("invalid_recallify_mcqs_input")
	ErrInvalidRecallifyMCQs       = errors.New("invalid_recallify_mcqs")
	ErrInvalidRecallifyMergeInput = errors.New("invalid_recallify_merge_input")
)

type RecallifyValidateMCQsExecutor struct{}

type RecallifyMergeStudySetExecutor struct{}

type recallifyMCQ struct {
	Question     string `json:"question"`
	Option1      string `json:"option1"`
	Explanation1 string `json:"explanation1"`
	Option2      string `json:"option2"`
	Explanation2 string `json:"explanation2"`
	Option3      string `json:"option3"`
	Explanation3 string `json:"explanation3"`
	Option4      string `json:"option4"`
	Explanation4 string `json:"explanation4"`
	Answer       int    `json:"answer"`
}

func (RecallifyValidateMCQsExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	source := recallifyNamedPredecessorInput(input.TaskRunInput, "generate_mcqs")
	rawJSON, ok := source["raw_json"].(string)
	if !ok || strings.TrimSpace(rawJSON) == "" {
		return ExecutionResult{}, ErrInvalidRecallifyMCQsInput
	}
	requestedCount := 0
	if err := optionalCount(source, "requested_count", &requestedCount); err != nil || requestedCount <= 0 {
		return ExecutionResult{}, ErrInvalidRecallifyMCQsInput
	}

	mcqs, err := ValidateRecallifyMCQs(rawJSON, requestedCount)
	if err != nil {
		return ExecutionResult{FailureReason: err.Error()}, nil
	}

	return ExecutionResult{Output: map[string]any{
		"kind":            "mcq",
		"requested_count": requestedCount,
		"count":           len(mcqs),
		"mcqs":            mcqs,
	}}, nil
}

func (RecallifyMergeStudySetExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	validated := recallifyNamedPredecessorInput(input.TaskRunInput, "validate_mcqs")
	mcqs, ok := recallifyMCQMaps(validated["mcqs"])
	if !ok || len(mcqs) == 0 {
		return ExecutionResult{}, ErrInvalidRecallifyMergeInput
	}

	workflowInput, ok := input.TaskRunInput["workflow_input"].(map[string]any)
	if !ok {
		return ExecutionResult{}, ErrInvalidRecallifyMergeInput
	}
	normalized, err := NormalizeRecallifyRunInput(workflowInput)
	if err != nil {
		return ExecutionResult{}, err
	}

	output := map[string]any{
		"title": normalized.Title,
		"level": normalized.Level,
		"mcqs":  mcqs,
		"counts": map[string]any{
			"mcqs": len(mcqs),
		},
	}
	if normalized.ExternalRequestID != "" {
		output["external_request_id"] = normalized.ExternalRequestID
	}
	return ExecutionResult{Output: output}, nil
}

func recallifyMCQMaps(value any) ([]map[string]any, bool) {
	if mcqs, ok := value.([]map[string]any); ok {
		return mcqs, true
	}

	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	mcqs := make([]map[string]any, 0, len(items))
	for _, item := range items {
		mcq, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		mcqs = append(mcqs, mcq)
	}
	return mcqs, true
}

func ValidateRecallifyMCQs(rawJSON string, requestedCount int) ([]map[string]any, error) {
	if requestedCount <= 0 {
		return nil, ErrInvalidRecallifyMCQs
	}

	var items []recallifyMCQ
	if err := json.Unmarshal([]byte(rawJSON), &items); err != nil {
		return nil, ErrInvalidRecallifyMCQs
	}
	if len(items) != requestedCount {
		return nil, ErrInvalidRecallifyMCQs
	}

	seenQuestions := map[string]struct{}{}
	output := make([]map[string]any, 0, len(items))
	for _, item := range items {
		normalized, err := normalizeRecallifyMCQ(item)
		if err != nil {
			return nil, err
		}

		questionKey := strings.ToLower(normalized["question"].(string))
		if _, exists := seenQuestions[questionKey]; exists {
			return nil, ErrInvalidRecallifyMCQs
		}
		seenQuestions[questionKey] = struct{}{}
		output = append(output, normalized)
	}

	return output, nil
}

func normalizeRecallifyMCQ(item recallifyMCQ) (map[string]any, error) {
	question := strings.Join(strings.Fields(item.Question), " ")
	options := []string{
		strings.Join(strings.Fields(item.Option1), " "),
		strings.Join(strings.Fields(item.Option2), " "),
		strings.Join(strings.Fields(item.Option3), " "),
		strings.Join(strings.Fields(item.Option4), " "),
	}
	explanations := []string{
		strings.Join(strings.Fields(item.Explanation1), " "),
		strings.Join(strings.Fields(item.Explanation2), " "),
		strings.Join(strings.Fields(item.Explanation3), " "),
		strings.Join(strings.Fields(item.Explanation4), " "),
	}
	if question == "" || item.Answer < 1 || item.Answer > 4 {
		return nil, ErrInvalidRecallifyMCQs
	}

	seenOptions := map[string]struct{}{}
	for i := range options {
		if options[i] == "" || explanations[i] == "" {
			return nil, ErrInvalidRecallifyMCQs
		}
		key := strings.ToLower(options[i])
		if _, exists := seenOptions[key]; exists {
			return nil, ErrInvalidRecallifyMCQs
		}
		seenOptions[key] = struct{}{}
	}

	return map[string]any{
		"question":     question,
		"option1":      options[0],
		"explanation1": explanations[0],
		"option2":      options[1],
		"explanation2": explanations[1],
		"option3":      options[2],
		"explanation3": explanations[2],
		"option4":      options[3],
		"explanation4": explanations[3],
		"answer":       item.Answer,
	}, nil
}
