package recallify

import (
	"context"
	"errors"
	"strings"
	"unicode"
)

const (
	DefaultRecallifyMaxTextBytes = 100_000

	recallifyTextTooLargeFailure = "recallify_document_text_too_large"
)

var ErrInvalidRecallifyMaxTextBytes = errors.New("invalid_recallify_max_text_bytes")

type RecallifyValidateRequestExecutor struct{}

func (RecallifyValidateRequestExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	normalized, err := NormalizeRecallifyRunInput(input.TaskRunInput)
	if err != nil {
		return ExecutionResult{}, err
	}

	return ExecutionResult{Output: normalizedRecallifyInputMap(normalized)}, nil
}

type RecallifyCleanTextExecutor struct{}

func (RecallifyCleanTextExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	maxBytes := DefaultRecallifyMaxTextBytes
	if err := optionalCount(input.Config, "max_text_bytes", &maxBytes); err != nil || maxBytes <= 0 {
		return ExecutionResult{}, ErrInvalidRecallifyMaxTextBytes
	}

	normalized, err := NormalizeRecallifyRunInput(recallifySourceInput(input.TaskRunInput))
	if err != nil {
		return ExecutionResult{}, err
	}

	cleanText := cleanRecallifyText(normalized.DocumentText)
	if len([]byte(cleanText)) > maxBytes {
		return ExecutionResult{FailureReason: recallifyTextTooLargeFailure}, nil
	}

	output := normalizedRecallifyInputMap(normalized)
	delete(output, "document_text")
	output["clean_text"] = cleanText
	return ExecutionResult{Output: output}, nil
}

func cleanRecallifyText(text string) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && !unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, text)
	return strings.Join(strings.Fields(text), " ")
}

func recallifySourceInput(input map[string]any) map[string]any {
	return recallifyNamedPredecessorInput(input, "validate_request")
}

func recallifyNamedPredecessorInput(input map[string]any, name string) map[string]any {
	predecessors, ok := input["predecessors"].(map[string]any)
	if !ok {
		return input
	}
	predecessor, ok := predecessors[name].(map[string]any)
	if !ok {
		return input
	}
	return predecessor
}

func normalizedRecallifyInputMap(input RecallifyRunInput) map[string]any {
	output := map[string]any{
		"document_text": input.DocumentText,
		"title":         input.Title,
		"level":         input.Level,
		"mcq_count":     input.MCQCount,
	}
	if input.CallbackURL != "" {
		output["callback_url"] = input.CallbackURL
	}
	if input.ExternalRequestID != "" {
		output["external_request_id"] = input.ExternalRequestID
	}
	return output
}
