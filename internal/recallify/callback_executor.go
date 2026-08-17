package recallify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

var ErrInvalidRecallifyCallbackInput = errors.New("invalid_recallify_callback_input")

type RecallifyNotifyCallbackExecutor struct {
	HTTPClient *http.Client
}

func (e RecallifyNotifyCallbackExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	predecessors, ok := input.TaskRunInput["predecessors"].(map[string]any)
	if !ok {
		return ExecutionResult{}, ErrInvalidRecallifyCallbackInput
	}
	merged, ok := predecessors["merge_study_set"].(map[string]any)
	if !ok || len(merged) == 0 {
		return ExecutionResult{}, ErrInvalidRecallifyCallbackInput
	}
	workflowInput, ok := input.TaskRunInput["workflow_input"].(map[string]any)
	if !ok {
		return ExecutionResult{}, ErrInvalidRecallifyCallbackInput
	}
	normalized, err := NormalizeRecallifyRunInput(workflowInput)
	if err != nil {
		return ExecutionResult{}, err
	}
	if normalized.CallbackURL == "" {
		return ExecutionResult{Output: map[string]any{"skipped": true}}, nil
	}

	body, err := json.Marshal(map[string]any{
		"status":              "completed",
		"external_request_id": normalized.ExternalRequestID,
		"summary": map[string]any{
			"title":     merged["title"],
			"level":     merged["level"],
			"mcq_count": recallifyCallbackMCQCount(merged),
		},
	})
	if err != nil {
		return ExecutionResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, normalized.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return ExecutionResult{}, ErrInvalidRecallifyCallbackInput
	}
	req.Header.Set("Content-Type", "application/json")

	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return ExecutionResult{FailureReason: err.Error(), Retryable: retryableRecallifyCallbackError(err)}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return ExecutionResult{
			FailureReason: fmt.Sprintf("recallify callback failed with status %d", resp.StatusCode),
			Retryable:     retryableRecallifyCallbackStatus(resp.StatusCode),
		}, nil
	}

	return ExecutionResult{Output: map[string]any{
		"posted":              true,
		"external_request_id": normalized.ExternalRequestID,
	}}, nil
}

func recallifyCallbackMCQCount(merged map[string]any) int {
	if counts, ok := merged["counts"].(map[string]any); ok {
		var count int
		if optionalCount(counts, "mcqs", &count) == nil {
			return count
		}
	}
	if mcqs, ok := recallifyMCQMaps(merged["mcqs"]); ok {
		return len(mcqs)
	}
	return 0
}

func retryableRecallifyCallbackError(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout()
}

func retryableRecallifyCallbackStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
