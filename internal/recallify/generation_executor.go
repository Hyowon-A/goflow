package recallify

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

const defaultRecallifyGenerateMCQsTimeout = 30 * time.Second

var (
	ErrInvalidRecallifyGenerateMCQsConfig = errors.New("invalid_recallify_generate_mcqs_config")
	ErrInvalidRecallifyGenerateMCQsInput  = errors.New("invalid_recallify_generate_mcqs_input")
)

type RecallifyGenerateMCQsExecutor struct {
	Client RecallifyClient
}

func NewRecallifyGenerateMCQsExecutor(client RecallifyClient) RecallifyGenerateMCQsExecutor {
	return RecallifyGenerateMCQsExecutor{Client: client}
}

func (e RecallifyGenerateMCQsExecutor) Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	client, err := e.recallifyClient(input.Config)
	if err != nil {
		return ExecutionResult{}, err
	}

	source := recallifyNamedPredecessorInput(input.TaskRunInput, "clean_text")
	cleanText, ok := source["clean_text"].(string)
	if !ok || strings.TrimSpace(cleanText) == "" {
		return ExecutionResult{}, ErrInvalidRecallifyGenerateMCQsInput
	}

	count := DefaultRecallifyCount
	if err := optionalCount(source, "mcq_count", &count); err != nil || count <= 0 {
		return ExecutionResult{}, ErrInvalidRecallifyGenerateMCQsInput
	}

	level := DefaultRecallifyLevel
	if value, ok := source["level"]; ok && value != nil {
		text, ok := value.(string)
		if !ok {
			return ExecutionResult{}, ErrInvalidRecallifyGenerateMCQsInput
		}
		level = strings.TrimSpace(text)
	}
	if level != "easy" && level != "medium" && level != "hard" {
		return ExecutionResult{}, ErrInvalidRecallifyGenerateMCQsInput
	}

	rawJSON, err := client.GenerateMCQs(ctx, strings.TrimSpace(cleanText), count, level)
	if err != nil {
		return ExecutionResult{FailureReason: err.Error(), Retryable: IsRetryableRecallifyError(err)}, nil
	}

	return ExecutionResult{Output: map[string]any{
		"kind":            "mcq",
		"requested_count": count,
		"raw_json":        rawJSON,
	}}, nil
}

func (e RecallifyGenerateMCQsExecutor) recallifyClient(config map[string]any) (RecallifyClient, error) {
	client := e.Client

	baseURL := strings.TrimSpace(client.BaseURL)
	if value, ok := config["base_url"]; ok && value != nil {
		text, ok := value.(string)
		if !ok {
			return RecallifyClient{}, ErrInvalidRecallifyGenerateMCQsConfig
		}
		baseURL = strings.TrimSpace(text)
	}
	if baseURL == "" {
		return RecallifyClient{}, ErrInvalidRecallifyGenerateMCQsConfig
	}
	client.BaseURL = baseURL

	if value, ok := config["bearer_token"]; ok && value != nil {
		text, ok := value.(string)
		if !ok {
			return RecallifyClient{}, ErrInvalidRecallifyGenerateMCQsConfig
		}
		client.BearerToken = strings.TrimSpace(text)
	}

	timeout := defaultRecallifyGenerateMCQsTimeout
	if value, ok := config["timeout"]; ok && value != nil {
		text, ok := value.(string)
		if !ok {
			return RecallifyClient{}, ErrInvalidRecallifyGenerateMCQsConfig
		}
		parsed, err := time.ParseDuration(text)
		if err != nil || parsed <= 0 {
			return RecallifyClient{}, ErrInvalidRecallifyGenerateMCQsConfig
		}
		timeout = parsed
	}
	if client.HTTPClient == nil {
		client.HTTPClient = &http.Client{Timeout: timeout}
	}

	return client, nil
}
