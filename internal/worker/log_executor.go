package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
)

var ErrMissingMessage = errors.New("missing_message")

type LogExecutor struct {
	logger *log.Logger
}

func NewLogExecutor(logger *log.Logger) LogExecutor {
	return LogExecutor{logger: logger}
}

func (e LogExecutor) Execute(
	ctx context.Context,
	input ExecutionInput,
) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	message, _ := input.Config["message"].(string)
	if strings.TrimSpace(message) == "" {
		message, _ = input.TaskRunInput["message"].(string)
	}
	if strings.TrimSpace(message) == "" {
		return ExecutionResult{}, ErrMissingMessage
	}

	output := map[string]any{"status": "completed", "message": message}
	encoded, err := json.Marshal(output)
	if err != nil {
		return ExecutionResult{}, err
	}

	logger := e.logger
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("%s", encoded)

	return ExecutionResult{Output: output}, nil
}
