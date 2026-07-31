package worker

import (
	"context"
	"errors"
	"time"
)

var ErrInvalidSleepDuration = errors.New("invalid_sleep_duration")

type SleepExecutor struct{}

func (SleepExecutor) Execute(
	ctx context.Context,
	input ExecutionInput,
) (ExecutionResult, error) {
	durationValue, ok := input.Config["duration"].(string)
	if !ok {
		return ExecutionResult{}, ErrInvalidSleepDuration
	}

	duration, err := time.ParseDuration(durationValue)
	if err != nil || duration < 0 {
		return ExecutionResult{}, ErrInvalidSleepDuration
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return ExecutionResult{Output: map[string]any{"status": "completed"}}, nil
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	}
}
