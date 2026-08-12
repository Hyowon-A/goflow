package worker

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
)

var ErrInvalidFailureProbability = errors.New("invalid_failure_probability")
var ErrInvalidRandomDraw = errors.New("invalid_random_draw")

const randomFailFailureReason = "random failure"

type RandomFailExecutor struct {
	rand func() float64
}

func NewRandomFailExecutor(rand func() float64) RandomFailExecutor {
	return RandomFailExecutor{rand: rand}
}

func (e RandomFailExecutor) Execute(
	ctx context.Context,
	input ExecutionInput,
) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}

	var failureProbability float64

	value, ok := input.Config["failure_probability"]
	if !ok {
		value, ok = input.TaskRunInput["failure_probability"]
	}
	if !ok {
		return ExecutionResult{}, ErrInvalidFailureProbability
	}

	failureProbability, ok = value.(float64)
	if !ok || math.IsNaN(failureProbability) || failureProbability < 0 || failureProbability > 1 {
		return ExecutionResult{}, ErrInvalidFailureProbability
	}

	switch failureProbability {
	case 0:
		return ExecutionResult{Output: map[string]any{"status": "completed", "success": true}}, nil
	case 1:
		return ExecutionResult{FailureReason: randomFailFailureReason, Retryable: true}, nil
	default:
		draw := rand.Float64()
		if e.rand != nil {
			draw = e.rand()
		}
		if math.IsNaN(draw) || draw < 0 || draw >= 1 {
			return ExecutionResult{}, ErrInvalidRandomDraw
		}

		if draw < failureProbability {
			return ExecutionResult{FailureReason: randomFailFailureReason, Retryable: true}, nil
		}

		return ExecutionResult{Output: map[string]any{"status": "completed", "success": true}}, nil

	}
}
