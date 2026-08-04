package workflow

import (
	"fmt"
	"math"
	"time"
)

type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	Multiplier   float64
}

type RetryDecision struct {
	Retry         bool
	NextRetryAt   time.Time
	FailureReason string
}

func ParseRetryPolicy(config map[string]any) (RetryPolicy, error) {
	policy := RetryPolicy{MaxAttempts: 1, Multiplier: 1}

	value, ok := config["retry"]
	if !ok || value == nil {
		return policy, nil
	}

	retry, ok := value.(map[string]any)
	if !ok {
		return RetryPolicy{}, fmt.Errorf("%w: retry must be an object", ErrValidation)
	}

	if value, ok := retry["max_attempts"]; ok {
		maxAttempts, ok := number(value)
		if !ok || math.Trunc(maxAttempts) != maxAttempts || maxAttempts < 1 {
			return RetryPolicy{}, fmt.Errorf("%w: retry.max_attempts must be a positive integer", ErrValidation)
		}
		policy.MaxAttempts = int(maxAttempts)
	}

	if value, ok := retry["initial_delay"]; ok {
		delay, ok := value.(string)
		if !ok {
			return RetryPolicy{}, fmt.Errorf("%w: retry.initial_delay must be a duration string", ErrValidation)
		}
		parsed, err := time.ParseDuration(delay)
		if err != nil {
			return RetryPolicy{}, fmt.Errorf("%w: retry.initial_delay must be a duration string", ErrValidation)
		}
		policy.InitialDelay = parsed
	}

	if value, ok := retry["multiplier"]; ok {
		multiplier, ok := number(value)
		if !ok || multiplier < 1 {
			return RetryPolicy{}, fmt.Errorf("%w: retry.multiplier must be at least 1", ErrValidation)
		}
		policy.Multiplier = multiplier
	}

	return policy, nil
}

func number(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

func DecideRetry(now time.Time, attemptNumber uint, policy RetryPolicy, retryable bool, failureReason string) RetryDecision {
	decision := RetryDecision{FailureReason: failureReason}
	if !retryable || attemptNumber >= uint(policy.MaxAttempts) {
		return decision
	}

	delay := time.Duration(float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(attemptNumber-1)))
	decision.Retry = true
	decision.NextRetryAt = now.Add(delay)
	return decision
}
