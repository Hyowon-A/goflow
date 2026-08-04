package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestParseRetryPolicyDefaultsToOneAttempt(t *testing.T) {
	policy, err := ParseRetryPolicy(map[string]any{})
	if err != nil {
		t.Fatalf("parse retry policy: %v", err)
	}

	if policy.MaxAttempts != 1 {
		t.Fatalf("expected one attempt, got %d", policy.MaxAttempts)
	}
}

func TestParseRetryPolicyAllowsThreeAttempts(t *testing.T) {
	policy, err := ParseRetryPolicy(map[string]any{
		"retry": map[string]any{
			"max_attempts":  3,
			"initial_delay": "2s",
			"multiplier":    2,
		},
	})
	if err != nil {
		t.Fatalf("parse retry policy: %v", err)
	}

	if policy.MaxAttempts != 3 || policy.InitialDelay != 2*time.Second || policy.Multiplier != 2 {
		t.Fatalf("unexpected policy: %#v", policy)
	}
}

func TestParseRetryPolicyRejectsInvalidDuration(t *testing.T) {
	_, err := ParseRetryPolicy(map[string]any{
		"retry": map[string]any{"initial_delay": "later"},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestParseRetryPolicyRejectsMultiplierBelowOne(t *testing.T) {
	_, err := ParseRetryPolicy(map[string]any{
		"retry": map[string]any{"multiplier": 0.5},
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestDecideRetryRetriesWhenAttemptsRemain(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	decision := DecideRetry(now, 1, RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Multiplier:   2,
	}, true, "temporary failure")

	if !decision.Retry {
		t.Fatal("expected retry")
	}
	if decision.NextRetryAt != now.Add(time.Second) {
		t.Fatalf("expected next retry at %s, got %s", now.Add(time.Second), decision.NextRetryAt)
	}
	if decision.FailureReason != "temporary failure" {
		t.Fatalf("expected failure reason to be preserved, got %q", decision.FailureReason)
	}
}

func TestDecideRetryStopsAtMaxAttempts(t *testing.T) {
	decision := DecideRetry(time.Now(), 3, RetryPolicy{MaxAttempts: 3, Multiplier: 1}, true, "failed")

	if decision.Retry {
		t.Fatal("expected permanent failure")
	}
	if !decision.NextRetryAt.IsZero() {
		t.Fatalf("expected no next retry time, got %s", decision.NextRetryAt)
	}
}

func TestDecideRetryStopsForNonRetryableFailure(t *testing.T) {
	decision := DecideRetry(time.Now(), 1, RetryPolicy{MaxAttempts: 3, Multiplier: 1}, false, "failed")

	if decision.Retry {
		t.Fatal("expected permanent failure")
	}
}

func TestDecideRetryDelayUsesAttemptNumber(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	decision := DecideRetry(now, 2, RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Second,
		Multiplier:   3,
	}, true, "failed")

	if decision.NextRetryAt != now.Add(3*time.Second) {
		t.Fatalf("expected next retry at %s, got %s", now.Add(3*time.Second), decision.NextRetryAt)
	}
}
