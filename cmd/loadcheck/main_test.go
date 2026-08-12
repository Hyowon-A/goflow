package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRenderSummary(t *testing.T) {
	var out bytes.Buffer
	summary := loadSummary{
		WorkflowRunsStarted:   3,
		WorkflowRunsCompleted: 2,
		WorkflowRunsFailed:    1,
		TaskAttempts:          9,
		Retries:               2,
		DeadLetters:           1,
		OutboxPending:         0,
		Elapsed:               1500 * time.Millisecond,
	}

	if err := renderSummary(&out, summary); err != nil {
		t.Fatalf("render summary: %v", err)
	}

	for _, want := range []string{
		"workflow runs started: 3",
		"workflow runs completed: 2",
		"workflow runs failed: 1",
		"task attempts: 9",
		"retries: 2",
		"dead letters: 1",
		"outbox pending: 0",
		"elapsed: 1.5s",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("expected summary to contain %q, got:\n%s", want, out.String())
		}
	}
}

func TestCheckInvariantsAcceptsTerminalRuns(t *testing.T) {
	err := checkInvariants(loadSummary{
		WorkflowRunsStarted:   2,
		WorkflowRunsCompleted: 1,
		WorkflowRunsFailed:    1,
		TaskAttempts:          4,
		OutboxPending:         0,
	}, 2)
	if err != nil {
		t.Fatalf("check invariants: %v", err)
	}
}

func TestCheckInvariantsRejectsFailures(t *testing.T) {
	tests := []struct {
		name    string
		summary loadSummary
		want    string
	}{
		{
			name: "wrong run count",
			summary: loadSummary{
				WorkflowRunsStarted:   1,
				WorkflowRunsCompleted: 1,
				TaskAttempts:          1,
			},
			want: "expected 2 workflow runs",
		},
		{
			name: "non terminal",
			summary: loadSummary{
				WorkflowRunsStarted:   2,
				WorkflowRunsCompleted: 1,
				TaskAttempts:          1,
			},
			want: "expected all workflow runs terminal",
		},
		{
			name: "no attempts",
			summary: loadSummary{
				WorkflowRunsStarted:   2,
				WorkflowRunsCompleted: 2,
			},
			want: "expected task attempts",
		},
		{
			name: "pending outbox",
			summary: loadSummary{
				WorkflowRunsStarted:   2,
				WorkflowRunsCompleted: 2,
				TaskAttempts:          4,
				OutboxPending:         1,
			},
			want: "expected no pending outbox events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkInvariants(tt.summary, 2)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestParseFlagsRejectsInvalidValues(t *testing.T) {
	for _, args := range [][]string{
		{"-runs", "0"},
		{"-workers", "0"},
		{"-timeout", "0s"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("expected parse error for args %#v", args)
		}
	}
}
