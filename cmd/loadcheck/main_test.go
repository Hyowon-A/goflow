package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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

	if err := renderSummary(&out, summary, false); err != nil {
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

func TestRenderJSONSummary(t *testing.T) {
	var out bytes.Buffer
	summary := loadSummary{
		Tag:                   "baseline",
		RunsRequested:         5,
		WorkersRequested:      2,
		WorkflowRunsStarted:   5,
		WorkflowRunsCompleted: 5,
		TaskAttempts:          20,
		Retries:               1,
		OutboxPending:         0,
		P50WorkflowDuration:   1200 * time.Millisecond,
		P95WorkflowDuration:   1800 * time.Millisecond,
		Elapsed:               2 * time.Second,
	}

	if err := renderSummary(&out, summary, true); err != nil {
		t.Fatalf("render json summary: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("summary JSON is invalid: %v\n%s", err, out.String())
	}
	if got["tag"] != "baseline" || got["runs_requested"] != float64(5) || got["p95_workflow_duration"] != float64(1800*time.Millisecond) {
		t.Fatalf("unexpected JSON summary: %#v", got)
	}
}

func TestWriteSummaryOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	summary := loadSummary{
		Tag:                 "file",
		RunsRequested:       1,
		WorkersRequested:    1,
		P50WorkflowDuration: time.Second,
		P95WorkflowDuration: time.Second,
		Elapsed:             time.Second,
	}

	if err := writeSummaryOutput(path, summary); err != nil {
		t.Fatalf("write summary output: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read summary output: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("output JSON is invalid: %v\n%s", err, string(data))
	}
	if got["tag"] != "file" {
		t.Fatalf("unexpected output JSON: %#v", got)
	}
}

func TestWriteSummaryOutputRejectsMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "summary.json")
	err := writeSummaryOutput(path, loadSummary{})
	if err == nil || !strings.Contains(err.Error(), "write summary output") {
		t.Fatalf("expected clear write error, got %v", err)
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
		{"-failure-probability", "-0.1"},
		{"-failure-probability", "1.1"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("expected parse error for args %#v", args)
		}
	}
}

func TestParseFlagsAcceptsBenchmarkOutputFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-json", "-output", "out.json", "-tag", "baseline", "-failure-probability", "0"})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if !cfg.jsonOutput || cfg.output != "out.json" || cfg.tag != "baseline" || cfg.failureProbability != 0 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}
