package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type fakeEvidenceSource struct {
	run    runEvidence
	tasks  []taskEvidence
	outbox int64
	err    error
}

func (f fakeEvidenceSource) LoadRun(context.Context, string) (runEvidence, error) {
	return f.run, f.err
}

func (f fakeEvidenceSource) LoadFailedTasks(context.Context, string) ([]taskEvidence, error) {
	return f.tasks, f.err
}

func (f fakeEvidenceSource) CountPendingOutbox(context.Context, string) (int64, error) {
	return f.outbox, f.err
}

func TestReportIncludesFailedTaskReasonAndEvidenceBasedChecks(t *testing.T) {
	report, err := loadDatabaseEvidence(context.Background(), fakeEvidenceSource{
		run:    runEvidence{ID: "run-1", WorkflowID: "workflow-1", Status: "failed"},
		outbox: 2,
		tasks: []taskEvidence{{
			ID: "task-run-1", Name: "generate_mcqs", Status: "dead_letter", AttemptCount: 3, FailureReason: "HTTP 503",
		}},
	}, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	pending := int64(1)
	report.RedisPending = &pending

	var out bytes.Buffer
	if err := renderReport(&out, report); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"generate_mcqs (dead_letter, attempts=3", "HTTP 503", "2 pending outbox event(s)", "1 pending message(s)"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("report missing %q:\n%s", want, out.String())
		}
	}
}

func TestCompletedRunProducesNoFakeIncident(t *testing.T) {
	report, err := loadDatabaseEvidence(context.Background(), fakeEvidenceSource{
		run: runEvidence{ID: "run-1", WorkflowID: "workflow-1", Status: "completed"},
	}, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	report.RedisPending = &zero

	var out bytes.Buffer
	if err := renderReport(&out, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "failed/dead-letter tasks: none") || !strings.Contains(out.String(), "suggested next checks: none") {
		t.Fatalf("unexpected completed report:\n%s", out.String())
	}
}

func TestMissingRunReturnsClearError(t *testing.T) {
	_, err := loadDatabaseEvidence(context.Background(), fakeEvidenceSource{err: errRunNotFound}, "missing")
	if !errors.Is(err, errRunNotFound) {
		t.Fatalf("expected errRunNotFound, got %v", err)
	}
}

func TestFetchMetricsReturnsGoFlowSamplesOnly(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("# HELP ignored\ngoflow_outbox_pending 2\nprocess_cpu_seconds 4\n")),
		}, nil
	})}
	metrics, err := fetchMetrics(context.Background(), client, "http://metrics.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0] != "goflow_outbox_pending 2" {
		t.Fatalf("unexpected metrics: %#v", metrics)
	}
}

func TestFetchMetricsRejectsFailureStatus(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       io.NopCloser(strings.NewReader("unavailable")),
		}, nil
	})}
	if _, err := fetchMetrics(context.Background(), client, "http://metrics.test"); err == nil {
		t.Fatal("expected metrics error")
	}
}
