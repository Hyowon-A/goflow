package metrics

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerReturnsPrometheusText(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	NewRegistry().Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Fatalf("expected text/plain content type, got %q", got)
	}
	if !strings.Contains(rec.Body.String(), "# TYPE goflow_workflow_runs_started_total counter") {
		t.Fatalf("expected Prometheus type line, got %s", rec.Body.String())
	}
}

func TestRenderIncludesCounterAndGaugeValues(t *testing.T) {
	registry := NewRegistry()
	registry.Add("goflow_workflow_runs_started_total", 2)
	registry.Inc("goflow_workflow_runs_started_total")
	registry.Gauge("goflow_outbox_pending", func(context.Context) (int64, error) {
		return 7, nil
	})

	var out bytes.Buffer
	if err := registry.Render(context.Background(), &out); err != nil {
		t.Fatalf("render metrics: %v", err)
	}

	output := out.String()
	for _, want := range []string{
		"# HELP goflow_workflow_runs_started_total Workflow runs created.",
		"# TYPE goflow_workflow_runs_started_total counter",
		"goflow_workflow_runs_started_total 3",
		"# TYPE goflow_outbox_pending gauge",
		"goflow_outbox_pending 7",
		"goflow_task_runs_running 0",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got\n%s", want, output)
		}
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	registry := NewRegistry()
	registry.Inc("goflow_task_runs_completed_total")
	registry.Gauge("goflow_task_runs_running", func(context.Context) (int64, error) {
		return 4, nil
	})

	var first bytes.Buffer
	if err := registry.Render(context.Background(), &first); err != nil {
		t.Fatalf("first render metrics: %v", err)
	}
	var second bytes.Buffer
	if err := registry.Render(context.Background(), &second); err != nil {
		t.Fatalf("second render metrics: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic render, got\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}
}

func TestHandlerReturnsErrorWhenGaugeProviderFails(t *testing.T) {
	registry := NewRegistry()
	registry.Gauge("goflow_outbox_pending", func(context.Context) (int64, error) {
		return 0, errors.New("database unavailable")
	})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	registry.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "database unavailable") {
		t.Fatalf("expected gauge error in response, got %s", rec.Body.String())
	}
}
