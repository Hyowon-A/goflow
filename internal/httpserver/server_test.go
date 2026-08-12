package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Hyowon-A/goflow/internal/metrics"
)

type fakePinger struct {
	err error
}

func (f fakePinger) Ping(context.Context) error {
	return f.err
}

func TestHealthReturnsOK(t *testing.T) {
	server := newServer(fakePinger{})

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("expected application/json content type, got %q", contentType)
	}

	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %#v", body["status"])
	}

	if body["service"] != "goflow-api" {
		t.Fatalf("expected service goflow-api, got %#v", body["service"])
	}

	if _, ok := body["timestamp"].(string); !ok {
		t.Fatalf("expected timestamp string, got %#v", body["timestamp"])
	}
}

func TestReadinessReturnsReadyWhenDatabasePingSucceeds(t *testing.T) {
	server := newServer(fakePinger{})

	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body["status"] != "ready" {
		t.Fatalf("expected status ready, got %q", body["status"])
	}
}

func TestReadinessReturnsUnavailableWhenDatabasePingFails(t *testing.T) {
	server := newServer(fakePinger{err: errors.New("database unavailable")})

	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, response.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body["status"] != "not_ready" {
		t.Fatalf("expected status not_ready, got %q", body["status"])
	}

	if body["reason"] != "database unavailable" {
		t.Fatalf("expected database unavailable reason, got %q", body["reason"])
	}
}

func TestMetricsReturnsPrometheusText(t *testing.T) {
	registry := metrics.NewRegistry()
	registry.Inc("goflow_workflow_runs_started_total")
	server := newServer(fakePinger{})
	server.metricsRegistry = registry

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}

	if contentType := response.Header().Get("Content-Type"); contentType != "text/plain; version=0.0.4" {
		t.Fatalf("expected Prometheus text content type, got %q", contentType)
	}

	if body := response.Body.String(); !strings.Contains(body, "goflow_workflow_runs_started_total 1\n") {
		t.Fatalf("expected incremented workflow-start metric, got:\n%s", body)
	}
}
