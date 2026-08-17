package recallify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecallifyClientGenerateMCQsReturnsRawJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ai/generateMcqs" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		want := map[string]string{"text": "cleaned text", "count": "10", "level": "medium"}
		if req["text"] != want["text"] || req["count"] != want["count"] || req["level"] != want["level"] {
			t.Fatalf("unexpected request body: %#v", req)
		}
		_, _ = w.Write([]byte(`{"mcqs":"[{\"question\":\"q\"}]"}`))
	}))
	defer server.Close()

	raw, err := (RecallifyClient{BaseURL: server.URL}).GenerateMCQs(context.Background(), "cleaned text", 10, "medium")
	if err != nil {
		t.Fatalf("generate mcqs: %v", err)
	}
	if raw != `[{"question":"q"}]` {
		t.Fatalf("unexpected raw JSON: %s", raw)
	}
}

func TestRecallifyClientGenerateMCQsClassifiesRetryableStatuses(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()

			_, err := (RecallifyClient{BaseURL: server.URL}).GenerateMCQs(context.Background(), "text", 1, "easy")
			if !IsRetryableRecallifyError(err) {
				t.Fatalf("expected retryable error, got %v", err)
			}
		})
	}
}

func TestRecallifyClientGenerateMCQsClassifiesPermanentStatuses(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()

			_, err := (RecallifyClient{BaseURL: server.URL}).GenerateMCQs(context.Background(), "text", 1, "easy")
			if err == nil || IsRetryableRecallifyError(err) {
				t.Fatalf("expected non-retryable error, got %v", err)
			}
		})
	}
}

func TestRecallifyClientGenerateMCQsRejectsInvalidResponseShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	_, err := (RecallifyClient{BaseURL: server.URL}).GenerateMCQs(context.Background(), "text", 1, "easy")
	if !errors.Is(err, ErrInvalidRecallifyResponse) || IsRetryableRecallifyError(err) {
		t.Fatalf("expected non-retryable invalid response, got %v", err)
	}
}

func TestRecallifyClientGenerateMCQsTimeoutIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := (RecallifyClient{BaseURL: server.URL}).GenerateMCQs(ctx, "text", 1, "easy")
	if !IsRetryableRecallifyError(err) {
		t.Fatalf("expected retryable timeout, got %v", err)
	}
}

func TestRecallifyClientGenerateMCQsHTTPClientTimeoutIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
	}))
	defer server.Close()

	_, err := (RecallifyClient{
		BaseURL:    server.URL,
		HTTPClient: &http.Client{Timeout: time.Millisecond},
	}).GenerateMCQs(context.Background(), "text", 1, "easy")
	if !IsRetryableRecallifyError(err) {
		t.Fatalf("expected retryable client timeout, got %v", err)
	}
}

func TestRecallifyClientErrorDoesNotExposeBearerToken(t *testing.T) {
	const token = "secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatalf("missing bearer token")
		}
		http.Error(w, r.Header.Get("Authorization"), http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := (RecallifyClient{BaseURL: server.URL, BearerToken: token}).GenerateMCQs(context.Background(), "text", 1, "easy")
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("token leaked in error: %v", err)
	}
}
