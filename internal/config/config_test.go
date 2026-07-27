package config

import (
	"strings"
	"testing"
)

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("HTTP_PORT", "")
	t.Setenv("DATABASE_URL", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected an error when DATABASE_URL is missing")
	}

	if !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("expected DATABASE_URL error, got %q", err.Error())
	}
}

func TestLoadUsesDefaults(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("HTTP_PORT", "")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/goflow?sslmode=disable")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.AppEnv != "development" {
		t.Fatalf("expected default AppEnv development, got %q", cfg.AppEnv)
	}

	if cfg.HTTPPort != "8080" {
		t.Fatalf("expected default HTTPPort 8080, got %q", cfg.HTTPPort)
	}

	if cfg.RedisAddr != "localhost:6379" {
		t.Fatalf("expected default RedisAddr localhost:6379, got %q", cfg.RedisAddr)
	}

	if cfg.QueueStreamName != "goflow:tasks" {
		t.Fatalf("expected default QueueStreamName goflow:tasks, got %q", cfg.QueueStreamName)
	}

	if cfg.Address() != ":8080" {
		t.Fatalf("expected address :8080, got %q", cfg.Address())
	}
}

func TestLoadUsesEnvironmentValues(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/goflow?sslmode=disable")
	t.Setenv("REDIS_ADDR", "redis.example.test:6379")
	t.Setenv("QUEUE_STREAM_NAME", "goflow:test-tasks")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.AppEnv != "test" {
		t.Fatalf("expected AppEnv test, got %q", cfg.AppEnv)
	}

	if cfg.HTTPPort != "9090" {
		t.Fatalf("expected HTTPPort 9090, got %q", cfg.HTTPPort)
	}

	if cfg.DatabaseURL != "postgres://user:pass@localhost:5432/goflow?sslmode=disable" {
		t.Fatalf("unexpected DatabaseURL %q", cfg.DatabaseURL)
	}

	if cfg.RedisAddr != "redis.example.test:6379" {
		t.Fatalf("expected RedisAddr redis.example.test:6379, got %q", cfg.RedisAddr)
	}

	if cfg.QueueStreamName != "goflow:test-tasks" {
		t.Fatalf("expected QueueStreamName goflow:test-tasks, got %q", cfg.QueueStreamName)
	}

	if cfg.Address() != ":9090" {
		t.Fatalf("expected address :9090, got %q", cfg.Address())
	}
}
