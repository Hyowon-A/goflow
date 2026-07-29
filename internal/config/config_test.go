package config

import (
	"strings"
	"testing"
	"time"
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

	if cfg.WorkerID == "" {
		t.Fatal("expected default WorkerID to be generated")
	}

	if cfg.QueueConsumerGroup != "goflow-workers" {
		t.Fatalf("expected default QueueConsumerGroup goflow-workers, got %q", cfg.QueueConsumerGroup)
	}

	if cfg.QueueBlockTimeout != time.Second {
		t.Fatalf("expected default QueueBlockTimeout 1s, got %s", cfg.QueueBlockTimeout)
	}

	if cfg.QueueReadCount != 1 {
		t.Fatalf("expected default QueueReadCount 1, got %d", cfg.QueueReadCount)
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
	t.Setenv("WORKER_ID", "worker-test-1")
	t.Setenv("QUEUE_CONSUMER_GROUP", "goflow-test-workers")
	t.Setenv("QUEUE_BLOCK_TIMEOUT", "250ms")
	t.Setenv("QUEUE_READ_COUNT", "3")

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

	if cfg.WorkerID != "worker-test-1" {
		t.Fatalf("expected WorkerID worker-test-1, got %q", cfg.WorkerID)
	}

	if cfg.QueueConsumerGroup != "goflow-test-workers" {
		t.Fatalf("expected QueueConsumerGroup goflow-test-workers, got %q", cfg.QueueConsumerGroup)
	}

	if cfg.QueueBlockTimeout != 250*time.Millisecond {
		t.Fatalf("expected QueueBlockTimeout 250ms, got %s", cfg.QueueBlockTimeout)
	}

	if cfg.QueueReadCount != 3 {
		t.Fatalf("expected QueueReadCount 3, got %d", cfg.QueueReadCount)
	}

	if cfg.Address() != ":9090" {
		t.Fatalf("expected address :9090, got %q", cfg.Address())
	}
}

func TestLoadRejectsInvalidWorkerQueueSettings(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{
			name: "invalid block timeout",
			key:  "QUEUE_BLOCK_TIMEOUT",
			val:  "not-a-duration",
		},
		{
			name: "zero read count",
			key:  "QUEUE_READ_COUNT",
			val:  "0",
		},
		{
			name: "invalid read count",
			key:  "QUEUE_READ_COUNT",
			val:  "not-a-number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/goflow?sslmode=disable")
			t.Setenv(tt.key, tt.val)

			_, err := Load()
			if err == nil {
				t.Fatal("expected invalid worker queue setting to return an error")
			}
		})
	}
}

func TestLoadRejectsBlankWorkerQueueSettings(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{
			name: "blank Redis address",
			key:  "REDIS_ADDR",
		},
		{
			name: "blank stream name",
			key:  "QUEUE_STREAM_NAME",
		},
		{
			name: "blank worker id",
			key:  "WORKER_ID",
		},
		{
			name: "blank consumer group",
			key:  "QUEUE_CONSUMER_GROUP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/goflow?sslmode=disable")
			t.Setenv(tt.key, " ")

			_, err := Load()
			if err == nil {
				t.Fatal("expected blank worker queue setting to return an error")
			}
		})
	}
}

func TestLoadUsesStableGeneratedWorkerID(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/goflow?sslmode=disable")

	first, err := Load()
	if err != nil {
		t.Fatalf("first Load returned error: %v", err)
	}

	second, err := Load()
	if err != nil {
		t.Fatalf("second Load returned error: %v", err)
	}

	if first.WorkerID == "" {
		t.Fatal("expected generated WorkerID")
	}
	if first.WorkerID != second.WorkerID {
		t.Fatalf("expected process-local WorkerID to be stable, got %q and %q", first.WorkerID, second.WorkerID)
	}
}
