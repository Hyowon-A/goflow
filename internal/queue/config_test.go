package queue

import (
	"errors"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Addr != "localhost:6379" {
		t.Fatalf("expected default Redis address localhost:6379, got %q", cfg.Addr)
	}
	if cfg.StreamName != "goflow:tasks" {
		t.Fatalf("expected default stream goflow:tasks, got %q", cfg.StreamName)
	}
}

func TestConfigValidateRejectsMissingValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "missing address",
			cfg:  Config{StreamName: "goflow:tasks"},
		},
		{
			name: "missing stream name",
			cfg:  Config{Addr: "localhost:6379"},
		},
		{
			name: "blank values",
			cfg:  Config{Addr: " ", StreamName: " "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}
}

func TestConfigValidateAcceptsCompleteConfig(t *testing.T) {
	cfg := Config{
		Addr:       "localhost:6379",
		StreamName: "goflow:tasks",
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
}
