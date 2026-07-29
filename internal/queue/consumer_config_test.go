package queue

import (
	"errors"
	"testing"
	"time"
)

func TestDefaultConsumerConfig(t *testing.T) {
	cfg := DefaultConsumerConfig()

	if cfg.Addr != "localhost:6379" {
		t.Fatalf("expected default Redis address localhost:6379, got %q", cfg.Addr)
	}
	if cfg.StreamName != "goflow:tasks" {
		t.Fatalf("expected default stream goflow:tasks, got %q", cfg.StreamName)
	}
	if cfg.GroupName != "goflow-workers" {
		t.Fatalf("expected default group goflow-workers, got %q", cfg.GroupName)
	}
	if cfg.ConsumerName == "" {
		t.Fatal("expected generated consumer name")
	}
	if cfg.BlockTimeout <= 0 {
		t.Fatalf("expected positive block timeout, got %s", cfg.BlockTimeout)
	}
	if cfg.Count != 1 {
		t.Fatalf("expected default count 1, got %d", cfg.Count)
	}
}

func TestDefaultConsumerConfigGeneratesDistinctConsumerNames(t *testing.T) {
	first := DefaultConsumerConfig()
	second := DefaultConsumerConfig()

	if first.ConsumerName == "" || second.ConsumerName == "" {
		t.Fatalf("expected generated consumer names, got %q and %q", first.ConsumerName, second.ConsumerName)
	}
	if first.ConsumerName == second.ConsumerName {
		t.Fatalf("expected distinct generated consumer names, got %q", first.ConsumerName)
	}
}

func TestConsumerConfigValidateRejectsMissingValues(t *testing.T) {
	valid := ConsumerConfig{
		Addr:         "localhost:6379",
		StreamName:   "goflow:tasks",
		GroupName:    "goflow-workers",
		ConsumerName: "worker-1",
		BlockTimeout: time.Second,
		Count:        1,
	}

	tests := []struct {
		name string
		cfg  ConsumerConfig
	}{
		{
			name: "missing address",
			cfg: ConsumerConfig{
				StreamName:   valid.StreamName,
				GroupName:    valid.GroupName,
				ConsumerName: valid.ConsumerName,
				BlockTimeout: valid.BlockTimeout,
				Count:        valid.Count,
			},
		},
		{
			name: "missing stream name",
			cfg: ConsumerConfig{
				Addr:         valid.Addr,
				GroupName:    valid.GroupName,
				ConsumerName: valid.ConsumerName,
				BlockTimeout: valid.BlockTimeout,
				Count:        valid.Count,
			},
		},
		{
			name: "missing group name",
			cfg: ConsumerConfig{
				Addr:         valid.Addr,
				StreamName:   valid.StreamName,
				ConsumerName: valid.ConsumerName,
				BlockTimeout: valid.BlockTimeout,
				Count:        valid.Count,
			},
		},
		{
			name: "missing consumer name",
			cfg: ConsumerConfig{
				Addr:         valid.Addr,
				StreamName:   valid.StreamName,
				GroupName:    valid.GroupName,
				BlockTimeout: valid.BlockTimeout,
				Count:        valid.Count,
			},
		},
		{
			name: "zero block timeout",
			cfg: ConsumerConfig{
				Addr:         valid.Addr,
				StreamName:   valid.StreamName,
				GroupName:    valid.GroupName,
				ConsumerName: valid.ConsumerName,
				Count:        valid.Count,
			},
		},
		{
			name: "zero count",
			cfg: ConsumerConfig{
				Addr:         valid.Addr,
				StreamName:   valid.StreamName,
				GroupName:    valid.GroupName,
				ConsumerName: valid.ConsumerName,
				BlockTimeout: valid.BlockTimeout,
			},
		},
		{
			name: "blank values",
			cfg: ConsumerConfig{
				Addr:         " ",
				StreamName:   " ",
				GroupName:    " ",
				ConsumerName: " ",
				BlockTimeout: valid.BlockTimeout,
				Count:        valid.Count,
			},
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

func TestConsumerConfigValidateAcceptsCompleteConfig(t *testing.T) {
	cfg := ConsumerConfig{
		Addr:         "localhost:6379",
		StreamName:   "goflow:tasks",
		GroupName:    "goflow-workers",
		ConsumerName: "worker-1",
		BlockTimeout: time.Second,
		Count:        1,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate consumer config: %v", err)
	}
}
