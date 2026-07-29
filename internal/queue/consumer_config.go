package queue

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

var defaultConsumerNameCounter uint64

type ConsumerConfig struct {
	Addr         string
	StreamName   string
	GroupName    string
	ConsumerName string
	BlockTimeout time.Duration
	Count        uint
}

func DefaultConsumerConfig() ConsumerConfig {
	cfg := DefaultConfig()

	return ConsumerConfig{
		Addr:         cfg.Addr,
		StreamName:   cfg.StreamName,
		GroupName:    "goflow-workers",
		ConsumerName: fmt.Sprintf("worker-%d", atomic.AddUint64(&defaultConsumerNameCounter, 1)),
		BlockTimeout: time.Second,
		Count:        1,
	}
}

func (c ConsumerConfig) Validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(c.StreamName) == "" {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(c.GroupName) == "" {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(c.ConsumerName) == "" {
		return ErrInvalidConfig
	}
	if c.BlockTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.Count == 0 {
		return ErrInvalidConfig
	}

	return nil
}
