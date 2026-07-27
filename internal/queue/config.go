package queue

import "strings"

type Config struct {
	Addr       string
	StreamName string
}

func DefaultConfig() Config {
	return Config{
		Addr:       "localhost:6379",
		StreamName: "goflow:tasks",
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return ErrInvalidConfig
	}
	if strings.TrimSpace(c.StreamName) == "" {
		return ErrInvalidConfig
	}

	return nil
}
