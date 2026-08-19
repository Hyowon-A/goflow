package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var defaultWorkerID = fmt.Sprintf("worker-%s", uuid.NewString())

type Config struct {
	AppEnv                  string
	HTTPPort                string
	DatabaseURL             string
	RedisAddr               string
	QueueStreamName         string
	WorkerID                string
	QueueConsumerGroup      string
	QueueBlockTimeout       time.Duration
	QueueReadCount          int
	WorkerLeaseDuration     time.Duration
	WorkerHeartbeatInterval time.Duration
	WorkerRecoveryInterval  time.Duration
	WorkerMetricsPort       string
}

func Load() (Config, error) {
	queueBlockTimeout, err := getDurationEnv("QUEUE_BLOCK_TIMEOUT", time.Second)
	if err != nil {
		return Config{}, err
	}

	queueReadCount, err := getPositiveIntEnv("QUEUE_READ_COUNT", 1)
	if err != nil {
		return Config{}, err
	}
	workerLeaseDuration, err := getDurationEnv("WORKER_LEASE_DURATION", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	workerHeartbeatInterval, err := getDurationEnv("WORKER_HEARTBEAT_INTERVAL", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	workerRecoveryInterval, err := getDurationEnv("WORKER_RECOVERY_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	if workerHeartbeatInterval >= workerLeaseDuration {
		return Config{}, errors.New("WORKER_HEARTBEAT_INTERVAL must be shorter than WORKER_LEASE_DURATION")
	}

	cfg := Config{
		AppEnv:                  getEnv("APP_ENV", "development"),
		HTTPPort:                getEnv("HTTP_PORT", "8080"),
		DatabaseURL:             os.Getenv("DATABASE_URL"), // returns an empty string when the variable is missing
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		QueueStreamName:         getEnv("QUEUE_STREAM_NAME", "goflow:tasks"),
		WorkerID:                getEnv("WORKER_ID", defaultWorkerID),
		QueueConsumerGroup:      getEnv("QUEUE_CONSUMER_GROUP", "goflow-workers"),
		QueueBlockTimeout:       queueBlockTimeout,
		QueueReadCount:          queueReadCount,
		WorkerLeaseDuration:     workerLeaseDuration,
		WorkerHeartbeatInterval: workerHeartbeatInterval,
		WorkerRecoveryInterval:  workerRecoveryInterval,
		WorkerMetricsPort:       getEnv("WORKER_METRICS_PORT", ""),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return Config{}, errors.New("REDIS_ADDR is required")
	}
	if strings.TrimSpace(cfg.QueueStreamName) == "" {
		return Config{}, errors.New("QUEUE_STREAM_NAME is required")
	}
	if strings.TrimSpace(cfg.WorkerID) == "" {
		return Config{}, errors.New("WORKER_ID is required")
	}
	if strings.TrimSpace(cfg.QueueConsumerGroup) == "" {
		return Config{}, errors.New("QUEUE_CONSUMER_GROUP is required")
	}
	if cfg.WorkerMetricsPort != "" {
		port, err := strconv.Atoi(cfg.WorkerMetricsPort)
		if err != nil || port <= 0 || port > 65535 {
			return Config{}, errors.New("WORKER_METRICS_PORT must be a valid port")
		}
	}

	return cfg, nil
}

func (c Config) Address() string {
	return fmt.Sprintf(":%s", c.HTTPPort)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}

	return duration, nil
}

func getPositiveIntEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}

	return parsed, nil
}
