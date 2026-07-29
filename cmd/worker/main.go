package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/worker"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("goflow worker failed: %v", err)
	}
}

func run() error {
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	consumerCfg := queue.ConsumerConfig{
		Addr:         cfg.RedisAddr,
		StreamName:   cfg.QueueStreamName,
		GroupName:    cfg.QueueConsumerGroup,
		ConsumerName: cfg.WorkerID,
		BlockTimeout: cfg.QueueBlockTimeout,
		Count:        uint(cfg.QueueReadCount),
	}

	redisConsumer, err := queue.NewRedisStreamConsumer(consumerCfg)
	if err != nil {
		return err
	}
	defer redisConsumer.Close()

	repo := workflow.NewPostgresRepository(db)
	service := worker.NewService(worker.ServiceConfig{
		WorkerID: cfg.WorkerID,
	}, redisConsumer, repo)

	log.Printf(
		"starting GoFlow worker id=%s stream=%s group=%s",
		cfg.WorkerID,
		cfg.QueueStreamName,
		cfg.QueueConsumerGroup,
	)

	for {
		if err := service.ProcessOne(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if errors.Is(err, queue.ErrNoMessage) {
				if ctx.Err() != nil {
					break
				}
				continue
			}

			log.Printf("worker process one failed: %v", err)
			continue
		}
	}

	log.Printf("GoFlow worker stopped gracefully id=%s", cfg.WorkerID)

	return nil
}
