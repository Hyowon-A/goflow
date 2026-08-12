package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
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
	publisher, err := queue.NewRedisStreamPublisher(queue.Config{
		Addr:       cfg.RedisAddr,
		StreamName: cfg.QueueStreamName,
	})
	if err != nil {
		return err
	}
	defer publisher.Close()
	outboxDispatcher := scheduler.NewOutboxDispatcher(repo, publisher)
	schedulerService := scheduler.NewService(repo, publisher)

	executors := worker.NewExecutorRegistry(map[string]worker.Executor{
		worker.ExecutorTypeSleep:      worker.SleepExecutor{},
		worker.ExecutorTypeLog:        worker.NewLogExecutor(log.Default()),
		worker.ExecutorTypeRandomFail: worker.NewRandomFailExecutor(nil),
	})
	service := worker.NewService(
		worker.ServiceConfig{
			WorkerID:          cfg.WorkerID,
			LeaseDuration:     cfg.WorkerLeaseDuration,
			HeartbeatInterval: cfg.WorkerHeartbeatInterval,
		},
		redisConsumer,
		repo,
		repo,
		executors,
		schedulerService,
	)
	outboxTicker := time.NewTicker(cfg.QueueBlockTimeout)
	defer outboxTicker.Stop()
	recoveryTicker := time.NewTicker(cfg.WorkerRecoveryInterval)
	defer recoveryTicker.Stop()
	dispatchOutbox := func() {
		if err := outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx); err != nil {
			log.Printf("task outbox dispatch failed: %v", err)
		}
	}
	recoverExpiredTaskRuns := func() {
		if err := schedulerService.RecoverExpiredRunningTaskRuns(ctx); err != nil {
			log.Printf("recover expired task runs failed: %v", err)
		}
	}

	log.Printf(
		"starting GoFlow worker id=%s stream=%s group=%s",
		cfg.WorkerID,
		cfg.QueueStreamName,
		cfg.QueueConsumerGroup,
	)

	for {
		select {
		case <-outboxTicker.C:
			dispatchOutbox()
		case <-recoveryTicker.C:
			recoverExpiredTaskRuns()
		default:
		}

		if err := service.ProcessOne(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if errors.Is(err, queue.ErrNoMessage) {
				if ctx.Err() != nil {
					break
				}
				dispatchOutbox()
				continue
			}

			log.Printf("worker process one failed: %v", err)
			continue
		}
	}

	log.Printf("GoFlow worker stopped gracefully id=%s", cfg.WorkerID)

	return nil
}
