package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/metrics"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/recallify"
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

	registry := metrics.NewRegistry()
	registry.Gauge("goflow_outbox_pending", repo.CountPendingTaskOutboxEvents)
	registry.Gauge("goflow_task_runs_running", repo.CountRunningTaskRuns)
	registry.Gauge("goflow_task_runs_expired_leases", repo.CountExpiredRunningTaskRuns)

	outboxDispatcher := scheduler.NewOutboxDispatcherWithMetrics(repo, publisher, registry)
	schedulerService := scheduler.NewServiceWithMetrics(repo, publisher, registry)

	var metricsServer *http.Server
	var metricsServerErrors <-chan error
	if cfg.WorkerMetricsPort != "" {
		metricsServer = &http.Server{
			Addr:              ":" + cfg.WorkerMetricsPort,
			Handler:           workerMetricsHandler(registry),
			ReadHeaderTimeout: 5 * time.Second,
		}
		serverErrors := make(chan error, 1)
		metricsServerErrors = serverErrors
		go func() {
			log.Printf("starting GoFlow worker metrics on :%s", cfg.WorkerMetricsPort)
			serverErrors <- metricsServer.ListenAndServe()
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(shutdownCtx)
		}()
	}

	service := worker.NewServiceWithMetrics(
		worker.ServiceConfig{
			WorkerID:          cfg.WorkerID,
			LeaseDuration:     cfg.WorkerLeaseDuration,
			HeartbeatInterval: cfg.WorkerHeartbeatInterval,
		},
		redisConsumer,
		repo,
		repo,
		newExecutorRegistry(),
		registry,
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
		case err := <-metricsServerErrors:
			if !errors.Is(err, http.ErrServerClosed) {
				return err
			}
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

func newExecutorRegistry() worker.ExecutorRegistry {
	return worker.NewExecutorRegistry(map[string]worker.Executor{
		worker.ExecutorTypeSleep:              worker.SleepExecutor{},
		worker.ExecutorTypeLog:                worker.NewLogExecutor(log.Default()),
		worker.ExecutorTypeRandomFail:         worker.NewRandomFailExecutor(nil),
		recallify.ExecutorTypeValidateRequest: recallify.RecallifyValidateRequestExecutor{},
		recallify.ExecutorTypeCleanText:       recallify.RecallifyCleanTextExecutor{},
		recallify.ExecutorTypeGenerateMCQs:    recallify.NewRecallifyGenerateMCQsExecutor(recallify.RecallifyClient{}),
		recallify.ExecutorTypeValidateMCQs:    recallify.RecallifyValidateMCQsExecutor{},
		recallify.ExecutorTypeMergeStudySet:   recallify.RecallifyMergeStudySetExecutor{},
		recallify.ExecutorTypeNotifyCallback:  recallify.RecallifyNotifyCallbackExecutor{},
	})
}

func workerMetricsHandler(registry *metrics.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
