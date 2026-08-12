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

	"github.com/joho/godotenv"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/httpserver"
	"github.com/Hyowon-A/goflow/internal/metrics"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("goflow API failed: %v", err) // prints the error and stops the program with a non-zero exit code.
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

	app := httpserver.NewWithMetrics(db, registry, scheduler.NewServiceWithMetrics(repo, publisher, registry))

	server := &http.Server{
		Addr:              cfg.Address(),
		Handler:           app.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("starting GoFlow API on %s", cfg.Address())
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")

	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	log.Println("GoFlow API stopped gracefully")

	return nil
}
