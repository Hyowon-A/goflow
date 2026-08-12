package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/worker"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type loadConfig struct {
	runs    int
	workers int
	timeout time.Duration
	stream  string
}

type loadSummary struct {
	WorkflowRunsStarted   int64
	WorkflowRunsCompleted int64
	WorkflowRunsFailed    int64
	TaskAttempts          int64
	Retries               int64
	DeadLetters           int64
	OutboxPending         int64
	Elapsed               time.Duration
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatalf("loadcheck failed: %v", err)
	}
}

func run(args []string, out io.Writer) error {
	_ = godotenv.Load()

	loadCfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if loadCfg.stream == "" {
		loadCfg.stream = cfg.QueueStreamName + ":loadcheck"
	}

	ctx, cancel := context.WithTimeout(context.Background(), loadCfg.timeout)
	defer cancel()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer db.Close()

	publisher, err := queue.NewRedisStreamPublisher(queue.Config{
		Addr:       cfg.RedisAddr,
		StreamName: loadCfg.stream,
	})
	if err != nil {
		return fmt.Errorf("create Redis publisher: %w", err)
	}
	defer publisher.Close()

	repo := workflow.NewPostgresRepository(db)
	schedulerService := scheduler.NewService(repo, publisher)
	outboxDispatcher := scheduler.NewOutboxDispatcher(repo, publisher)
	consumers, err := startWorkers(ctx, cfg, loadCfg, repo, schedulerService)
	if err != nil {
		return err
	}
	defer func() {
		for _, consumer := range consumers {
			_ = consumer.Close()
		}
	}()

	startedAt := time.Now()
	workflowID, runIDs, err := createLoadWorkflowRuns(ctx, repo, schedulerService, loadCfg.runs)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	startMaintenanceLoop(ctx, &wg, schedulerService, outboxDispatcher)
	startWorkerLoops(ctx, &wg, cfg, consumers, repo, schedulerService)

	summary, err := waitForSummary(ctx, db, workflowID, len(runIDs), startedAt)
	cancel()
	wg.Wait()
	if err != nil {
		_ = renderSummary(out, summary)
		return err
	}
	if err := checkInvariants(summary, loadCfg.runs); err != nil {
		_ = renderSummary(out, summary)
		return err
	}

	return renderSummary(out, summary)
}

func parseFlags(args []string) (loadConfig, error) {
	flags := flag.NewFlagSet("loadcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	cfg := loadConfig{}
	flags.IntVar(&cfg.runs, "runs", 10, "workflow runs to create")
	flags.IntVar(&cfg.workers, "workers", 2, "in-process workers to run")
	flags.DurationVar(&cfg.timeout, "timeout", 60*time.Second, "maximum time to wait")
	flags.StringVar(&cfg.stream, "stream", "", "Redis stream override")

	if err := flags.Parse(args); err != nil {
		return loadConfig{}, err
	}
	if cfg.runs <= 0 {
		return loadConfig{}, errors.New("-runs must be positive")
	}
	if cfg.workers <= 0 {
		return loadConfig{}, errors.New("-workers must be positive")
	}
	if cfg.timeout <= 0 {
		return loadConfig{}, errors.New("-timeout must be positive")
	}
	return cfg, nil
}

func createLoadWorkflowRuns(ctx context.Context, repo *workflow.PostgresRepository, schedulerService *scheduler.Service, runs int) (string, []string, error) {
	service := workflow.NewService(repo)
	created, err := service.CreateWorkflow(ctx, workflow.CreateWorkflowInput{
		Name: fmt.Sprintf("loadcheck-%d", time.Now().UnixNano()),
	})
	if err != nil {
		return "", nil, fmt.Errorf("create load workflow: %w", err)
	}

	tasks := map[string]workflow.Task{}
	for _, input := range []workflow.CreateTaskInput{
		{Name: "A", ExecutorType: worker.ExecutorTypeLog, Config: map[string]any{"message": "loadcheck A"}},
		{Name: "B", ExecutorType: worker.ExecutorTypeSleep, Config: map[string]any{"duration": "10ms"}},
		{Name: "C", ExecutorType: worker.ExecutorTypeRandomFail, Config: map[string]any{
			"failure_probability": 0.2,
			"retry": map[string]any{
				"max_attempts":  2,
				"initial_delay": "10ms",
				"multiplier":    1,
			},
		}},
		{Name: "D", ExecutorType: worker.ExecutorTypeLog, Config: map[string]any{"message": "loadcheck D"}},
	} {
		task, err := service.CreateTask(ctx, created.ID, input)
		if err != nil {
			return "", nil, fmt.Errorf("create task %s: %w", input.Name, err)
		}
		tasks[input.Name] = task
	}

	for _, edge := range [][2]string{{"A", "B"}, {"A", "C"}, {"B", "D"}, {"C", "D"}} {
		if _, err := service.CreateDependency(ctx, created.ID, workflow.CreateDependencyInput{
			PredecessorTaskID: tasks[edge[0]].ID,
			SuccessorTaskID:   tasks[edge[1]].ID,
		}); err != nil {
			return "", nil, fmt.Errorf("create dependency %s->%s: %w", edge[0], edge[1], err)
		}
	}

	runIDs := make([]string, 0, runs)
	for i := 0; i < runs; i++ {
		run, err := service.CreateWorkflowRun(ctx, created.ID, workflow.CreateWorkflowRunInput{
			Input: map[string]any{"loadcheck_run": i + 1},
		})
		if err != nil {
			return "", nil, fmt.Errorf("create workflow run %d: %w", i+1, err)
		}
		runIDs = append(runIDs, run.ID)
		if err := schedulerService.QueueRunnableTaskRuns(ctx, run.ID); err != nil {
			return "", nil, fmt.Errorf("queue workflow run %d roots: %w", i+1, err)
		}
	}

	return created.ID, runIDs, nil
}

func startWorkers(ctx context.Context, cfg config.Config, loadCfg loadConfig, repo *workflow.PostgresRepository, schedulerService *scheduler.Service) ([]*queue.RedisStreamConsumer, error) {
	consumers := make([]*queue.RedisStreamConsumer, 0, loadCfg.workers)
	groupName := fmt.Sprintf("%s-loadcheck-%d", cfg.QueueConsumerGroup, time.Now().UnixNano())

	for i := 0; i < loadCfg.workers; i++ {
		consumer, err := queue.NewRedisStreamConsumer(queue.ConsumerConfig{
			Addr:         cfg.RedisAddr,
			StreamName:   loadCfg.stream,
			GroupName:    groupName,
			ConsumerName: fmt.Sprintf("%s-%d", cfg.WorkerID, i+1),
			BlockTimeout: 200 * time.Millisecond,
			Count:        1,
		})
		if err != nil {
			for _, consumer := range consumers {
				_ = consumer.Close()
			}
			return nil, fmt.Errorf("create Redis consumer: %w", err)
		}
		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

func startWorkerLoops(ctx context.Context, wg *sync.WaitGroup, cfg config.Config, consumers []*queue.RedisStreamConsumer, repo *workflow.PostgresRepository, schedulerService *scheduler.Service) {
	for i, consumer := range consumers {
		service := worker.NewService(
			worker.ServiceConfig{
				WorkerID:          fmt.Sprintf("%s-loadcheck-%d", cfg.WorkerID, i+1),
				LeaseDuration:     cfg.WorkerLeaseDuration,
				HeartbeatInterval: cfg.WorkerHeartbeatInterval,
			},
			consumer,
			repo,
			repo,
			worker.NewExecutorRegistry(map[string]worker.Executor{
				worker.ExecutorTypeSleep:      worker.SleepExecutor{},
				worker.ExecutorTypeLog:        worker.NewLogExecutor(log.New(io.Discard, "", 0)),
				worker.ExecutorTypeRandomFail: worker.NewRandomFailExecutor(nil),
			}),
			schedulerService,
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				err := service.ProcessOne(ctx)
				if err == nil || errors.Is(err, queue.ErrNoMessage) {
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				log.Printf("loadcheck worker failed: %v", err)
			}
		}()
	}
}

func startMaintenanceLoop(ctx context.Context, wg *sync.WaitGroup, schedulerService *scheduler.Service, outboxDispatcher *scheduler.OutboxDispatcher) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_ = schedulerService.QueueDueRetryTaskRuns(ctx, now)
				_ = schedulerService.RecoverExpiredRunningTaskRuns(ctx)
				_ = outboxDispatcher.DispatchPendingTaskOutboxEvents(ctx)
			}
		}
	}()
}

func waitForSummary(ctx context.Context, db *pgxpool.Pool, workflowID string, runs int, startedAt time.Time) (loadSummary, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		summary, err := loadSummaryForWorkflow(ctx, db, workflowID, startedAt)
		if err != nil {
			return summary, err
		}
		if summary.WorkflowRunsCompleted+summary.WorkflowRunsFailed == int64(runs) {
			return summary, nil
		}

		select {
		case <-ctx.Done():
			summary.Elapsed = time.Since(startedAt)
			return summary, fmt.Errorf("timeout waiting for %d workflow runs to finish: %w", runs, ctx.Err())
		case <-ticker.C:
		}
	}
}

func loadSummaryForWorkflow(ctx context.Context, db *pgxpool.Pool, workflowID string, startedAt time.Time) (loadSummary, error) {
	var summary loadSummary
	err := db.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM workflow_runs
		WHERE workflow_id = $1
	`, workflowID).Scan(&summary.WorkflowRunsStarted, &summary.WorkflowRunsCompleted, &summary.WorkflowRunsFailed)
	if err != nil {
		return loadSummary{}, fmt.Errorf("summarize workflow runs: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(GREATEST(tr.attempt_count - 1, 0)), 0),
			COUNT(*) FILTER (WHERE tr.status = 'dead_letter')
		FROM task_runs tr
		WHERE tr.workflow_id = $1
	`, workflowID).Scan(&summary.Retries, &summary.DeadLetters)
	if err != nil {
		return loadSummary{}, fmt.Errorf("summarize task runs: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_attempts ta
		JOIN task_runs tr ON tr.id = ta.task_run_id
		WHERE tr.workflow_id = $1
	`, workflowID).Scan(&summary.TaskAttempts)
	if err != nil {
		return loadSummary{}, fmt.Errorf("summarize task attempts: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_outbox_events
		WHERE workflow_id = $1
			AND status = 'pending'
	`, workflowID).Scan(&summary.OutboxPending)
	if err != nil {
		return loadSummary{}, fmt.Errorf("summarize task outbox: %w", err)
	}

	summary.Elapsed = time.Since(startedAt)
	return summary, nil
}

func checkInvariants(summary loadSummary, wantRuns int) error {
	if summary.WorkflowRunsStarted != int64(wantRuns) {
		return fmt.Errorf("expected %d workflow runs, got %d", wantRuns, summary.WorkflowRunsStarted)
	}
	if summary.WorkflowRunsCompleted+summary.WorkflowRunsFailed != summary.WorkflowRunsStarted {
		return fmt.Errorf("expected all workflow runs terminal, got completed=%d failed=%d started=%d", summary.WorkflowRunsCompleted, summary.WorkflowRunsFailed, summary.WorkflowRunsStarted)
	}
	if summary.TaskAttempts == 0 {
		return errors.New("expected task attempts")
	}
	if summary.OutboxPending != 0 {
		return fmt.Errorf("expected no pending outbox events, got %d", summary.OutboxPending)
	}
	return nil
}

func renderSummary(w io.Writer, summary loadSummary) error {
	_, err := fmt.Fprintf(w, `workflow runs started: %d
workflow runs completed: %d
workflow runs failed: %d
task attempts: %d
retries: %d
dead letters: %d
outbox pending: %d
elapsed: %s
`,
		summary.WorkflowRunsStarted,
		summary.WorkflowRunsCompleted,
		summary.WorkflowRunsFailed,
		summary.TaskAttempts,
		summary.Retries,
		summary.DeadLetters,
		summary.OutboxPending,
		summary.Elapsed.Round(time.Millisecond),
	)
	return err
}
