package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/recallify"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/worker"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/joho/godotenv"
)

const defaultWorkflowName = "recallify"
const defaultRecallifyFixturePath = "examples/recallify/go-notes.txt"

var (
	errPositiveRuns    = errors.New("-runs must be positive")
	errPositiveWorkers = errors.New("-workers must be positive")
	errPositiveTimeout = errors.New("-timeout must be positive")
	errEmptyFixture    = errors.New("recallify fixture is empty")
)

type recallifyConfig struct {
	runs         int
	workers      int
	timeout      time.Duration
	stream       string
	recallifyURL string
	fixture      string
}

type workflowCreator interface {
	CreateWorkflow(context.Context, workflow.CreateWorkflowInput) (workflow.Workflow, error)
	CreateWorkflowRun(context.Context, string, workflow.CreateWorkflowRunInput) (workflow.WorkflowRun, error)
	CreateTask(context.Context, string, workflow.CreateTaskInput) (workflow.Task, error)
	CreateDependency(context.Context, string, workflow.CreateDependencyInput) (workflow.Dependency, error)
}

type taskRunQueuer interface {
	QueueRunnableTaskRuns(context.Context, string) error
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatalf("recallify failed: %v", err)
	}
}

func run(args []string, out io.Writer) error {
	_ = godotenv.Load()

	recallifyCfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if recallifyCfg.stream == "" {
		recallifyCfg.stream = cfg.QueueStreamName + ":recallify"
	}

	fixtureText, err := loadRecallifyFixture(recallifyCfg.fixture)
	if err != nil {
		return err
	}

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, recallifyCfg.timeout)
	defer cancel()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	repo := workflow.NewPostgresRepository(db)
	publisher, err := queue.NewRedisStreamPublisher(queue.Config{
		Addr:       cfg.RedisAddr,
		StreamName: recallifyCfg.stream,
	})
	if err != nil {
		return err
	}
	defer publisher.Close()
	if recallifyCfg.recallifyURL == "" {
		server := startFakeRecallifyServer()
		defer server.Close()
		recallifyCfg.recallifyURL = server.URL
	}

	service := workflow.NewService(repo)
	created, tasks, err := createRecallifyWorkflow(ctx, service, defaultWorkflowName, recallifyCfg.recallifyURL)
	if err != nil {
		return err
	}
	schedulerService := scheduler.NewService(repo, publisher)
	consumers, err := startWorkers(ctx, cfg, recallifyCfg)
	if err != nil {
		return err
	}
	defer func() {
		for _, consumer := range consumers {
			_ = consumer.Close()
		}
	}()

	var wg sync.WaitGroup
	startWorkerLoops(ctx, &wg, cfg, consumers, repo, schedulerService)
	defer func() {
		cancel()
		wg.Wait()
	}()

	startedAt := time.Now()
	runIDs, err := createRecallifyWorkflowRuns(ctx, service, created.ID, recallifyCfg.runs, fixtureText, schedulerService)
	if err != nil {
		return err
	}

	template := recallify.NewTemplate(recallifyCfg.recallifyURL, "")
	summary, err := waitForRecallifySummary(ctx, db, created.ID, len(runIDs), len(tasks), len(template.Dependencies), startedAt)
	if err != nil {
		_ = renderRecallifySummary(out, summary)
		return err
	}
	if err := checkRecallifyInvariants(summary, recallifyCfg.runs); err != nil {
		_ = renderRecallifySummary(out, summary)
		return err
	}
	return renderRecallifySummary(out, summary)
}

func parseFlags(args []string) (recallifyConfig, error) {
	flags := flag.NewFlagSet("recallify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	recallifyCfg := recallifyConfig{}
	flags.IntVar(&recallifyCfg.runs, "runs", 3, "workflow runs to create")
	flags.IntVar(&recallifyCfg.workers, "workers", 2, "in-process workers to run")
	flags.DurationVar(&recallifyCfg.timeout, "timeout", 90*time.Second, "maximum time to wait")
	flags.StringVar(&recallifyCfg.stream, "stream", "", "Redis stream override")
	flags.StringVar(&recallifyCfg.recallifyURL, "recallify-url", "", "Recallify backend URL; empty uses deterministic fake generation")
	flags.StringVar(&recallifyCfg.fixture, "fixture", "", "path to a text fixture")

	if err := flags.Parse(args); err != nil {
		return recallifyConfig{}, err
	}
	if recallifyCfg.runs <= 0 {
		return recallifyConfig{}, errPositiveRuns
	}
	if recallifyCfg.workers <= 0 {
		return recallifyConfig{}, errPositiveWorkers
	}
	if recallifyCfg.timeout <= 0 {
		return recallifyConfig{}, errPositiveTimeout
	}
	return recallifyCfg, nil
}

func loadRecallifyFixture(path string) (string, error) {
	if path == "" {
		path = defaultRecallifyFixturePath
	}
	data, err := os.ReadFile(path)
	if err != nil && path == defaultRecallifyFixturePath {
		data, err = os.ReadFile(filepath.Join("..", "..", path))
	}
	if err != nil {
		return "", fmt.Errorf("load recallify fixture: %w", err)
	}
	text := string(data)
	if strings.TrimSpace(text) == "" {
		return "", errEmptyFixture
	}
	return text, nil
}

func startFakeRecallifyServer() *httptest.Server {
	mcqs := `[{"question":"What does GoFlow coordinate?",
				"option1":"Durable workflow tasks",
				"explanation1":"GoFlow stores state and queues task runs.",
				"option2":"CSS themes",
				"explanation2":"That is not part of this workflow demo.",
				"option3":"Browser tabs",
				"explanation3":"The demo is backend orchestration.",
				"option4":"Image filters",
				"explanation4":"The demo does not process images.",
				"answer":1}]`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ai/generateMcqs" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"mcqs": mcqs})
	}))
}

func startWorkers(ctx context.Context, cfg config.Config, recallifyCfg recallifyConfig) ([]*queue.RedisStreamConsumer, error) {
	consumers := make([]*queue.RedisStreamConsumer, 0, recallifyCfg.workers)
	groupName := fmt.Sprintf("%s-recallify-%d", cfg.QueueConsumerGroup, time.Now().UnixNano())

	for i := 0; i < recallifyCfg.workers; i++ {
		consumer, err := queue.NewRedisStreamConsumer(queue.ConsumerConfig{
			Addr:         cfg.RedisAddr,
			StreamName:   recallifyCfg.stream,
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
				WorkerID:          fmt.Sprintf("%s-recallify-%d", cfg.WorkerID, i+1),
				LeaseDuration:     cfg.WorkerLeaseDuration,
				HeartbeatInterval: cfg.WorkerHeartbeatInterval,
			},
			consumer,
			repo,
			repo,
			newExecutorRegistry(),
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
				log.Printf("recallify worker failed: %v", err)
			}
		}()
	}
}

func newExecutorRegistry() worker.ExecutorRegistry {
	return worker.NewExecutorRegistry(map[string]worker.Executor{
		recallify.ExecutorTypeValidateRequest: recallify.RecallifyValidateRequestExecutor{},
		recallify.ExecutorTypeCleanText:       recallify.RecallifyCleanTextExecutor{},
		recallify.ExecutorTypeGenerateMCQs:    recallify.NewRecallifyGenerateMCQsExecutor(recallify.RecallifyClient{}),
		recallify.ExecutorTypeValidateMCQs:    recallify.RecallifyValidateMCQsExecutor{},
		recallify.ExecutorTypeMergeStudySet:   recallify.RecallifyMergeStudySetExecutor{},
		recallify.ExecutorTypeNotifyCallback:  recallify.RecallifyNotifyCallbackExecutor{},
	})
}

func createRecallifyWorkflow(ctx context.Context, service workflowCreator, name string, recallifyURL string) (workflow.Workflow, map[string]workflow.Task, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultWorkflowName
	}

	created, err := service.CreateWorkflow(ctx, workflow.CreateWorkflowInput{Name: name})
	if err != nil {
		return workflow.Workflow{}, nil, fmt.Errorf("create recallify workflow: %w", err)
	}

	tasks := map[string]workflow.Task{}
	template := recallify.NewTemplate(recallifyURL, "")
	for _, spec := range template.Tasks {
		task, err := service.CreateTask(ctx, created.ID, workflow.CreateTaskInput{
			Name:         spec.Name,
			ExecutorType: spec.ExecutorType,
			Config:       spec.Config,
		})
		if err != nil {
			return workflow.Workflow{}, nil, fmt.Errorf("create task %s: %w", spec.Name, err)
		}
		tasks[spec.Name] = task
	}

	for _, edge := range template.Dependencies {
		if _, err := service.CreateDependency(ctx, created.ID, workflow.CreateDependencyInput{
			PredecessorTaskID: tasks[edge[0]].ID,
			SuccessorTaskID:   tasks[edge[1]].ID,
		}); err != nil {
			return workflow.Workflow{}, nil, fmt.Errorf("create dependency %s->%s: %w", edge[0], edge[1], err)
		}
	}

	return created, tasks, nil
}

func createRecallifyWorkflowRuns(ctx context.Context, service workflowCreator, workflowID string, runs int, fixtureText string, schedulerService taskRunQueuer) ([]string, error) {
	runIDs := make([]string, 0, runs)
	for i := 0; i < runs; i++ {
		run, err := service.CreateWorkflowRun(ctx, workflowID, workflow.CreateWorkflowRunInput{
			Input: map[string]any{
				"document_text":       fixtureText,
				"title":               "Recallify Local Demo",
				"level":               "medium",
				"mcq_count":           1,
				"external_request_id": fmt.Sprintf("recallify-local-%d", i+1),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create recallify workflow run %d: %w", i+1, err)
		}
		runIDs = append(runIDs, run.ID)
		if err := schedulerService.QueueRunnableTaskRuns(ctx, run.ID); err != nil {
			return nil, fmt.Errorf("queue recallify workflow run %d roots: %w", i+1, err)
		}
	}
	return runIDs, nil
}
