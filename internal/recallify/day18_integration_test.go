package recallify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/worker"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	recallifyTestPoolOnce sync.Once
	recallifyTestPool     *pgxpool.Pool
	recallifyTestPoolErr  error
)

func TestRecallifyDay18WorkflowCompletesAndStoresMergedOutput(t *testing.T) {
	pool := recallifyIntegrationTestPool(t)
	ctx := context.Background()
	repo := workflow.NewPostgresRepository(pool)
	workflowService := workflow.NewService(repo)
	createdWorkflow, tasks := createDay18RecallifyWorkflow(t, workflowService)

	workflowRun, err := workflowService.CreateWorkflowRun(ctx, createdWorkflow.ID, workflow.CreateWorkflowRunInput{
		Input: map[string]any{
			"document_text":        "  GoFlow runs background workflows. ",
			"title":                " GoFlow Basics ",
			"level":                "medium",
			"mcq_count":            1,
			"external_request_id":  "req-1",
			"ignored_product_data": "ignored",
		},
	})
	if err != nil {
		t.Fatalf("create workflow run: %v", err)
	}

	queue := &recallifyTestQueue{}
	schedulerService := scheduler.NewService(repo, queue)
	workerService := worker.NewService(
		worker.ServiceConfig{WorkerID: "worker-1", LeaseDuration: 30 * time.Second},
		queue,
		repo,
		repo,
		worker.NewExecutorRegistry(map[string]worker.Executor{
			ExecutorTypeValidateRequest: RecallifyValidateRequestExecutor{},
			ExecutorTypeCleanText:       RecallifyCleanTextExecutor{},
			ExecutorTypeGenerateMCQs: &recallifyFakeExecutor{output: map[string]any{
				"kind":            "mcq",
				"requested_count": 1,
				"raw_json":        `[` + validRecallifyMCQJSON("What does GoFlow run?") + `]`,
			}},
			ExecutorTypeValidateMCQs:  RecallifyValidateMCQsExecutor{},
			ExecutorTypeMergeStudySet: RecallifyMergeStudySetExecutor{},
		}),
		schedulerService,
	)

	if err := schedulerService.QueueRunnableTaskRuns(ctx, workflowRun.ID); err != nil {
		t.Fatalf("queue root: %v", err)
	}
	for range tasks {
		if err := workerService.ProcessOne(ctx); err != nil {
			t.Fatalf("process recallify task: %v", err)
		}
	}

	completed, err := workflowService.GetWorkflowRun(ctx, createdWorkflow.ID, workflowRun.ID)
	if err != nil {
		t.Fatalf("get completed workflow run: %v", err)
	}
	if completed.Status != string(workflow.WorkflowRunStatusCompleted) {
		t.Fatalf("expected completed workflow, got %q", completed.Status)
	}
	if completed.Output["title"] != "GoFlow Basics" || completed.Output["level"] != "medium" {
		t.Fatalf("unexpected output metadata: %#v", completed.Output)
	}
	if completed.Output["external_request_id"] != "req-1" {
		t.Fatalf("expected external_request_id, got %#v", completed.Output["external_request_id"])
	}
	counts, ok := completed.Output["counts"].(map[string]any)
	if !ok || counts["mcqs"] != float64(1) {
		t.Fatalf("unexpected counts: %#v", completed.Output["counts"])
	}
	mcqs, ok := completed.Output["mcqs"].([]any)
	if !ok || len(mcqs) != 1 {
		t.Fatalf("expected one stored mcq, got %#v", completed.Output["mcqs"])
	}
	if tasks["merge_study_set"].ExecutorType != ExecutorTypeMergeStudySet {
		t.Fatalf("unexpected merge executor: %q", tasks["merge_study_set"].ExecutorType)
	}
}

func createDay18RecallifyWorkflow(t *testing.T, service *workflow.Service) (workflow.Workflow, map[string]workflow.Task) {
	t.Helper()

	ctx := context.Background()
	name := fmt.Sprintf("day18-recallify-%d", time.Now().UnixNano())
	createdWorkflow, err := service.CreateWorkflow(ctx, workflow.CreateWorkflowInput{Name: name})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	pool := recallifyIntegrationTestPool(t)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `
			WITH target_workflows AS (
				SELECT id FROM workflows WHERE name = $1
			)
			DELETE FROM task_attempts
			WHERE task_run_id IN (
				SELECT id FROM task_runs
				WHERE workflow_id IN (SELECT id FROM target_workflows)
			)
		`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, name)
	})

	specs := []struct {
		name         string
		executorType string
	}{
		{"validate_request", ExecutorTypeValidateRequest},
		{"clean_text", ExecutorTypeCleanText},
		{"generate_mcqs", ExecutorTypeGenerateMCQs},
		{"validate_mcqs", ExecutorTypeValidateMCQs},
		{"merge_study_set", ExecutorTypeMergeStudySet},
	}

	tasks := map[string]workflow.Task{}
	for _, spec := range specs {
		task, err := service.CreateTask(ctx, createdWorkflow.ID, workflow.CreateTaskInput{
			Name:         spec.name,
			ExecutorType: spec.executorType,
		})
		if err != nil {
			t.Fatalf("create task %s: %v", spec.name, err)
		}
		tasks[spec.name] = task
	}
	for _, edge := range [][2]string{
		{"validate_request", "clean_text"},
		{"clean_text", "generate_mcqs"},
		{"generate_mcqs", "validate_mcqs"},
		{"validate_mcqs", "merge_study_set"},
	} {
		if _, err := service.CreateDependency(ctx, createdWorkflow.ID, workflow.CreateDependencyInput{
			PredecessorTaskID: tasks[edge[0]].ID,
			SuccessorTaskID:   tasks[edge[1]].ID,
		}); err != nil {
			t.Fatalf("create dependency %s->%s: %v", edge[0], edge[1], err)
		}
	}

	return createdWorkflow, tasks
}

func recallifyIntegrationTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	recallifyTestPoolOnce.Do(func() {
		databaseURL := os.Getenv("DATABASE_URL")
		if databaseURL == "" {
			databaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
		}
		recallifyTestPool, recallifyTestPoolErr = database.Connect(context.Background(), databaseURL)
		if recallifyTestPoolErr == nil {
			var schemaExists bool
			recallifyTestPoolErr = recallifyTestPool.QueryRow(context.Background(), `SELECT to_regclass('public.workflows') IS NOT NULL`).Scan(&schemaExists)
			if recallifyTestPoolErr == nil && !schemaExists {
				recallifyTestPoolErr = errors.New("database schema is not applied")
			}
		}
	})
	if recallifyTestPoolErr != nil {
		t.Skipf("postgres not available for Recallify workflow integration test (run `make postgres-up`): %v", recallifyTestPoolErr)
	}
	return recallifyTestPool
}

type recallifyTestQueue struct {
	nextID  int
	pending []queue.ReceivedTaskMessage
}

func (q *recallifyTestQueue) PublishTask(_ context.Context, message queue.TaskMessage) (string, error) {
	q.nextID++
	messageID := fmt.Sprintf("message-%d", q.nextID)
	q.pending = append(q.pending, queue.ReceivedTaskMessage{MessageID: messageID, TaskMessage: message})
	return messageID, nil
}

func (q *recallifyTestQueue) ReceiveTask(context.Context) (queue.ReceivedTaskMessage, error) {
	if len(q.pending) == 0 {
		return queue.ReceivedTaskMessage{}, queue.ErrNoMessage
	}
	message := q.pending[0]
	q.pending = q.pending[1:]
	return message, nil
}

func (*recallifyTestQueue) AckTask(context.Context, string) error { return nil }
func (*recallifyTestQueue) Close() error                          { return nil }

type recallifyFakeExecutor struct{ output map[string]any }

func (f *recallifyFakeExecutor) Execute(context.Context, worker.ExecutionInput) (worker.ExecutionResult, error) {
	return worker.ExecutionResult{Output: f.output}, nil
}
