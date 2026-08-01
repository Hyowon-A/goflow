package worker

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSchedulerAndWorkerExecuteFanOutFanInDAG(t *testing.T) {
	pool := workerServiceTestPool(t)
	fixture := seedWorkerDAG(t, pool)
	repo := workflow.NewPostgresRepository(pool)
	queue := &dagQueue{}
	scheduler := scheduler.NewService(repo, queue)
	worker := NewService(
		ServiceConfig{WorkerID: "worker-1"},
		queue,
		repo,
		repo,
		NewExecutorRegistry(map[string]Executor{
			ExecutorTypeLog: NewLogExecutor(nil),
		}),
	)

	if err := scheduler.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("queue roots: %v", err)
	}
	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	if err := scheduler.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("queue B and C: %v", err)
	}
	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("execute first fan-out task: %v", err)
	}
	if err := scheduler.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("scheduler should leave D blocked: %v", err)
	}
	if queue.pendingCount() != 1 {
		t.Fatalf("expected one fan-out task still queued, got %d", queue.pendingCount())
	}

	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("execute second fan-out task: %v", err)
	}
	if err := scheduler.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("queue D: %v", err)
	}
	if err := worker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("execute D: %v", err)
	}

	assertWorkerDAGStatuses(t, pool, fixture.workflowRunID, map[string]workflow.TaskRunStatus{
		fixture.taskIDs["A"]: workflow.TaskRunStatusCompleted,
		fixture.taskIDs["B"]: workflow.TaskRunStatusCompleted,
		fixture.taskIDs["C"]: workflow.TaskRunStatusCompleted,
		fixture.taskIDs["D"]: workflow.TaskRunStatusCompleted,
	})
	if len(queue.acks) != 4 {
		t.Fatalf("expected four acknowledged messages, got %#v", queue.acks)
	}
}

type workerDAGFixture struct {
	workflowName  string
	workflowID    string
	workflowRunID string
	taskIDs       map[string]string
}

func seedWorkerDAG(t *testing.T, pool *pgxpool.Pool) workerDAGFixture {
	t.Helper()

	ctx := context.Background()
	fixture := workerDAGFixture{
		workflowName:  fmt.Sprintf("day9-e2e-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskIDs: map[string]string{
			"A": uuid.NewString(),
			"B": uuid.NewString(),
			"C": uuid.NewString(),
			"D": uuid.NewString(),
		},
	}

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	for name, taskID := range fixture.taskIDs {
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, workflow_id, name, executor_type, config)
			VALUES ($1, $2, $3, $4, $5)
		`, taskID, fixture.workflowID, name, ExecutorTypeLog, map[string]any{"message": name})
		if err != nil {
			t.Fatalf("insert task %s: %v", name, err)
		}
	}
	for _, dependency := range [][2]string{
		{"A", "B"},
		{"A", "C"},
		{"B", "D"},
		{"C", "D"},
	} {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id)
			VALUES ($1, $2, $3)
		`, fixture.workflowID, fixture.taskIDs[dependency[0]], fixture.taskIDs[dependency[1]])
		if err != nil {
			t.Fatalf("insert dependency %s->%s: %v", dependency[0], dependency[1], err)
		}
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, workflow.WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	for _, taskID := range fixture.taskIDs {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), fixture.workflowID, fixture.workflowRunID, taskID, workflow.TaskRunStatusPending)
		if err != nil {
			t.Fatalf("insert task run: %v", err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `
			WITH target_workflows AS (
				SELECT id FROM workflows WHERE name = $1
			)
			DELETE FROM task_attempts
			WHERE task_run_id IN (
				SELECT id FROM task_runs
				WHERE workflow_id IN (SELECT id FROM target_workflows)
			)
		`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}

func assertWorkerDAGStatuses(t *testing.T, pool *pgxpool.Pool, workflowRunID string, want map[string]workflow.TaskRunStatus) {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT task_id, status
		FROM task_runs
		WHERE workflow_run_id = $1
	`, workflowRunID)
	if err != nil {
		t.Fatalf("query DAG task statuses: %v", err)
	}
	defer rows.Close()

	got := map[string]workflow.TaskRunStatus{}
	for rows.Next() {
		var taskID string
		var status workflow.TaskRunStatus
		if err := rows.Scan(&taskID, &status); err != nil {
			t.Fatalf("scan DAG task status: %v", err)
		}
		got[taskID] = status
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate DAG task statuses: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected DAG task statuses: got %#v, want %#v", got, want)
	}
}

type dagQueue struct {
	nextID  int
	pending []queue.ReceivedTaskMessage
	acks    []string
}

func (q *dagQueue) PublishTask(_ context.Context, message queue.TaskMessage) (string, error) {
	q.nextID++
	messageID := fmt.Sprintf("message-%d", q.nextID)
	q.pending = append(q.pending, queue.ReceivedTaskMessage{
		MessageID:   messageID,
		TaskMessage: message,
	})
	return messageID, nil
}

func (q *dagQueue) ReceiveTask(context.Context) (queue.ReceivedTaskMessage, error) {
	if len(q.pending) == 0 {
		return queue.ReceivedTaskMessage{}, queue.ErrNoMessage
	}

	message := q.pending[0]
	q.pending = q.pending[1:]
	return message, nil
}

func (q *dagQueue) AckTask(_ context.Context, messageID string) error {
	q.acks = append(q.acks, messageID)
	return nil
}

func (q *dagQueue) Close() error {
	return nil
}

func (q *dagQueue) pendingCount() int {
	return len(q.pending)
}
