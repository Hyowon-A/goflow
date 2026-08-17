package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryQueueRunnableTaskRunsQueuesOnlyUnblockedPendingTasks(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableTaskRuns(t, pool)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), " "+fixture.workflowRunID+" ")
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}

	if len(queued) != 1 {
		t.Fatalf("expected one queued task run, got %#v", queued)
	}
	if queued[0].TaskID != fixture.readyTaskID || queued[0].Status != TaskRunStatusQueued {
		t.Fatalf("unexpected queued task run: %#v", queued[0])
	}

	statuses := taskRunStatusesByTask(t, pool, fixture.workflowRunID)
	want := map[string]TaskRunStatus{
		fixture.doneTaskID:    TaskRunStatusCompleted,
		fixture.readyTaskID:   TaskRunStatusQueued,
		fixture.blockedTaskID: TaskRunStatusPending,
	}
	if !reflect.DeepEqual(statuses, want) {
		t.Fatalf("unexpected task run statuses: got %#v, want %#v", statuses, want)
	}

	queuedAgain, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue runnable task runs again: %v", err)
	}
	if len(queuedAgain) != 0 {
		t.Fatalf("expected idempotent second queue to return no rows, got %#v", queuedAgain)
	}
	assertTaskOutboxEventCount(t, pool, queued[0].ID, 1)
}

func TestPostgresRepositoryQueueRunnableTaskRunsCreatesRootOutboxEvent(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableRoots(t, pool, 1)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue root task run: %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("expected one queued root task run, got %#v", queued)
	}

	assertTaskOutboxEvent(t, pool, queued[0])
}

func TestPostgresRepositoryQueueRunnableTaskRunsRejectsBlankWorkflowRunID(t *testing.T) {
	repo := NewPostgresRepository(workflowClaimTestPool(t))

	_, err := repo.QueueRunnableTaskRuns(context.Background(), " ")
	if !errors.Is(err, ErrWorkflowRunNotFound) {
		t.Fatalf("expected ErrWorkflowRunNotFound, got %v", err)
	}
}

func TestPostgresRepositoryQueueRunnableTaskRunsHandlesFanOutAndFanIn(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableDAG(t, pool)
	repo := NewPostgresRepository(pool)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue first runnable task runs: %v", err)
	}
	want := []string{fixture.taskIDs["B"], fixture.taskIDs["C"]}
	sort.Strings(want)
	if got := taskIDs(queued); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected B and C to queue after A, got %#v", got)
	}
	assertWorkflowRunOutboxEventCount(t, pool, fixture.workflowRunID, 2)
	assertTaskRunStatuses(t, pool, fixture.workflowRunID, map[string]TaskRunStatus{
		fixture.taskIDs["A"]: TaskRunStatusCompleted,
		fixture.taskIDs["B"]: TaskRunStatusQueued,
		fixture.taskIDs["C"]: TaskRunStatusQueued,
		fixture.taskIDs["D"]: TaskRunStatusPending,
	})

	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], TaskRunStatusCompleted)
	queued, err = repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue after B completes: %v", err)
	}
	if len(queued) != 0 {
		t.Fatalf("expected D to stay pending until C completes, got %#v", queued)
	}

	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], TaskRunStatusCompleted)
	queued, err = repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue after C completes: %v", err)
	}
	if got := taskIDs(queued); !reflect.DeepEqual(got, []string{fixture.taskIDs["D"]}) {
		t.Fatalf("expected D to queue after B and C complete, got %#v", got)
	}
	assertWorkflowRunOutboxEventCount(t, pool, fixture.workflowRunID, 3)

	queued, err = repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue D duplicate scheduler call: %v", err)
	}
	if len(queued) != 0 {
		t.Fatalf("expected duplicate successor scheduling to return no rows, got %#v", queued)
	}
	assertWorkflowRunOutboxEventCount(t, pool, fixture.workflowRunID, 3)
}

func TestPostgresRepositoryQueueRunnableTaskRunsStoresPredecessorInput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableDAG(t, pool)
	repo := NewPostgresRepository(pool)
	workflowInput := map[string]any{"document_text": "lecture notes"}
	predecessorOutput := map[string]any{"clean_text": "lecture notes"}
	setWorkflowRunInput(t, pool, fixture.workflowRunID, workflowInput)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["A"], predecessorOutput)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	want := []string{fixture.taskIDs["B"], fixture.taskIDs["C"]}
	sort.Strings(want)
	if got := taskIDs(queued); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected B and C to queue, got %#v", got)
	}

	inputs := taskRunInputsByTaskName(t, pool, fixture.workflowRunID)
	for _, taskName := range []string{"B", "C"} {
		assertSuccessorInput(t, inputs[taskName], workflowInput, map[string]any{"A": predecessorOutput})
	}
}

func TestPostgresRepositoryQueueRunnableTaskRunsStoresFanInPredecessorInputs(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableDAG(t, pool)
	repo := NewPostgresRepository(pool)
	workflowInput := map[string]any{"document_text": "lecture notes"}
	outputB := map[string]any{"mcqs": "raw"}
	outputC := map[string]any{"flashcards": "raw"}
	setWorkflowRunInput(t, pool, fixture.workflowRunID, workflowInput)
	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], TaskRunStatusCompleted)
	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], TaskRunStatusCompleted)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], outputB)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], outputC)

	queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	if got := taskIDs(queued); !reflect.DeepEqual(got, []string{fixture.taskIDs["D"]}) {
		t.Fatalf("expected D to queue, got %#v", got)
	}

	inputs := taskRunInputsByTaskName(t, pool, fixture.workflowRunID)
	assertSuccessorInput(t, inputs["D"], workflowInput, map[string]any{
		"B": outputB,
		"C": outputC,
	})
}

func TestPostgresRepositoryQueueRunnableTaskRunsIgnoresUnrelatedWorkflowRunOutputs(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableDAG(t, pool)
	repo := NewPostgresRepository(pool)
	workflowInput := map[string]any{"document_text": "lecture notes"}
	currentOutput := map[string]any{"mcqs": "current"}
	unrelatedOutput := map[string]any{"mcqs": "unrelated"}
	setWorkflowRunInput(t, pool, fixture.workflowRunID, workflowInput)
	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], TaskRunStatusCompleted)
	setTaskRunStatus(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], TaskRunStatusCompleted)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["B"], currentOutput)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["C"], map[string]any{"flashcards": "current"})
	insertOtherWorkflowRunTaskOutput(t, pool, fixture.workflowID, fixture.taskIDs["B"], unrelatedOutput)

	if _, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}

	inputs := taskRunInputsByTaskName(t, pool, fixture.workflowRunID)
	predecessors, ok := inputs["D"]["predecessors"].(map[string]any)
	if !ok {
		t.Fatalf("expected predecessor map, got %#v", inputs["D"]["predecessors"])
	}
	if !reflect.DeepEqual(predecessors["B"], currentOutput) {
		t.Fatalf("expected current run output, got %#v", predecessors["B"])
	}
	if reflect.DeepEqual(predecessors["B"], unrelatedOutput) {
		t.Fatalf("included unrelated output: %#v", predecessors["B"])
	}
}

func TestPostgresRepositoryQueueRunnableTaskRunsDoesNotRewriteQueuedSuccessorInput(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableDAG(t, pool)
	repo := NewPostgresRepository(pool)
	workflowInput := map[string]any{"document_text": "lecture notes"}
	firstOutput := map[string]any{"clean_text": "first"}
	secondOutput := map[string]any{"clean_text": "second"}
	setWorkflowRunInput(t, pool, fixture.workflowRunID, workflowInput)
	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["A"], firstOutput)

	if _, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID); err != nil {
		t.Fatalf("queue runnable task runs: %v", err)
	}
	before := taskRunInputsByTaskName(t, pool, fixture.workflowRunID)["B"]

	setTaskRunOutput(t, pool, fixture.workflowRunID, fixture.taskIDs["A"], secondOutput)
	queuedAgain, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err != nil {
		t.Fatalf("queue runnable task runs again: %v", err)
	}
	if len(queuedAgain) != 0 {
		t.Fatalf("expected no duplicate queueing, got %#v", queuedAgain)
	}
	after := taskRunInputsByTaskName(t, pool, fixture.workflowRunID)["B"]
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("expected queued input to stay unchanged:\n before %#v\n after  %#v", before, after)
	}
}

func TestPostgresRepositoryQueueRunnableTaskRunsConcurrentCallsQueueEachTaskOnce(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableRoots(t, pool, 2)
	repo := NewPostgresRepository(pool)

	results := make(chan []TaskRun, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queued, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
			if err != nil {
				errs <- err
				return
			}
			results <- queued
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected queue error: %v", err)
		}
	}

	seen := map[string]bool{}
	for queued := range results {
		for _, taskRun := range queued {
			if seen[taskRun.ID] {
				t.Fatalf("task run queued twice: %s", taskRun.ID)
			}
			seen[taskRun.ID] = true
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected two unique queued task runs, got %#v", seen)
	}
	assertWorkflowRunOutboxEventCount(t, pool, fixture.workflowRunID, 2)
}

func TestPostgresRepositoryQueueRunnableTaskRunsRollsBackWhenOutboxInsertFails(t *testing.T) {
	pool := workflowClaimTestPool(t)
	fixture := seedRunnableRoots(t, pool, 1)
	repo := NewPostgresRepository(pool)

	var taskID string
	for _, id := range fixture.taskIDs {
		taskID = id
	}
	taskRunID := taskRunIDByTask(t, pool, fixture.workflowRunID, taskID)
	insertPendingTaskOutboxEvent(t, pool, fixture.workflowID, fixture.workflowRunID, taskID, taskRunID)

	_, err := repo.QueueRunnableTaskRuns(context.Background(), fixture.workflowRunID)
	if err == nil {
		t.Fatal("expected outbox insert error")
	}

	assertTaskRunStatuses(t, pool, fixture.workflowRunID, map[string]TaskRunStatus{
		taskID: TaskRunStatusPending,
	})
	assertTaskOutboxEventCount(t, pool, taskRunID, 1)
}

type runnableTaskRunsFixture struct {
	workflowName  string
	workflowID    string
	workflowRunID string
	doneTaskID    string
	readyTaskID   string
	blockedTaskID string
}

func seedRunnableTaskRuns(t *testing.T, pool *pgxpool.Pool) runnableTaskRunsFixture {
	t.Helper()

	ctx := context.Background()
	fixture := runnableTaskRunsFixture{
		workflowName:  fmt.Sprintf("day9-queue-runnable-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		doneTaskID:    uuid.NewString(),
		readyTaskID:   uuid.NewString(),
		blockedTaskID: uuid.NewString(),
	}

	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, fixture.workflowID, fixture.workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	for _, task := range []struct {
		id   string
		name string
	}{
		{fixture.doneTaskID, "done"},
		{fixture.readyTaskID, "ready"},
		{fixture.blockedTaskID, "blocked"},
	} {
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, workflow_id, name, executor_type)
			VALUES ($1, $2, $3, $4)
		`, task.id, fixture.workflowID, task.name, "log")
		if err != nil {
			t.Fatalf("insert task %s: %v", task.name, err)
		}
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id)
		VALUES ($1, $2, $3), ($1, $3, $4)
	`, fixture.workflowID, fixture.doneTaskID, fixture.readyTaskID, fixture.blockedTaskID)
	if err != nil {
		t.Fatalf("insert dependencies: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, fixture.workflowRunID, fixture.workflowID, WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	for _, taskRun := range []struct {
		taskID string
		status TaskRunStatus
	}{
		{fixture.doneTaskID, TaskRunStatusCompleted},
		{fixture.readyTaskID, TaskRunStatusPending},
		{fixture.blockedTaskID, TaskRunStatusPending},
	} {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), fixture.workflowID, fixture.workflowRunID, taskRun.taskID, taskRun.status)
		if err != nil {
			t.Fatalf("insert task run: %v", err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, fixture.workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, fixture.workflowName)
	})

	return fixture
}

func taskRunStatusesByTask(t *testing.T, pool *pgxpool.Pool, workflowRunID string) map[string]TaskRunStatus {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT task_id, status
		FROM task_runs
		WHERE workflow_run_id = $1
	`, workflowRunID)
	if err != nil {
		t.Fatalf("query task run statuses: %v", err)
	}
	defer rows.Close()

	statuses := map[string]TaskRunStatus{}
	for rows.Next() {
		var taskID string
		var status TaskRunStatus
		if err := rows.Scan(&taskID, &status); err != nil {
			t.Fatalf("scan task run status: %v", err)
		}
		statuses[taskID] = status
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate task run statuses: %v", err)
	}

	return statuses
}

type runnableDAGFixture struct {
	workflowName  string
	workflowID    string
	workflowRunID string
	taskIDs       map[string]string
}

func seedRunnableDAG(t *testing.T, pool *pgxpool.Pool) runnableDAGFixture {
	t.Helper()

	taskIDs := map[string]string{
		"A": uuid.NewString(),
		"B": uuid.NewString(),
		"C": uuid.NewString(),
		"D": uuid.NewString(),
	}
	fixture := runnableDAGFixture{
		workflowName:  fmt.Sprintf("day9-dag-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskIDs:       taskIDs,
	}
	seedWorkflowRunGraph(t, pool, fixture.workflowName, fixture.workflowID, fixture.workflowRunID, taskIDs, [][2]string{
		{"A", "B"},
		{"A", "C"},
		{"B", "D"},
		{"C", "D"},
	}, map[string]TaskRunStatus{
		"A": TaskRunStatusCompleted,
		"B": TaskRunStatusPending,
		"C": TaskRunStatusPending,
		"D": TaskRunStatusPending,
	})
	return fixture
}

func seedRunnableRoots(t *testing.T, pool *pgxpool.Pool, count int) runnableDAGFixture {
	t.Helper()

	taskIDs := map[string]string{}
	statuses := map[string]TaskRunStatus{}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("task-%d", i)
		taskIDs[name] = uuid.NewString()
		statuses[name] = TaskRunStatusPending
	}
	fixture := runnableDAGFixture{
		workflowName:  fmt.Sprintf("day9-roots-%d", time.Now().UnixNano()),
		workflowID:    uuid.NewString(),
		workflowRunID: uuid.NewString(),
		taskIDs:       taskIDs,
	}
	seedWorkflowRunGraph(t, pool, fixture.workflowName, fixture.workflowID, fixture.workflowRunID, taskIDs, nil, statuses)
	return fixture
}

func seedWorkflowRunGraph(
	t *testing.T,
	pool *pgxpool.Pool,
	workflowName string,
	workflowID string,
	workflowRunID string,
	taskIDs map[string]string,
	dependencies [][2]string,
	statuses map[string]TaskRunStatus,
) {
	t.Helper()

	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO workflows (id, name) VALUES ($1, $2)`, workflowID, workflowName)
	if err != nil {
		t.Fatalf("insert workflow: %v", err)
	}
	for name, taskID := range taskIDs {
		_, err = pool.Exec(ctx, `
			INSERT INTO tasks (id, workflow_id, name, executor_type)
			VALUES ($1, $2, $3, $4)
		`, taskID, workflowID, name, "log")
		if err != nil {
			t.Fatalf("insert task %s: %v", name, err)
		}
	}
	for _, dependency := range dependencies {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_dependencies (workflow_id, predecessor_task_id, successor_task_id)
			VALUES ($1, $2, $3)
		`, workflowID, taskIDs[dependency[0]], taskIDs[dependency[1]])
		if err != nil {
			t.Fatalf("insert dependency %s->%s: %v", dependency[0], dependency[1], err)
		}
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, workflowRunID, workflowID, WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert workflow run: %v", err)
	}
	for name, taskID := range taskIDs {
		_, err = pool.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status)
			VALUES ($1, $2, $3, $4, $5)
		`, uuid.NewString(), workflowID, workflowRunID, taskID, statuses[name])
		if err != nil {
			t.Fatalf("insert task run %s: %v", name, err)
		}
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, workflowName)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, workflowName)
	})
}

func taskIDs(taskRuns []TaskRun) []string {
	ids := make([]string, 0, len(taskRuns))
	for _, taskRun := range taskRuns {
		ids = append(ids, taskRun.TaskID)
	}
	sort.Strings(ids)
	return ids
}

func assertTaskRunStatuses(t *testing.T, pool *pgxpool.Pool, workflowRunID string, want map[string]TaskRunStatus) {
	t.Helper()

	if got := taskRunStatusesByTask(t, pool, workflowRunID); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected task run statuses: got %#v, want %#v", got, want)
	}
}

func setTaskRunStatus(t *testing.T, pool *pgxpool.Pool, workflowRunID, taskID string, status TaskRunStatus) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		UPDATE task_runs
		SET status = $3
		WHERE workflow_run_id = $1
			AND task_id = $2
	`, workflowRunID, taskID, status)
	if err != nil {
		t.Fatalf("set task run status: %v", err)
	}
}

func setWorkflowRunInput(t *testing.T, pool *pgxpool.Pool, workflowRunID string, input map[string]any) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		UPDATE workflow_runs
		SET input = $2
		WHERE id = $1
	`, workflowRunID, input)
	if err != nil {
		t.Fatalf("set workflow run input: %v", err)
	}
}

func setTaskRunOutput(t *testing.T, pool *pgxpool.Pool, workflowRunID, taskID string, output map[string]any) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		UPDATE task_runs
		SET output = $3
		WHERE workflow_run_id = $1
			AND task_id = $2
	`, workflowRunID, taskID, output)
	if err != nil {
		t.Fatalf("set task run output: %v", err)
	}
}

func insertOtherWorkflowRunTaskOutput(t *testing.T, pool *pgxpool.Pool, workflowID, taskID string, output map[string]any) {
	t.Helper()

	workflowRunID := uuid.NewString()
	taskRunID := uuid.NewString()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO workflow_runs (id, workflow_id, status)
		VALUES ($1, $2, $3)
	`, workflowRunID, workflowID, WorkflowRunStatusRunning)
	if err != nil {
		t.Fatalf("insert other workflow run: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
		INSERT INTO task_runs (id, workflow_id, workflow_run_id, task_id, status, output)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, taskRunID, workflowID, workflowRunID, taskID, TaskRunStatusCompleted, output)
	if err != nil {
		t.Fatalf("insert other task run output: %v", err)
	}
}

func assertSuccessorInput(t *testing.T, got, wantWorkflowInput map[string]any, wantPredecessors map[string]any) {
	t.Helper()

	if !reflect.DeepEqual(got["workflow_input"], wantWorkflowInput) {
		t.Fatalf("unexpected workflow input: got %#v, want %#v", got["workflow_input"], wantWorkflowInput)
	}
	if !reflect.DeepEqual(got["predecessors"], wantPredecessors) {
		t.Fatalf("unexpected predecessor input: got %#v, want %#v", got["predecessors"], wantPredecessors)
	}
}

func assertTaskOutboxEvent(t *testing.T, pool *pgxpool.Pool, taskRun TaskRun) {
	t.Helper()

	var workflowID, workflowRunID, taskID, status, eventType string
	err := pool.QueryRow(context.Background(), `
		SELECT workflow_id, workflow_run_id, task_id, status, event_type
		FROM task_outbox_events
		WHERE task_run_id = $1
	`, taskRun.ID).Scan(&workflowID, &workflowRunID, &taskID, &status, &eventType)
	if err != nil {
		t.Fatalf("load task outbox event: %v", err)
	}

	if workflowID != taskRun.WorkflowID || workflowRunID != taskRun.WorkflowRunID || taskID != taskRun.TaskID {
		t.Fatalf("outbox event does not match task run: got workflow=%s run=%s task=%s, want %#v", workflowID, workflowRunID, taskID, taskRun)
	}
	if status != "pending" || eventType != "task_run_queued" {
		t.Fatalf("unexpected outbox event state: status=%q event_type=%q", status, eventType)
	}
}

func assertTaskOutboxEventCount(t *testing.T, pool *pgxpool.Pool, taskRunID string, want int) {
	t.Helper()

	var got int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_outbox_events
		WHERE task_run_id = $1
	`, taskRunID).Scan(&got)
	if err != nil {
		t.Fatalf("count task outbox events: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d task outbox events, got %d", want, got)
	}
}

func assertWorkflowRunOutboxEventCount(t *testing.T, pool *pgxpool.Pool, workflowRunID string, want int) {
	t.Helper()

	var got int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_outbox_events
		WHERE workflow_run_id = $1
	`, workflowRunID).Scan(&got)
	if err != nil {
		t.Fatalf("count workflow run outbox events: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d workflow-run outbox events, got %d", want, got)
	}
}

func taskRunIDByTask(t *testing.T, pool *pgxpool.Pool, workflowRunID, taskID string) string {
	t.Helper()

	var taskRunID string
	err := pool.QueryRow(context.Background(), `
		SELECT id
		FROM task_runs
		WHERE workflow_run_id = $1
			AND task_id = $2
	`, workflowRunID, taskID).Scan(&taskRunID)
	if err != nil {
		t.Fatalf("load task run id: %v", err)
	}
	return taskRunID
}

func insertPendingTaskOutboxEvent(t *testing.T, pool *pgxpool.Pool, workflowID, workflowRunID, taskID, taskRunID string) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.NewString(), workflowID, workflowRunID, taskID, taskRunID)
	if err != nil {
		t.Fatalf("insert pending task outbox event: %v", err)
	}
}
