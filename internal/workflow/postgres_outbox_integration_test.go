package workflow

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryClaimPendingTaskOutboxEventsConcurrentClaimersDoNotOverlap(t *testing.T) {
	pool := workflowClaimTestPool(t)
	seedPendingTaskOutboxEvents(t, pool, 2)
	repo := NewPostgresRepository(pool)

	results := make(chan []TaskOutboxEvent, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := repo.ClaimPendingTaskOutboxEvents(context.Background())
			if err != nil {
				errs <- err
				return
			}
			results <- events
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("claim pending task outbox events: %v", err)
		}
	}

	seen := map[string]bool{}
	for events := range results {
		for _, event := range events {
			if seen[event.ID] {
				t.Fatalf("outbox event claimed twice: %s", event.ID)
			}
			seen[event.ID] = true
			if event.Status != "publishing" || event.AttemptCount != 1 {
				t.Fatalf("unexpected claimed event state: %#v", event)
			}
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected two claimed outbox events, got %#v", seen)
	}
}

func TestPostgresRepositoryMarkTaskOutboxEventPublishedPreventsReclaim(t *testing.T) {
	pool := workflowClaimTestPool(t)
	events := seedPendingTaskOutboxEvents(t, pool, 1)
	repo := NewPostgresRepository(pool)

	claimed, err := repo.ClaimPendingTaskOutboxEvents(context.Background())
	if err != nil {
		t.Fatalf("claim pending task outbox events: %v", err)
	}
	if got := eventIDs(claimed); !reflect.DeepEqual(got, []string{events[0].ID}) {
		t.Fatalf("unexpected claimed event IDs: %#v", got)
	}

	if err := repo.MarkTaskOutboxEventPublished(context.Background(), MarkTaskOutboxEventPublishedInput{
		EventID:        events[0].ID,
		RedisMessageID: "redis-1-0",
	}); err != nil {
		t.Fatalf("mark task outbox event published: %v", err)
	}
	assertTaskOutboxEventPublished(t, pool, events[0].ID, "redis-1-0")

	claimed, err = repo.ClaimPendingTaskOutboxEvents(context.Background())
	if err != nil {
		t.Fatalf("claim pending task outbox events again: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected published outbox event to stay unclaimed, got %#v", claimed)
	}
}

func TestPostgresRepositoryRecordTaskOutboxEventFailureLeavesRowClaimable(t *testing.T) {
	pool := workflowClaimTestPool(t)
	events := seedPendingTaskOutboxEvents(t, pool, 1)
	repo := NewPostgresRepository(pool)

	claimed, err := repo.ClaimPendingTaskOutboxEvents(context.Background())
	if err != nil {
		t.Fatalf("claim pending task outbox events: %v", err)
	}
	if got := eventIDs(claimed); !reflect.DeepEqual(got, []string{events[0].ID}) {
		t.Fatalf("unexpected claimed event IDs: %#v", got)
	}

	if err := repo.RecordTaskOutboxEventFailure(context.Background(), RecordTaskOutboxEventFailureInput{
		EventID:   events[0].ID,
		LastError: "redis unavailable",
	}); err != nil {
		t.Fatalf("record task outbox event failure: %v", err)
	}
	assertTaskOutboxEventPendingWithError(t, pool, events[0].ID, "redis unavailable")

	claimed, err = repo.ClaimPendingTaskOutboxEvents(context.Background())
	if err != nil {
		t.Fatalf("claim failed task outbox event again: %v", err)
	}
	if got := eventIDs(claimed); !reflect.DeepEqual(got, []string{events[0].ID}) {
		t.Fatalf("expected failed outbox event to be claimable again, got %#v", got)
	}
	if claimed[0].AttemptCount != 2 {
		t.Fatalf("expected attempt count 2, got %d", claimed[0].AttemptCount)
	}
}

func seedPendingTaskOutboxEvents(t *testing.T, pool *pgxpool.Pool, count int) []TaskOutboxEvent {
	t.Helper()

	fixture := seedRunnableRoots(t, pool, count)
	events := make([]TaskOutboxEvent, 0, count)
	for _, taskID := range fixture.taskIDs {
		taskRunID := taskRunIDByTask(t, pool, fixture.workflowRunID, taskID)
		event := TaskOutboxEvent{
			ID:            uuid.NewString(),
			WorkflowID:    fixture.workflowID,
			WorkflowRunID: fixture.workflowRunID,
			TaskID:        taskID,
			TaskRunID:     taskRunID,
		}
		_, err := pool.Exec(context.Background(), `
			INSERT INTO task_outbox_events (id, workflow_id, workflow_run_id, task_id, task_run_id)
			VALUES ($1, $2, $3, $4, $5)
		`, event.ID, event.WorkflowID, event.WorkflowRunID, event.TaskID, event.TaskRunID)
		if err != nil {
			t.Fatalf("insert task outbox event: %v", err)
		}
		events = append(events, event)
	}
	return events
}

func eventIDs(events []TaskOutboxEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

func assertTaskOutboxEventPublished(t *testing.T, pool *pgxpool.Pool, eventID, redisMessageID string) {
	t.Helper()

	var status, gotRedisMessageID string
	var published bool
	err := pool.QueryRow(context.Background(), `
		SELECT status, redis_message_id, published_at IS NOT NULL
		FROM task_outbox_events
		WHERE id = $1
	`, eventID).Scan(&status, &gotRedisMessageID, &published)
	if err != nil {
		t.Fatalf("load task outbox event: %v", err)
	}
	if status != "published" || gotRedisMessageID != redisMessageID || !published {
		t.Fatalf("unexpected published outbox state: status=%q redis_message_id=%q published=%v", status, gotRedisMessageID, published)
	}
}

func assertTaskOutboxEventPendingWithError(t *testing.T, pool *pgxpool.Pool, eventID, lastError string) {
	t.Helper()

	var status, gotLastError string
	err := pool.QueryRow(context.Background(), `
		SELECT status, last_error
		FROM task_outbox_events
		WHERE id = $1
	`, eventID).Scan(&status, &gotLastError)
	if err != nil {
		t.Fatalf("load task outbox event: %v", err)
	}
	if status != "pending" || gotLastError != lastError {
		t.Fatalf("unexpected failed outbox state: status=%q last_error=%q", status, gotLastError)
	}
}
