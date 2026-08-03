package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/scheduler"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkflowAPITestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	workflowAPIMigrationPath          = "../../migrations/001_initial_schema.up.sql"
	workflowAPIIdempotencyPath        = "../../migrations/002_workflow_run_idempotency.up.sql"
	workflowAPIOutboxPath             = "../../migrations/003_task_outbox_events.up.sql"
)

var (
	workflowAPIPoolOnce sync.Once
	workflowAPIShared   *pgxpool.Pool
	workflowAPIPoolErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if workflowAPIShared != nil {
		workflowAPIShared.Close()
	}
	os.Exit(code)
}

func workflowAPITestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	workflowAPIPoolOnce.Do(func() {
		workflowAPIShared, workflowAPIPoolErr = setupWorkflowAPITestDatabase(context.Background())
	})

	if workflowAPIPoolErr != nil {
		t.Skipf("postgres not available for Day 3 API tests (run `make postgres-up`): %v", workflowAPIPoolErr)
	}

	return workflowAPIShared
}

func setupWorkflowAPITestDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultWorkflowAPITestDatabaseURL
	}

	pool, err := database.Connect(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	var schemaApplied bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'workflows'
		)
	`).Scan(&schemaApplied)
	if err != nil {
		pool.Close()
		return nil, err
	}

	if !schemaApplied {
		migrationSQL, err := os.ReadFile(workflowAPIMigrationPath)
		if err != nil {
			pool.Close()
			return nil, err
		}
		if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
			pool.Close()
			return nil, err
		}
	}
	if err := ensureWorkflowAPIIdempotencySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureWorkflowAPIOutboxSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func ensureWorkflowAPIIdempotencySchema(ctx context.Context, pool *pgxpool.Pool) error {
	var columnExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
				AND table_name = 'workflow_runs'
				AND column_name = 'idempotency_key'
		)
	`).Scan(&columnExists)
	if err != nil {
		return err
	}
	if !columnExists {
		migrationSQL, err := os.ReadFile(workflowAPIIdempotencyPath)
		if err != nil {
			return err
		}
		_, err = pool.Exec(ctx, string(migrationSQL))
		return err
	}

	if _, err := pool.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_runs_idempotency
			ON workflow_runs (workflow_id, idempotency_key)
			WHERE idempotency_key IS NOT NULL
	`); err != nil {
		return err
	}

	var constraintExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = 'chk_workflow_runs_idempotency_hash'
		)
	`).Scan(&constraintExists)
	if err != nil || constraintExists {
		return err
	}
	_, err = pool.Exec(ctx, `
		ALTER TABLE workflow_runs
			ADD CONSTRAINT chk_workflow_runs_idempotency_hash
			CHECK (idempotency_key IS NULL OR request_hash IS NOT NULL)
	`)
	return err
}

func ensureWorkflowAPIOutboxSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var tableExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public'
				AND table_name = 'task_outbox_events'
		)
	`).Scan(&tableExists)
	if err != nil {
		return err
	}
	if !tableExists {
		migrationSQL, err := os.ReadFile(workflowAPIOutboxPath)
		if err != nil {
			return err
		}
		_, err = pool.Exec(ctx, string(migrationSQL))
		return err
	}

	_, err = pool.Exec(ctx, `
		ALTER TABLE task_outbox_events
			DROP CONSTRAINT IF EXISTS chk_task_outbox_events_status;
		ALTER TABLE task_outbox_events
			ADD CONSTRAINT chk_task_outbox_events_status
			CHECK (status IN ('pending', 'publishing', 'published'));
		DROP INDEX IF EXISTS uq_task_outbox_events_unpublished_task_run;
		CREATE UNIQUE INDEX uq_task_outbox_events_unpublished_task_run
			ON task_outbox_events (task_run_id, event_type)
			WHERE status <> 'published';
	`)
	return err
}

func workflowAPIHandler(t *testing.T) (*pgxpool.Pool, http.Handler) {
	t.Helper()

	pool := workflowAPITestPool(t)
	return pool, New(pool).Handler()
}

func workflowAPIHandlerWithScheduler(t *testing.T, scheduler *schedulerRecorder) (*pgxpool.Pool, http.Handler) {
	t.Helper()

	pool := workflowAPITestPool(t)
	return pool, New(pool, scheduler).Handler()
}

type schedulerRecorder struct {
	workflowRunIDs []string
}

func (s *schedulerRecorder) QueueRunnableTaskRuns(_ context.Context, workflowRunID string) error {
	s.workflowRunIDs = append(s.workflowRunIDs, workflowRunID)
	return nil
}

func uniqueWorkflowName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func cleanupWorkflowByName(t *testing.T, pool *pgxpool.Pool, name string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.Background()
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
		_, _ = pool.Exec(ctx, `DELETE FROM task_outbox_events WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, name)
	})
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, requestID string) *httptest.ResponseRecorder {
	t.Helper()

	return postJSONWithHeaders(t, handler, path, body, requestID, nil)
}

func postJSONWithHeaders(t *testing.T, handler http.Handler, path string, body any, requestID string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		request.Header.Set("X-Request-ID", requestID)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func postRaw(t *testing.T, handler http.Handler, path string, body string, requestID string) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		request.Header.Set("X-Request-ID", requestID)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeJSONBody(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return body
}

func expectStatus(t *testing.T, response *httptest.ResponseRecorder, want int) {
	t.Helper()

	if response.Code != want {
		t.Fatalf("expected status %d, got %d with body %q", want, response.Code, response.Body.String())
	}
}

func expectJSONResponse(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("expected application/json content type, got %q", contentType)
	}
}

func expectRequestIDHeader(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()

	got := response.Header().Get("X-Request-ID")
	if got == "" {
		t.Fatal("expected X-Request-ID response header")
	}
	if want != "" && got != want {
		t.Fatalf("expected X-Request-ID %q, got %q", want, got)
	}
}

func expectStringField(t *testing.T, body map[string]any, field string) string {
	t.Helper()

	value, ok := body[field].(string)
	if !ok || value == "" {
		t.Fatalf("expected non-empty string field %q, got %#v", field, body[field])
	}
	return value
}

func expectFieldEquals(t *testing.T, body map[string]any, field, want string) {
	t.Helper()

	if got := expectStringField(t, body, field); got != want {
		t.Fatalf("expected %s %q, got %q", field, want, got)
	}
}

func createWorkflowThroughAPI(t *testing.T, handler http.Handler, name string) string {
	t.Helper()

	response := postJSON(t, handler, "/workflows", map[string]any{
		"name":          name,
		"input_schema":  map[string]any{"type": "object"},
		"output_schema": map[string]any{"type": "object"},
	}, "")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "name", name)
	return expectStringField(t, body, "id")
}

func createTaskThroughAPI(t *testing.T, handler http.Handler, workflowID, name string) string {
	t.Helper()

	response := postJSON(t, handler, "/workflows/"+workflowID+"/tasks", map[string]any{
		"name":          name,
		"executor_type": "http",
		"config":        map[string]any{"url": "https://example.test/" + name},
		"input_schema":  map[string]any{"type": "object"},
		"output_schema": map[string]any{"type": "object"},
	}, "")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "workflow_id", workflowID)
	expectFieldEquals(t, body, "name", name)
	expectFieldEquals(t, body, "executor_type", "http")
	return expectStringField(t, body, "id")
}

func createWorkflowRunThroughAPI(t *testing.T, handler http.Handler, workflowID string, input map[string]any) string {
	t.Helper()

	response := postJSON(t, handler, "/workflows/"+workflowID+"/runs", map[string]any{
		"input": input,
	}, "")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "workflow_id", workflowID)
	expectFieldEquals(t, body, "status", "pending")
	return expectStringField(t, body, "id")
}

func TestWorkflowDefinitionAPIContract(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	response := postJSON(t, handler, "/workflows", map[string]any{
		"name":          workflowName,
		"input_schema":  map[string]any{"type": "object"},
		"output_schema": map[string]any{"type": "object"},
	}, "workflow-create-request")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "workflow-create-request")

	body := decodeJSONBody(t, response)
	workflowID := expectStringField(t, body, "id")
	expectFieldEquals(t, body, "name", workflowName)

	if location := response.Header().Get("Location"); location != "/workflows/"+workflowID {
		t.Fatalf("expected Location /workflows/%s, got %q", workflowID, location)
	}
}

func TestWorkflowDefinitionAPIValidation(t *testing.T) {
	_, handler := workflowAPIHandler(t)

	t.Run("malformed JSON", func(t *testing.T) {
		response := postRaw(t, handler, "/workflows", `{"name":`, "malformed-workflow-request")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)
		expectRequestIDHeader(t, response, "malformed-workflow-request")

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "malformed_json")
		expectFieldEquals(t, body, "request_id", "malformed-workflow-request")
	})

	t.Run("missing name", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows", map[string]any{
			"input_schema": map[string]any{"type": "object"},
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "validation_error")
	})
}

func TestTaskDefinitionAPIContract(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-task-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)

	response := postJSON(t, handler, "/workflows/"+workflowID+"/tasks", map[string]any{
		"name":          "extract",
		"executor_type": "http",
		"config":        map[string]any{"url": "https://example.test/extract"},
		"input_schema":  map[string]any{"type": "object"},
		"output_schema": map[string]any{"type": "object"},
	}, "task-create-request")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "task-create-request")

	body := decodeJSONBody(t, response)
	taskID := expectStringField(t, body, "id")
	expectFieldEquals(t, body, "workflow_id", workflowID)
	expectFieldEquals(t, body, "name", "extract")
	expectFieldEquals(t, body, "executor_type", "http")

	if location := response.Header().Get("Location"); location != "/workflows/"+workflowID+"/tasks/"+taskID {
		t.Fatalf("expected task Location for %s, got %q", taskID, location)
	}
}

func TestTaskDefinitionAPIValidation(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-task-validation")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)

	t.Run("malformed JSON", func(t *testing.T) {
		response := postRaw(t, handler, "/workflows/"+workflowID+"/tasks", `{"name":`, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "malformed_json")
	})

	t.Run("missing executor type", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/"+workflowID+"/tasks", map[string]any{
			"name": "extract",
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "validation_error")
	})

	t.Run("unknown workflow", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/00000000-0000-0000-0000-000000000999/tasks", map[string]any{
			"name":          "extract",
			"executor_type": "http",
		}, "")
		expectStatus(t, response, http.StatusNotFound)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "workflow_not_found")
	})

	t.Run("invalid workflow id", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/not-a-uuid/tasks", map[string]any{
			"name":          "extract",
			"executor_type": "http",
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "invalid_uuid")
	})
}

func TestTaskDefinitionAPIDuplicateName(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-duplicate-task")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	createTaskThroughAPI(t, handler, workflowID, "extract")

	response := postJSON(t, handler, "/workflows/"+workflowID+"/tasks", map[string]any{
		"name":          "extract",
		"executor_type": "http",
	}, "")
	expectStatus(t, response, http.StatusConflict)
	expectJSONResponse(t, response)

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "error", "duplicate_task_name")
}

func TestDependencyAPIContract(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-dependency-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	extractID := createTaskThroughAPI(t, handler, workflowID, "extract")
	transformID := createTaskThroughAPI(t, handler, workflowID, "transform")

	response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
		"predecessor_task_id": extractID,
		"successor_task_id":   transformID,
	}, "dependency-create-request")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "dependency-create-request")

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "workflow_id", workflowID)
	expectFieldEquals(t, body, "predecessor_task_id", extractID)
	expectFieldEquals(t, body, "successor_task_id", transformID)
}

func TestDependencyAPIValidation(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-dependency-validation")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	extractID := createTaskThroughAPI(t, handler, workflowID, "extract")

	t.Run("malformed JSON", func(t *testing.T) {
		response := postRaw(t, handler, "/workflows/"+workflowID+"/dependencies", `{"predecessor_task_id":`, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "malformed_json")
	})

	t.Run("self dependency", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
			"predecessor_task_id": extractID,
			"successor_task_id":   extractID,
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "self_dependency")
	})

	t.Run("invalid task reference", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
			"predecessor_task_id": extractID,
			"successor_task_id":   "00000000-0000-0000-0000-000000000998",
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "invalid_task_reference")
	})

	t.Run("invalid workflow id", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/not-a-uuid/dependencies", map[string]any{
			"predecessor_task_id": extractID,
			"successor_task_id":   "00000000-0000-0000-0000-000000000998",
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "invalid_uuid")
	})

	t.Run("invalid task id", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
			"predecessor_task_id": extractID,
			"successor_task_id":   "not-a-uuid",
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "invalid_uuid")
	})
}

func TestDependencyAPIDuplicateDependency(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-duplicate-dependency")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	extractID := createTaskThroughAPI(t, handler, workflowID, "extract")
	transformID := createTaskThroughAPI(t, handler, workflowID, "transform")

	body := map[string]any{
		"predecessor_task_id": extractID,
		"successor_task_id":   transformID,
	}
	response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", body, "")
	expectStatus(t, response, http.StatusCreated)

	response = postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", body, "")
	expectStatus(t, response, http.StatusConflict)
	expectJSONResponse(t, response)

	responseBody := decodeJSONBody(t, response)
	expectFieldEquals(t, responseBody, "error", "duplicate_dependency")
}

func TestDependencyAPICycleValidation(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day5-dependency-cycle")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	extractID := createTaskThroughAPI(t, handler, workflowID, "extract")
	transformID := createTaskThroughAPI(t, handler, workflowID, "transform")
	loadID := createTaskThroughAPI(t, handler, workflowID, "load")

	response := postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
		"predecessor_task_id": extractID,
		"successor_task_id":   transformID,
	}, "")
	expectStatus(t, response, http.StatusCreated)

	response = postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
		"predecessor_task_id": transformID,
		"successor_task_id":   extractID,
	}, "dependency-cycle-request")
	expectStatus(t, response, http.StatusBadRequest)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "dependency-cycle-request")

	body := decodeJSONBody(t, response)
	expectFieldEquals(t, body, "error", "dependency_cycle")
	expectFieldEquals(t, body, "request_id", "dependency-cycle-request")

	response = postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
		"predecessor_task_id": transformID,
		"successor_task_id":   loadID,
	}, "")
	expectStatus(t, response, http.StatusCreated)

	response = postJSON(t, handler, "/workflows/"+workflowID+"/dependencies", map[string]any{
		"predecessor_task_id": loadID,
		"successor_task_id":   extractID,
	}, "dependency-long-cycle-request")
	expectStatus(t, response, http.StatusBadRequest)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "dependency-long-cycle-request")

	body = decodeJSONBody(t, response)
	expectFieldEquals(t, body, "error", "dependency_cycle")
	expectFieldEquals(t, body, "request_id", "dependency-long-cycle-request")
}

func TestWorkflowRunAPIContract(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-run-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	createTaskThroughAPI(t, handler, workflowID, "extract")

	response := postJSON(t, handler, "/workflows/"+workflowID+"/runs", map[string]any{
		"input": map[string]any{"document_id": "doc-123"},
	}, "workflow-run-create-request")

	expectStatus(t, response, http.StatusCreated)
	expectJSONResponse(t, response)
	expectRequestIDHeader(t, response, "workflow-run-create-request")

	body := decodeJSONBody(t, response)
	runID := expectStringField(t, body, "id")
	expectFieldEquals(t, body, "workflow_id", workflowID)
	expectFieldEquals(t, body, "status", "pending")

	if location := response.Header().Get("Location"); location != "/workflows/"+workflowID+"/runs/"+runID {
		t.Fatalf("expected workflow run Location for %s, got %q", runID, location)
	}
}

func TestWorkflowRunIdempotencyKeySemantics(t *testing.T) {
	t.Run("missing key keeps creating runs", func(t *testing.T) {
		pool, handler := workflowAPIHandler(t)

		workflowName := uniqueWorkflowName("day10-run-no-key")
		cleanupWorkflowByName(t, pool, workflowName)

		workflowID := createWorkflowThroughAPI(t, handler, workflowName)
		createTaskThroughAPI(t, handler, workflowID, "extract")
		body := map[string]any{"input": map[string]any{"document_id": "doc-123"}}

		first := postJSON(t, handler, "/workflows/"+workflowID+"/runs", body, "")
		second := postJSON(t, handler, "/workflows/"+workflowID+"/runs", body, "")
		expectStatus(t, first, http.StatusCreated)
		expectStatus(t, second, http.StatusCreated)

		firstID := expectStringField(t, decodeJSONBody(t, first), "id")
		secondID := expectStringField(t, decodeJSONBody(t, second), "id")
		if firstID == secondID {
			t.Fatalf("expected missing idempotency key to create separate runs, got %s twice", firstID)
		}
	})

	t.Run("same key and same request returns original run", func(t *testing.T) {
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })

		pool, handler := workflowAPIHandler(t)

		workflowName := uniqueWorkflowName("day10-run-same-key")
		cleanupWorkflowByName(t, pool, workflowName)

		workflowID := createWorkflowThroughAPI(t, handler, workflowName)
		createTaskThroughAPI(t, handler, workflowID, "extract")
		path := "/workflows/" + workflowID + "/runs"
		body := map[string]any{"input": map[string]any{"document_id": "doc-123"}}

		first := postJSONWithHeaders(t, handler, path, body, "", map[string]string{"Idempotency-Key": " day10-key "})
		second := postJSONWithHeaders(t, handler, path, body, "", map[string]string{"Idempotency-Key": "day10-key"})
		expectStatus(t, first, http.StatusCreated)
		expectStatus(t, second, http.StatusCreated)

		firstID := expectStringField(t, decodeJSONBody(t, first), "id")
		secondID := expectStringField(t, decodeJSONBody(t, second), "id")
		if firstID != secondID {
			t.Fatalf("expected idempotent replay to return %s, got %s", firstID, secondID)
		}
		if location := second.Header().Get("Location"); location != path+"/"+firstID {
			t.Fatalf("expected replay Location %q, got %q", path+"/"+firstID, location)
		}
		expectWorkflowRunCount(t, pool, workflowID, 1)
		logOutput := logs.String()
		for _, want := range []string{
			`"msg":"idempotency_key_reused"`,
			`"workflow_id":"` + workflowID + `"`,
			`"workflow_run_id":"` + firstID + `"`,
		} {
			if !strings.Contains(logOutput, want) {
				t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
			}
		}
	})

	t.Run("same key and different request returns conflict", func(t *testing.T) {
		var logs bytes.Buffer
		previousLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		t.Cleanup(func() { slog.SetDefault(previousLogger) })

		pool, handler := workflowAPIHandler(t)

		workflowName := uniqueWorkflowName("day10-run-conflict")
		cleanupWorkflowByName(t, pool, workflowName)

		workflowID := createWorkflowThroughAPI(t, handler, workflowName)
		createTaskThroughAPI(t, handler, workflowID, "extract")
		path := "/workflows/" + workflowID + "/runs"
		headers := map[string]string{"Idempotency-Key": "day10-conflict-key"}

		first := postJSONWithHeaders(t, handler, path, map[string]any{
			"input": map[string]any{"document_id": "doc-123"},
		}, "", headers)
		expectStatus(t, first, http.StatusCreated)

		second := postJSONWithHeaders(t, handler, path, map[string]any{
			"input": map[string]any{"document_id": "doc-456"},
		}, "idempotency-conflict-request", headers)
		expectStatus(t, second, http.StatusConflict)
		expectRequestIDHeader(t, second, "idempotency-conflict-request")

		responseBody := decodeJSONBody(t, second)
		expectFieldEquals(t, responseBody, "error", "idempotency_conflict")
		expectFieldEquals(t, responseBody, "request_id", "idempotency-conflict-request")
		expectWorkflowRunCount(t, pool, workflowID, 1)
		logOutput := logs.String()
		for _, want := range []string{
			`"msg":"idempotency_key_conflict"`,
			`"request_id":"idempotency-conflict-request"`,
			`"workflow_id":"` + workflowID + `"`,
		} {
			if !strings.Contains(logOutput, want) {
				t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
			}
		}
	})
}

func TestWorkflowRunCreatesTaskRunsForEachWorkflowTask(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-task-runs-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	extractID := createTaskThroughAPI(t, handler, workflowID, "extract")
	transformID := createTaskThroughAPI(t, handler, workflowID, "transform")
	loadID := createTaskThroughAPI(t, handler, workflowID, "load")

	workflowRunID := createWorkflowRunThroughAPI(t, handler, workflowID, map[string]any{
		"document_id": "doc-task-runs",
	})

	rows, err := pool.Query(context.Background(), `
		SELECT task_id, status, attempt_count
		FROM task_runs
		WHERE workflow_id = $1 AND workflow_run_id = $2
		ORDER BY task_id
	`, workflowID, workflowRunID)
	if err != nil {
		t.Fatalf("query task runs: %v", err)
	}
	defer rows.Close()

	got := make(map[string]struct {
		status       string
		attemptCount int
	})
	for rows.Next() {
		var taskID, status string
		var attemptCount int
		if err := rows.Scan(&taskID, &status, &attemptCount); err != nil {
			t.Fatalf("scan task run: %v", err)
		}

		got[taskID] = struct {
			status       string
			attemptCount int
		}{
			status:       status,
			attemptCount: attemptCount,
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate task runs: %v", err)
	}

	wantTaskIDs := []string{extractID, transformID, loadID}
	if len(got) != len(wantTaskIDs) {
		t.Fatalf("expected %d task runs, got %d: %#v", len(wantTaskIDs), len(got), got)
	}

	for _, taskID := range wantTaskIDs {
		taskRun, ok := got[taskID]
		if !ok {
			t.Fatalf("expected task run for task %s, got %#v", taskID, got)
		}
		if taskRun.status != "pending" {
			t.Fatalf("expected task run %s status pending, got %q", taskID, taskRun.status)
		}
		if taskRun.attemptCount != 0 {
			t.Fatalf("expected task run %s attempt_count 0, got %d", taskID, taskRun.attemptCount)
		}
	}
}

func expectWorkflowRunCount(t *testing.T, pool *pgxpool.Pool, workflowID string, want int) {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM workflow_runs
		WHERE workflow_id = $1
	`, workflowID).Scan(&count); err != nil {
		t.Fatalf("count workflow runs: %v", err)
	}
	if count != want {
		t.Fatalf("expected %d workflow runs, got %d", want, count)
	}
}

func TestWorkflowRunCreationTriggersScheduler(t *testing.T) {
	scheduler := &schedulerRecorder{}
	pool, handler := workflowAPIHandlerWithScheduler(t, scheduler)

	workflowName := uniqueWorkflowName("day9-scheduler-trigger")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	createTaskThroughAPI(t, handler, workflowID, "extract")

	workflowRunID := createWorkflowRunThroughAPI(t, handler, workflowID, map[string]any{
		"document_id": "doc-scheduler-trigger",
	})

	if !reflect.DeepEqual(scheduler.workflowRunIDs, []string{workflowRunID}) {
		t.Fatalf("expected scheduler to receive workflow run %s, got %#v", workflowRunID, scheduler.workflowRunIDs)
	}
}

func TestWorkflowRunCreationReturnsCreatedWhenOutboxDispatchFailsAndLaterRecovers(t *testing.T) {
	pool := workflowAPITestPool(t)
	repo := workflow.NewPostgresRepository(pool)
	publishErr := errors.New("redis unavailable")
	failingPublisher := &workflowAPIPublisher{err: publishErr}
	handler := New(pool, scheduler.NewService(repo, failingPublisher)).Handler()

	workflowName := uniqueWorkflowName("day11-outbox-recovery")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	createTaskThroughAPI(t, handler, workflowID, "extract")

	response := postJSON(t, handler, "/workflows/"+workflowID+"/runs", map[string]any{
		"input": map[string]any{"document_id": "doc-outbox-recovery"},
	}, "")
	expectStatus(t, response, http.StatusCreated)

	body := decodeJSONBody(t, response)
	workflowRunID := expectStringField(t, body, "id")
	if len(failingPublisher.messages) != 1 {
		t.Fatalf("expected one failed publish attempt, got %#v", failingPublisher.messages)
	}
	assertTaskOutboxEventState(t, pool, workflowRunID, "pending", publishErr.Error(), "", false)

	recoveryPublisher := &workflowAPIPublisher{}
	dispatcher := scheduler.NewOutboxDispatcher(repo, recoveryPublisher)
	if err := dispatcher.DispatchPendingTaskOutboxEvents(context.Background()); err != nil {
		t.Fatalf("dispatch recovered task outbox event: %v", err)
	}

	if len(recoveryPublisher.messages) != 1 {
		t.Fatalf("expected one recovered publish, got %#v", recoveryPublisher.messages)
	}
	assertTaskOutboxEventState(t, pool, workflowRunID, "published", "", "message-id", true)
}

func TestWorkflowRunStartsWithoutTaskAttempts(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-task-attempts-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)
	createTaskThroughAPI(t, handler, workflowID, "extract")
	createTaskThroughAPI(t, handler, workflowID, "transform")

	workflowRunID := createWorkflowRunThroughAPI(t, handler, workflowID, map[string]any{
		"document_id": "doc-task-attempts",
	})

	var taskRunCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_runs
		WHERE workflow_id = $1 AND workflow_run_id = $2
	`, workflowID, workflowRunID).Scan(&taskRunCount); err != nil {
		t.Fatalf("count task runs: %v", err)
	}
	if taskRunCount != 2 {
		t.Fatalf("expected 2 task runs before checking attempts, got %d", taskRunCount)
	}

	var attemptCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM task_attempts
		WHERE task_run_id IN (
			SELECT id FROM task_runs
			WHERE workflow_id = $1 AND workflow_run_id = $2
		)
	`, workflowID, workflowRunID).Scan(&attemptCount); err != nil {
		t.Fatalf("count task attempts: %v", err)
	}
	if attemptCount != 0 {
		t.Fatalf("expected workflow run creation to create 0 task attempts, got %d", attemptCount)
	}
}

type workflowAPIPublisher struct {
	messages []queue.TaskMessage
	err      error
}

func (p *workflowAPIPublisher) PublishTask(_ context.Context, message queue.TaskMessage) (string, error) {
	p.messages = append(p.messages, message)
	if p.err != nil {
		return "", p.err
	}
	return "message-id", nil
}

func assertTaskOutboxEventState(t *testing.T, pool *pgxpool.Pool, workflowRunID, wantStatus, wantLastError, wantRedisMessageID string, wantPublished bool) {
	t.Helper()

	var status, lastError, redisMessageID string
	var published bool
	err := pool.QueryRow(context.Background(), `
		SELECT status, COALESCE(last_error, ''), COALESCE(redis_message_id, ''), published_at IS NOT NULL
		FROM task_outbox_events
		WHERE workflow_run_id = $1
	`, workflowRunID).Scan(&status, &lastError, &redisMessageID, &published)
	if err != nil {
		t.Fatalf("load task outbox event: %v", err)
	}
	if status != wantStatus || lastError != wantLastError || redisMessageID != wantRedisMessageID || published != wantPublished {
		t.Fatalf(
			"unexpected outbox state: status=%q last_error=%q redis_message_id=%q published=%v",
			status,
			lastError,
			redisMessageID,
			published,
		)
	}
}

func TestWorkflowRunAPIValidation(t *testing.T) {
	pool, handler := workflowAPIHandler(t)

	workflowName := uniqueWorkflowName("day3-empty-run-workflow")
	cleanupWorkflowByName(t, pool, workflowName)

	workflowID := createWorkflowThroughAPI(t, handler, workflowName)

	t.Run("malformed JSON", func(t *testing.T) {
		response := postRaw(t, handler, "/workflows/"+workflowID+"/runs", `{"input":`, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "malformed_json")
	})

	t.Run("empty workflow", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/"+workflowID+"/runs", map[string]any{
			"input": map[string]any{"document_id": "doc-123"},
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "empty_workflow")
	})

	t.Run("unknown workflow", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/00000000-0000-0000-0000-000000000997/runs", map[string]any{
			"input": map[string]any{"document_id": "doc-123"},
		}, "")
		expectStatus(t, response, http.StatusNotFound)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "workflow_not_found")
	})

	t.Run("invalid workflow id", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows/not-a-uuid/runs", map[string]any{
			"input": map[string]any{"document_id": "doc-123"},
		}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectJSONResponse(t, response)

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "error", "invalid_uuid")
	})
}

func TestWorkflowAPIRequestIDs(t *testing.T) {
	_, handler := workflowAPIHandler(t)

	t.Run("echoes provided request id on validation error", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows", map[string]any{}, "provided-request-id")
		expectStatus(t, response, http.StatusBadRequest)
		expectRequestIDHeader(t, response, "provided-request-id")

		body := decodeJSONBody(t, response)
		expectFieldEquals(t, body, "request_id", "provided-request-id")
	})

	t.Run("generates request id when missing", func(t *testing.T) {
		response := postJSON(t, handler, "/workflows", map[string]any{}, "")
		expectStatus(t, response, http.StatusBadRequest)
		expectRequestIDHeader(t, response, "")

		body := decodeJSONBody(t, response)
		expectStringField(t, body, "request_id")
	})
}

func TestWorkflowAPIRequestLogging(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	server := newServer(fakePinger{})
	response := postJSON(t, server.Handler(), "/workflows", map[string]any{}, "log-request-id")
	expectStatus(t, response, http.StatusBadRequest)

	logOutput := logs.String()
	for _, want := range []string{
		`"msg":"http_request"`,
		`"request_id":"log-request-id"`,
		`"method":"POST"`,
		`"path":"/workflows"`,
		`"status":400`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("expected log output to contain %s, got %s", want, logOutput)
		}
	}
}
