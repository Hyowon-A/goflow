package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkflowAPITestDatabaseURL = "postgres://goflow:goflow@localhost:5433/goflow?sslmode=disable"
	workflowAPIMigrationPath          = "../../migrations/001_initial_schema.up.sql"
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

	return pool, nil
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
		_, _ = pool.Exec(ctx, `DELETE FROM task_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflow_runs WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM task_dependencies WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE workflow_id IN (SELECT id FROM workflows WHERE name = $1)`, name)
		_, _ = pool.Exec(ctx, `DELETE FROM workflows WHERE name = $1`, name)
	})
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, requestID string) *httptest.ResponseRecorder {
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
