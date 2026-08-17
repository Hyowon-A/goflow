package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Hyowon-A/goflow/internal/recallify"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestCreateRecallifyDemoRunCreatesWorkflowAndQueuesRun(t *testing.T) {
	service := &fakeRecallifyDemoWorkflowService{}
	scheduler := &schedulerRecorder{}
	server := newServer(fakePinger{}, service)
	server.scheduler = scheduler

	request := httptest.NewRequest(http.MethodPost, "/demos/recallify/runs", strings.NewReader(`{
		"document_text":"GoFlow notes",
		"title":"GoFlow Basics",
		"level":"medium",
		"mcq_count":1,
		"callback_url":"http://localhost/callback",
		"external_request_id":"req-1",
		"recallify_url":"http://recallify.test",
		"recallify_bearer_token":"token-1"
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, response.Code, response.Body.String())
	}
	var body recallifyDemoRunResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.WorkflowID != "workflow-id" || body.WorkflowRunID != "run-id" || body.Status != "queued" || !body.Queued {
		t.Fatalf("unexpected response: %#v", body)
	}
	if len(service.createdTasks) != 6 {
		t.Fatalf("expected 6 tasks, got %d", len(service.createdTasks))
	}
	if len(service.createdDependencies) != 5 {
		t.Fatalf("expected 5 dependencies, got %d", len(service.createdDependencies))
	}
	if service.createdTasks[2].ExecutorType != recallify.ExecutorTypeGenerateMCQs || service.createdTasks[2].Config["base_url"] != "http://recallify.test" {
		t.Fatalf("unexpected generate task: %#v", service.createdTasks[2])
	}
	if service.createdTasks[2].Config["bearer_token"] != "token-1" {
		t.Fatalf("expected bearer token in generate task config, got %#v", service.createdTasks[2].Config)
	}
	if service.createdTasks[5].ExecutorType != recallify.ExecutorTypeNotifyCallback {
		t.Fatalf("expected callback task, got %#v", service.createdTasks[5])
	}
	if service.createdRun.Input["callback_url"] != "http://localhost/callback" || service.createdRun.Input["external_request_id"] != "req-1" {
		t.Fatalf("unexpected run input: %#v", service.createdRun.Input)
	}
	if !reflect.DeepEqual(scheduler.workflowRunIDs, []string{"run-id"}) {
		t.Fatalf("expected queued run-id, got %#v", scheduler.workflowRunIDs)
	}
}

func TestCreateRecallifyDemoRunRequiresRecallifyURL(t *testing.T) {
	server := newServer(fakePinger{}, &fakeRecallifyDemoWorkflowService{})

	request := httptest.NewRequest(http.MethodPost, "/demos/recallify/runs", strings.NewReader(`{
		"document_text":"GoFlow notes"
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, response.Code, response.Body.String())
	}
}

type fakeRecallifyDemoWorkflowService struct {
	createdTasks        []workflow.CreateTaskInput
	createdDependencies []workflow.CreateDependencyInput
	createdRun          workflow.CreateWorkflowRunInput
}

func (f *fakeRecallifyDemoWorkflowService) CreateWorkflow(_ context.Context, input workflow.CreateWorkflowInput) (workflow.Workflow, error) {
	return workflow.Workflow{ID: "workflow-id", Name: input.Name}, nil
}

func (f *fakeRecallifyDemoWorkflowService) CreateTask(_ context.Context, workflowID string, input workflow.CreateTaskInput) (workflow.Task, error) {
	f.createdTasks = append(f.createdTasks, input)
	return workflow.Task{
		ID:           input.Name + "-id",
		WorkflowID:   workflowID,
		Name:         input.Name,
		ExecutorType: input.ExecutorType,
		Config:       input.Config,
	}, nil
}

func (f *fakeRecallifyDemoWorkflowService) CreateDependency(_ context.Context, workflowID string, input workflow.CreateDependencyInput) (workflow.Dependency, error) {
	f.createdDependencies = append(f.createdDependencies, input)
	return workflow.Dependency{
		WorkflowID:        workflowID,
		PredecessorTaskID: input.PredecessorTaskID,
		SuccessorTaskID:   input.SuccessorTaskID,
	}, nil
}

func (f *fakeRecallifyDemoWorkflowService) CreateWorkflowRun(_ context.Context, workflowID string, input workflow.CreateWorkflowRunInput) (workflow.WorkflowRun, error) {
	f.createdRun = input
	return workflow.WorkflowRun{
		ID:         "run-id",
		WorkflowID: workflowID,
		Status:     "pending",
		Input:      input.Input,
	}, nil
}

func (f *fakeRecallifyDemoWorkflowService) GetWorkflowRun(context.Context, string, string) (workflow.WorkflowRun, error) {
	return workflow.WorkflowRun{}, workflow.ErrNotImplemented
}

func (f *fakeRecallifyDemoWorkflowService) ListTaskRuns(context.Context, string, string) ([]workflow.TaskRun, error) {
	return nil, workflow.ErrNotImplemented
}

func (f *fakeRecallifyDemoWorkflowService) ListTaskAttempts(context.Context, string, string, string) ([]workflow.TaskAttempt, error) {
	return nil, workflow.ErrNotImplemented
}
