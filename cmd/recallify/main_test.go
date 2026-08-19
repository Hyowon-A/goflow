package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Hyowon-A/goflow/internal/recallify"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-runs", "4",
		"-workers", "3",
		"-timeout", "5s",
		"-stream", "custom",
		"-recallify-url", "http://localhost:8080",
		"-fixture", "notes.txt",
		"-json",
		"-output", "summary.json",
		"-tag", "baseline",
	})
	if err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if cfg.runs != 4 || cfg.workers != 3 || cfg.timeout != 5*time.Second || cfg.stream != "custom" || cfg.recallifyURL != "http://localhost:8080" || cfg.fixture != "notes.txt" || !cfg.jsonOutput || cfg.output != "summary.json" || cfg.tag != "baseline" {
		t.Fatalf("unexpected config: %#v", cfg)
	}

	if _, err := parseFlags([]string{"-runs", "0"}); !errors.Is(err, errPositiveRuns) {
		t.Fatalf("expected errPositiveRuns, got %v", err)
	}
}

func TestLoadRecallifyFixture(t *testing.T) {
	text, err := loadRecallifyFixture("")
	if err != nil {
		t.Fatalf("load default fixture: %v", err)
	}
	if !strings.Contains(text, "Go") {
		t.Fatalf("unexpected default fixture: %q", text)
	}

	path := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(path, []byte("custom notes"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	text, err = loadRecallifyFixture(path)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	if text != "custom notes" {
		t.Fatalf("unexpected fixture text: %q", text)
	}

	emptyPath := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(emptyPath, []byte(" \n\t "), 0o600); err != nil {
		t.Fatalf("write empty fixture: %v", err)
	}
	if _, err := loadRecallifyFixture(emptyPath); !errors.Is(err, errEmptyFixture) {
		t.Fatalf("expected errEmptyFixture, got %v", err)
	}
}

func TestNewExecutorRegistryResolvesRecallifyExecutors(t *testing.T) {
	registry := newExecutorRegistry()

	for _, executorType := range []string{
		recallify.ExecutorTypeValidateRequest,
		recallify.ExecutorTypeCleanText,
		recallify.ExecutorTypeGenerateMCQs,
		recallify.ExecutorTypeValidateMCQs,
		recallify.ExecutorTypeMergeStudySet,
		recallify.ExecutorTypeNotifyCallback,
	} {
		if _, err := registry.Resolve(executorType); err != nil {
			t.Fatalf("resolve %s: %v", executorType, err)
		}
	}
}

func TestStartFakeRecallifyServerReturnsMCQs(t *testing.T) {
	server := startFakeRecallifyServer()
	defer server.Close()

	raw, err := (recallify.RecallifyClient{BaseURL: server.URL}).GenerateMCQs(context.Background(), "notes", 1, "medium")
	if err != nil {
		t.Fatalf("generate fake MCQs: %v", err)
	}
	if _, err := recallify.ValidateRecallifyMCQs(raw, 1); err != nil {
		t.Fatalf("validate fake MCQs: %v", err)
	}
}

func TestCreateRecallifyWorkflowUsesWorkflowService(t *testing.T) {
	fake := &fakeWorkflowCreator{}

	created, tasks, err := createRecallifyWorkflow(context.Background(), fake, "  ", "http://recallify.test")
	if err != nil {
		t.Fatalf("create recallify workflow: %v", err)
	}

	if created.Name != defaultWorkflowName {
		t.Fatalf("expected default workflow name %q, got %q", defaultWorkflowName, created.Name)
	}
	if len(tasks) != 6 || len(fake.createdTasks) != 6 {
		t.Fatalf("expected 6 tasks, got map=%d recorded=%d", len(tasks), len(fake.createdTasks))
	}
	if len(fake.createdDependencies) != 5 {
		t.Fatalf("expected 5 dependencies, got %d", len(fake.createdDependencies))
	}
	if got := fake.createdTasks[2].Config["base_url"]; got != "http://recallify.test" {
		t.Fatalf("unexpected generate_mcqs base_url: %#v", got)
	}
}

func TestCreateRecallifyWorkflowRunsUsesFixtureInput(t *testing.T) {
	fake := &fakeWorkflowCreator{}
	queuer := &fakeTaskRunQueuer{}

	runIDs, err := createRecallifyWorkflowRuns(context.Background(), fake, "workflow-id", 2, "fixture text", queuer)
	if err != nil {
		t.Fatalf("create recallify workflow runs: %v", err)
	}
	if !reflect.DeepEqual(runIDs, []string{"run-1", "run-2"}) {
		t.Fatalf("unexpected run ids: %#v", runIDs)
	}
	if len(fake.createdRuns) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(fake.createdRuns))
	}
	if got := fake.createdRuns[0].Input["document_text"]; got != "fixture text" {
		t.Fatalf("unexpected fixture input: %#v", got)
	}
	if got := fake.createdRuns[1].Input["external_request_id"]; got != "recallify-local-2" {
		t.Fatalf("unexpected external_request_id: %#v", got)
	}
	if !reflect.DeepEqual(queuer.queuedRunIDs, []string{"run-1", "run-2"}) {
		t.Fatalf("unexpected queued run ids: %#v", queuer.queuedRunIDs)
	}
}

func TestCheckRecallifyInvariants(t *testing.T) {
	summary := recallifySummary{
		WorkflowRunsStarted:   2,
		WorkflowRunsCompleted: 2,
		MCQValidationPasses:   2,
		TaskAttempts:          10,
	}
	if err := checkRecallifyInvariants(summary, 2); err != nil {
		t.Fatalf("check invariants: %v", err)
	}

	summary.WorkflowRunsFailed = 1
	if err := checkRecallifyInvariants(summary, 2); err == nil {
		t.Fatal("expected failed workflow invariant error")
	}

	summary = recallifySummary{
		WorkflowRunsStarted:   2,
		WorkflowRunsCompleted: 2,
		MCQValidationPasses:   1,
		TaskAttempts:          10,
	}
	if err := checkRecallifyInvariants(summary, 2); err == nil || !strings.Contains(err.Error(), "MCQ validation passes") {
		t.Fatalf("expected validation count error, got %v", err)
	}
}

func TestPercentileDuration(t *testing.T) {
	if got := percentileDuration(nil, 0.95); got != 0 {
		t.Fatalf("expected empty percentile 0, got %s", got)
	}
	if got := percentileDuration([]time.Duration{3 * time.Second}, 0.95); got != 3*time.Second {
		t.Fatalf("expected one item percentile, got %s", got)
	}

	values := []time.Duration{4 * time.Second, time.Second, 3 * time.Second, 2 * time.Second}
	if got := percentileDuration(values, 0.50); got != 2*time.Second {
		t.Fatalf("expected p50 2s, got %s", got)
	}
	if got := percentileDuration(values, 0.95); got != 4*time.Second {
		t.Fatalf("expected p95 4s, got %s", got)
	}
}

func TestRenderRecallifySummary(t *testing.T) {
	var out bytes.Buffer
	err := renderRecallifySummary(&out, recallifySummary{
		WorkflowID:            "workflow-id",
		Tasks:                 6,
		Dependencies:          5,
		WorkflowRunsStarted:   2,
		WorkflowRunsCompleted: 2,
		MCQValidationPasses:   2,
		TaskAttempts:          10,
		Retries:               1,
		P50WorkflowDuration:   time.Second,
		P95WorkflowDuration:   2 * time.Second,
		Elapsed:               1500 * time.Millisecond,
	}, false)
	if err != nil {
		t.Fatalf("render summary: %v", err)
	}

	want := `workflow: workflow-id
tasks: 6
dependencies: 5
workflow runs started: 2
workflow runs completed: 2
workflow runs failed: 0
MCQ validation passes: 2
task attempts: 10
retries: 1
dead letters: 0
p50 workflow duration: 1s
p95 workflow duration: 2s
outbox pending: 0
elapsed: 1.5s
`
	if out.String() != want {
		t.Fatalf("summary mismatch:\n got %q\nwant %q", out.String(), want)
	}
}

func TestRenderRecallifyJSONSummary(t *testing.T) {
	var out bytes.Buffer
	err := renderRecallifySummary(&out, recallifySummary{
		Tag:                   "baseline",
		RunsRequested:         5,
		WorkersRequested:      1,
		WorkflowID:            "workflow-id",
		Tasks:                 6,
		Dependencies:          5,
		WorkflowRunsStarted:   5,
		WorkflowRunsCompleted: 5,
		MCQValidationPasses:   5,
		CallbackSkipped:       5,
		GenerationMode:        "fake",
		Fixture:               "go-notes.txt",
		TaskAttempts:          30,
		P50WorkflowDuration:   time.Second,
		P95WorkflowDuration:   2 * time.Second,
		Elapsed:               3 * time.Second,
	}, true)
	if err != nil {
		t.Fatalf("render JSON summary: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("summary JSON is invalid: %v\n%s", err, out.String())
	}
	for key, want := range map[string]any{
		"tag":                   "baseline",
		"runs_requested":        float64(5),
		"workers_requested":     float64(1),
		"generation_mode":       "fake",
		"fixture":               "go-notes.txt",
		"mcq_validation_passes": float64(5),
		"callback_skipped":      float64(5),
		"p95_workflow_duration": float64(2 * time.Second),
	} {
		if got[key] != want {
			t.Fatalf("expected %s=%#v, got %#v in %#v", key, want, got[key], got)
		}
	}
}

func TestWriteRecallifySummaryOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	if err := writeRecallifySummaryOutput(path, recallifySummary{GenerationMode: "recallify_backend"}); err != nil {
		t.Fatalf("write summary output: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat summary output: %v", err)
	}
}

type fakeWorkflowCreator struct {
	createdTasks        []workflow.CreateTaskInput
	createdDependencies []workflow.CreateDependencyInput
	createdRuns         []workflow.CreateWorkflowRunInput
}

func (f *fakeWorkflowCreator) CreateWorkflow(_ context.Context, input workflow.CreateWorkflowInput) (workflow.Workflow, error) {
	return workflow.Workflow{ID: "workflow-id", Name: input.Name}, nil
}

func (f *fakeWorkflowCreator) CreateTask(_ context.Context, workflowID string, input workflow.CreateTaskInput) (workflow.Task, error) {
	f.createdTasks = append(f.createdTasks, input)
	return workflow.Task{
		ID:           input.Name + "-id",
		WorkflowID:   workflowID,
		Name:         input.Name,
		ExecutorType: input.ExecutorType,
	}, nil
}

func (f *fakeWorkflowCreator) CreateWorkflowRun(_ context.Context, workflowID string, input workflow.CreateWorkflowRunInput) (workflow.WorkflowRun, error) {
	f.createdRuns = append(f.createdRuns, input)
	return workflow.WorkflowRun{
		ID:         fmt.Sprintf("run-%d", len(f.createdRuns)),
		WorkflowID: workflowID,
		Input:      input.Input,
	}, nil
}

func (f *fakeWorkflowCreator) CreateDependency(_ context.Context, workflowID string, input workflow.CreateDependencyInput) (workflow.Dependency, error) {
	f.createdDependencies = append(f.createdDependencies, input)
	return workflow.Dependency{
		WorkflowID:        workflowID,
		PredecessorTaskID: input.PredecessorTaskID,
		SuccessorTaskID:   input.SuccessorTaskID,
	}, nil
}

type fakeTaskRunQueuer struct {
	queuedRunIDs []string
}

func (f *fakeTaskRunQueuer) QueueRunnableTaskRuns(_ context.Context, workflowRunID string) error {
	f.queuedRunIDs = append(f.queuedRunIDs, workflowRunID)
	return nil
}
