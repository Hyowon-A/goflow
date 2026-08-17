package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Hyowon-A/goflow/internal/recallify"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type recallifyDemoRunRequest struct {
	DocumentText      string `json:"document_text"`
	Title             string `json:"title"`
	Level             string `json:"level"`
	MCQCount          *int   `json:"mcq_count"`
	CallbackURL       string `json:"callback_url"`
	ExternalRequestID string `json:"external_request_id"`
	RecallifyURL      string `json:"recallify_url"`
	RecallifyToken    string `json:"recallify_bearer_token"`
}

type recallifyDemoRunResponse struct {
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	Status        string `json:"status"`
	Queued        bool   `json:"queued"`
}

func (s *Server) createRecallifyDemoRun(w http.ResponseWriter, r *http.Request) {
	id := requestIDForRequest(r)

	var req recallifyDemoRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}

	runInput := recallifyDemoRunInput(req)
	if _, err := recallify.NormalizeRecallifyRunInput(runInput); err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", "invalid Recallify run input", id)
		return
	}
	recallifyURL := strings.TrimSpace(req.RecallifyURL)
	if recallifyURL == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "recallify_url is required", id)
		return
	}

	created, _, err := createRecallifyDemoWorkflow(r.Context(), s.workflows, recallifyURL, req.RecallifyToken)
	if err != nil {
		writeWorkflowError(w, id, err)
		return
	}

	workflowRun, err := s.workflows.CreateWorkflowRun(r.Context(), created.ID, workflow.CreateWorkflowRunInput{
		Input:          runInput,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		writeWorkflowError(w, id, err)
		return
	}

	queued := false
	if s.scheduler != nil {
		if err := s.scheduler.QueueRunnableTaskRuns(r.Context(), workflowRun.ID); err != nil {
			writeWorkflowError(w, id, err)
			return
		}
		queued = true
	}

	w.Header().Set("Location", "/workflows/"+created.ID+"/runs/"+workflowRun.ID)
	writeJSON(w, http.StatusCreated, recallifyDemoRunResponse{
		WorkflowID:    created.ID,
		WorkflowRunID: workflowRun.ID,
		Status:        "queued",
		Queued:        queued,
	})

}

func recallifyDemoRunInput(req recallifyDemoRunRequest) map[string]any {
	input := map[string]any{
		"document_text": req.DocumentText,
	}
	if strings.TrimSpace(req.Title) != "" {
		input["title"] = req.Title
	}
	if strings.TrimSpace(req.Level) != "" {
		input["level"] = req.Level
	}
	if req.MCQCount != nil {
		input["mcq_count"] = *req.MCQCount
	}
	if strings.TrimSpace(req.CallbackURL) != "" {
		input["callback_url"] = req.CallbackURL
	}
	if strings.TrimSpace(req.ExternalRequestID) != "" {
		input["external_request_id"] = req.ExternalRequestID
	}
	return input
}

func createRecallifyDemoWorkflow(ctx context.Context, service workflowService, recallifyURL, recallifyToken string) (workflow.Workflow, map[string]workflow.Task, error) {
	created, err := service.CreateWorkflow(ctx, workflow.CreateWorkflowInput{
		Name: fmt.Sprintf("recallify-demo-%d", time.Now().UnixNano()),
	})
	if err != nil {
		return workflow.Workflow{}, nil, fmt.Errorf("create recallify demo workflow: %w", err)
	}

	tasks := map[string]workflow.Task{}
	template := recallify.NewTemplate(recallifyURL, recallifyToken)
	for _, spec := range template.Tasks {
		task, err := service.CreateTask(ctx, created.ID, workflow.CreateTaskInput{
			Name:         spec.Name,
			ExecutorType: spec.ExecutorType,
			Config:       spec.Config,
		})
		if err != nil {
			return workflow.Workflow{}, nil, fmt.Errorf("create recallify demo task %s: %w", spec.Name, err)
		}
		tasks[spec.Name] = task
	}

	for _, edge := range template.Dependencies {
		if _, err := service.CreateDependency(ctx, created.ID, workflow.CreateDependencyInput{
			PredecessorTaskID: tasks[edge[0]].ID,
			SuccessorTaskID:   tasks[edge[1]].ID,
		}); err != nil {
			return workflow.Workflow{}, nil, fmt.Errorf("create recallify demo dependency %s->%s: %w", edge[0], edge[1], err)
		}
	}

	return created, tasks, nil
}
