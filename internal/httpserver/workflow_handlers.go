package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type createWorkflowRequest struct {
	Name         string         `json:"name"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema"`
}

type workflowResponse struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	InputSchema  map[string]any `json:"input_schema,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
}

type createTaskRequest struct {
	Name         string         `json:"name"`
	ExecutorType string         `json:"executor_type"`
	Config       map[string]any `json:"config"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema"`
}

type taskResponse struct {
	ID           string         `json:"id"`
	WorkflowID   string         `json:"workflow_id"`
	Name         string         `json:"name"`
	ExecutorType string         `json:"executor_type"`
	Config       map[string]any `json:"config,omitempty"`
	InputSchema  map[string]any `json:"input_schema,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
}

type createDependencyRequest struct {
	PredecessorTaskID string `json:"predecessor_task_id"`
	SuccessorTaskID   string `json:"successor_task_id"`
}

type dependencyResponse struct {
	WorkflowID        string `json:"workflow_id"`
	PredecessorTaskID string `json:"predecessor_task_id"`
	SuccessorTaskID   string `json:"successor_task_id"`
}

type createWorkflowRunRequest struct {
	Input map[string]any `json:"input"`
}

type workflowRunResponse struct {
	ID         string         `json:"id"`
	WorkflowID string         `json:"workflow_id"`
	Status     string         `json:"status"`
	Input      map[string]any `json:"input,omitempty"`
}

type notImplementedWorkflowService struct{}

func (notImplementedWorkflowService) CreateWorkflow(context.Context, workflow.CreateWorkflowInput) (workflow.Workflow, error) {
	return workflow.Workflow{}, workflow.ErrNotImplemented
}

func (notImplementedWorkflowService) CreateTask(context.Context, string, workflow.CreateTaskInput) (workflow.Task, error) {
	return workflow.Task{}, workflow.ErrNotImplemented
}

func (notImplementedWorkflowService) CreateDependency(context.Context, string, workflow.CreateDependencyInput) (workflow.Dependency, error) {
	return workflow.Dependency{}, workflow.ErrNotImplemented
}

func (notImplementedWorkflowService) CreateWorkflowRun(context.Context, string, workflow.CreateWorkflowRunInput) (workflow.WorkflowRun, error) {
	return workflow.WorkflowRun{}, workflow.ErrNotImplemented
}

func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	id := requestIDForRequest(r)

	var req createWorkflowRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "name is required", id)
		return
	}

	result, err := s.workflows.CreateWorkflow(r.Context(), workflow.CreateWorkflowInput{
		Name:         req.Name,
		InputSchema:  req.InputSchema,
		OutputSchema: req.OutputSchema,
	})
	if err != nil {
		writeWorkflowError(w, id, err)
		return
	}

	w.Header().Set("Location", "/workflows/"+result.ID)
	writeJSON(w, http.StatusCreated, workflowResponse{
		ID:           result.ID,
		Name:         result.Name,
		InputSchema:  result.InputSchema,
		OutputSchema: result.OutputSchema,
	})
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	id := requestIDForRequest(r)

	workflowID, ok := workflowIDPathParam(w, r, id)
	if !ok {
		return
	}

	var req createTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Name == "" || req.ExecutorType == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "name and executor_type are required", id)
		return
	}

	result, err := s.workflows.CreateTask(r.Context(), workflowID, workflow.CreateTaskInput{
		Name:         req.Name,
		ExecutorType: req.ExecutorType,
		Config:       req.Config,
		InputSchema:  req.InputSchema,
		OutputSchema: req.OutputSchema,
	})
	if err != nil {
		writeWorkflowError(w, id, err)
		return
	}

	w.Header().Set("Location", "/workflows/"+workflowID+"/tasks/"+result.ID)
	writeJSON(w, http.StatusCreated, taskResponse{
		ID:           result.ID,
		WorkflowID:   result.WorkflowID,
		Name:         result.Name,
		ExecutorType: result.ExecutorType,
		Config:       result.Config,
		InputSchema:  result.InputSchema,
		OutputSchema: result.OutputSchema,
	})
}

func (s *Server) createDependency(w http.ResponseWriter, r *http.Request) {
	id := requestIDForRequest(r)

	workflowID, ok := workflowIDPathParam(w, r, id)
	if !ok {
		return
	}

	var req createDependencyRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.PredecessorTaskID == "" || req.SuccessorTaskID == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "predecessor_task_id and successor_task_id are required", id)
		return
	}
	if !isUUID(req.PredecessorTaskID) || !isUUID(req.SuccessorTaskID) {
		writeError(w, http.StatusBadRequest, "invalid_uuid", "predecessor_task_id and successor_task_id must be valid UUIDs", id)
		return
	}
	if req.PredecessorTaskID == req.SuccessorTaskID {
		writeError(w, http.StatusBadRequest, "self_dependency", "a task cannot depend on itself", id)
		return
	}

	result, err := s.workflows.CreateDependency(r.Context(), workflowID, workflow.CreateDependencyInput{
		PredecessorTaskID: req.PredecessorTaskID,
		SuccessorTaskID:   req.SuccessorTaskID,
	})
	if err != nil {
		writeWorkflowError(w, id, err)
		return
	}

	writeJSON(w, http.StatusCreated, dependencyResponse{
		WorkflowID:        result.WorkflowID,
		PredecessorTaskID: result.PredecessorTaskID,
		SuccessorTaskID:   result.SuccessorTaskID,
	})
}

func (s *Server) createWorkflowRun(w http.ResponseWriter, r *http.Request) {
	id := requestIDForRequest(r)

	workflowID, ok := workflowIDPathParam(w, r, id)
	if !ok {
		return
	}

	var req createWorkflowRunRequest
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}

	workflowRun, err := s.workflows.CreateWorkflowRun(r.Context(), workflowID, workflow.CreateWorkflowRunInput{
		Input:          req.Input,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		if errors.Is(err, workflow.ErrIdempotencyConflict) {
			s.logger.InfoContext(r.Context(), "idempotency_key_conflict",
				slog.String("request_id", id),
				slog.String("workflow_id", workflowID),
			)
		}
		writeWorkflowError(w, id, err)
		return
	}
	if workflowRun.IdempotencyReused {
		s.logger.InfoContext(r.Context(), "idempotency_key_reused",
			slog.String("request_id", id),
			slog.String("workflow_id", workflowID),
			slog.String("workflow_run_id", workflowRun.ID),
		)
	}

	if s.scheduler != nil {
		if err := s.scheduler.QueueRunnableTaskRuns(r.Context(), workflowRun.ID); err != nil {
			writeWorkflowError(w, id, err)
			return
		}
	}

	w.Header().Set("Location", "/workflows/"+workflowID+"/runs/"+workflowRun.ID)
	writeJSON(w, http.StatusCreated, workflowRunResponse{
		ID:         workflowRun.ID,
		WorkflowID: workflowRun.WorkflowID,
		Status:     workflowRun.Status,
		Input:      workflowRun.Input,
	})
}

func writeWorkflowError(w http.ResponseWriter, requestID string, err error) {
	switch {
	case errors.Is(err, workflow.ErrWorkflowNotFound):
		writeError(w, http.StatusNotFound, "workflow_not_found", "workflow was not found", requestID)
	case errors.Is(err, workflow.ErrInvalidTaskReference):
		writeError(w, http.StatusBadRequest, "invalid_task_reference", "dependency references a task outside the workflow or a missing task", requestID)
	case errors.Is(err, workflow.ErrDuplicateTaskName):
		writeError(w, http.StatusConflict, "duplicate_task_name", "task name already exists in this workflow", requestID)
	case errors.Is(err, workflow.ErrDuplicateDependency):
		writeError(w, http.StatusConflict, "duplicate_dependency", "dependency already exists", requestID)
	case errors.Is(err, workflow.ErrSelfDependency):
		writeError(w, http.StatusBadRequest, "self_dependency", "a task cannot depend on itself", requestID)
	case errors.Is(err, workflow.ErrDependencyCycle):
		writeError(w, http.StatusBadRequest, "dependency_cycle", "dependency would create a cycle", requestID)
	case errors.Is(err, workflow.ErrEmptyWorkflow):
		writeError(w, http.StatusBadRequest, "empty_workflow", "workflow must contain at least one task before a run can be created", requestID)
	case errors.Is(err, workflow.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key was already used with a different request", requestID)
	case errors.Is(err, workflow.ErrValidation):
		writeError(w, http.StatusBadRequest, "validation_error", "request failed validation", requestID)
	case errors.Is(err, workflow.ErrNotImplemented):
		writeError(w, http.StatusNotImplemented, "not_implemented", "workflow API service is not implemented yet", requestID)
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected workflow API error", requestID)
	}
}

func workflowIDPathParam(w http.ResponseWriter, r *http.Request, requestID string) (string, bool) {
	workflowID := chi.URLParam(r, "workflowID")
	if !isUUID(workflowID) {
		writeError(w, http.StatusBadRequest, "invalid_uuid", "workflowID must be a valid UUID", requestID)
		return "", false
	}

	return workflowID, true
}

func isUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
