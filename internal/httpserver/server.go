package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Hyowon-A/goflow/internal/metrics"
	"github.com/Hyowon-A/goflow/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	router          *chi.Mux
	db              databasePinger
	workflows       workflowService
	scheduler       schedulerService
	logger          *slog.Logger
	metricsRegistry *metrics.Registry
}

type databasePinger interface {
	Ping(context.Context) error
}

type workflowService interface {
	CreateWorkflow(ctx context.Context, input workflow.CreateWorkflowInput) (workflow.Workflow, error)
	CreateTask(ctx context.Context, workflowID string, input workflow.CreateTaskInput) (workflow.Task, error)
	CreateDependency(ctx context.Context, workflowID string, input workflow.CreateDependencyInput) (workflow.Dependency, error)
	CreateWorkflowRun(ctx context.Context, workflowID string, input workflow.CreateWorkflowRunInput) (workflow.WorkflowRun, error)
	GetWorkflowRun(ctx context.Context, workflowID, workflowRunID string) (workflow.WorkflowRun, error)
	ListTaskRuns(ctx context.Context, workflowID, workflowRunID string) ([]workflow.TaskRun, error)
	ListTaskAttempts(ctx context.Context, workflowID, workflowRunID, taskRunID string) ([]workflow.TaskAttempt, error)
}

type schedulerService interface {
	QueueRunnableTaskRuns(ctx context.Context, workflowRunID string) error
}

func New(db *pgxpool.Pool, schedulers ...schedulerService) *Server {
	return NewWithMetrics(db, nil, schedulers...)
}

func NewWithMetrics(db *pgxpool.Pool, registry *metrics.Registry, schedulers ...schedulerService) *Server {
	repo := workflow.NewPostgresRepository(db)
	service := workflow.NewServiceWithMetrics(repo, registry)
	server := newServer(db, service)
	if registry != nil {
		server.metricsRegistry = registry
	}
	if len(schedulers) > 0 {
		server.scheduler = schedulers[0]
	}
	return server
}

func newServer(db databasePinger, services ...workflowService) *Server {
	workflows := workflowService(notImplementedWorkflowService{})
	if len(services) > 0 {
		workflows = services[0]
	}

	server := &Server{
		router:          chi.NewRouter(),
		db:              db,
		workflows:       workflows,
		logger:          slog.Default(),
		metricsRegistry: metrics.NewRegistry(),
	}

	server.useMiddleware()
	server.registerRoutes()

	return server
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) registerRoutes() {
	s.router.Get("/health", s.health)
	s.router.Get("/ready", s.readiness)
	s.router.Get("/metrics", s.metrics)

	s.router.Post("/workflows", s.createWorkflow)
	s.router.Post("/workflows/{workflowID}/tasks", s.createTask)
	s.router.Post("/workflows/{workflowID}/dependencies", s.createDependency)
	s.router.Post("/workflows/{workflowID}/runs", s.createWorkflowRun)
	s.router.Get("/workflows/{workflowID}/runs/{workflowRunID}", s.getWorkflowRun)
	s.router.Get("/workflows/{workflowID}/runs/{workflowRunID}/task-runs", s.listTaskRuns)
	s.router.Get("/workflows/{workflowID}/runs/{workflowRunID}/task-runs/{taskRunID}/attempts", s.listTaskAttempts)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"service":   "goflow-api",
		"timestamp": time.Now().UTC(),
	})
}

func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"reason": "database unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	s.metricsRegistry.Handler().ServeHTTP(w, r)
}
