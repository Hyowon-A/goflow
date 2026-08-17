package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Hyowon-A/goflow/internal/config"
	"github.com/Hyowon-A/goflow/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var (
	errRunIDRequired = errors.New("workflow run ID is required")
	errRunNotFound   = errors.New("workflow run not found")
)

type runEvidence struct {
	ID         string
	WorkflowID string
	Status     string
}

type taskEvidence struct {
	ID            string
	Name          string
	Status        string
	AttemptCount  int
	FailureReason string
}

type incidentReport struct {
	Run           runEvidence
	Tasks         []taskEvidence
	OutboxPending int64
	RedisPending  *int64
	RedisError    string
	Metrics       []string
	MetricsError  string
}

type evidenceSource interface {
	LoadRun(context.Context, string) (runEvidence, error)
	LoadFailedTasks(context.Context, string) ([]taskEvidence, error)
	CountPendingOutbox(context.Context, string) (int64, error)
}

type postgresEvidenceSource struct{ db *pgxpool.Pool }

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "incident report failed:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	_ = godotenv.Load()

	flags := flag.NewFlagSet("incidentreport", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var runID, metricsURL string
	flags.StringVar(&runID, "run", "", "workflow run ID")
	flags.StringVar(&metricsURL, "metrics-url", "", "optional GoFlow metrics URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if runID == "" && flags.NArg() == 1 {
		runID = flags.Arg(0)
	}
	if _, err := uuid.Parse(runID); err != nil {
		return errRunIDRequired
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	report, err := loadDatabaseEvidence(ctx, postgresEvidenceSource{db}, runID)
	if err != nil {
		return err
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	pending, err := redisClient.XPending(ctx, cfg.QueueStreamName, cfg.QueueConsumerGroup).Result()
	if err != nil {
		report.RedisError = err.Error()
	} else {
		report.RedisPending = &pending.Count
	}

	if strings.TrimSpace(metricsURL) != "" {
		report.Metrics, err = fetchMetrics(ctx, http.DefaultClient, metricsURL)
		if err != nil {
			report.MetricsError = err.Error()
		}
	}

	return renderReport(out, report)
}

func loadDatabaseEvidence(ctx context.Context, source evidenceSource, runID string) (incidentReport, error) {
	run, err := source.LoadRun(ctx, runID)
	if err != nil {
		return incidentReport{}, err
	}
	tasks, err := source.LoadFailedTasks(ctx, runID)
	if err != nil {
		return incidentReport{}, err
	}
	outboxPending, err := source.CountPendingOutbox(ctx, runID)
	if err != nil {
		return incidentReport{}, err
	}
	return incidentReport{Run: run, Tasks: tasks, OutboxPending: outboxPending}, nil
}

func (s postgresEvidenceSource) LoadRun(ctx context.Context, runID string) (runEvidence, error) {
	var run runEvidence
	err := s.db.QueryRow(ctx, `
		SELECT id, workflow_id, status
		FROM workflow_runs
		WHERE id = $1
	`, runID).Scan(&run.ID, &run.WorkflowID, &run.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return runEvidence{}, fmt.Errorf("%w: %s", errRunNotFound, runID)
	}
	if err != nil {
		return runEvidence{}, fmt.Errorf("load workflow run: %w", err)
	}
	return run, nil
}

func (s postgresEvidenceSource) LoadFailedTasks(ctx context.Context, runID string) ([]taskEvidence, error) {
	rows, err := s.db.Query(ctx, `
		SELECT tr.id, t.name, tr.status, tr.attempt_count,
			COALESCE(latest.failure_reason, '')
		FROM task_runs tr
		JOIN tasks t ON t.id = tr.task_id
		LEFT JOIN LATERAL (
			SELECT failure_reason
			FROM task_attempts
			WHERE task_run_id = tr.id
			ORDER BY attempt_number DESC
			LIMIT 1
		) latest ON true
		WHERE tr.workflow_run_id = $1
			AND tr.status IN ('failed', 'dead_letter')
		ORDER BY t.name, tr.id
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("load failed task runs: %w", err)
	}
	defer rows.Close()

	var tasks []taskEvidence
	for rows.Next() {
		var task taskEvidence
		if err := rows.Scan(&task.ID, &task.Name, &task.Status, &task.AttemptCount, &task.FailureReason); err != nil {
			return nil, fmt.Errorf("scan failed task run: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read failed task runs: %w", err)
	}
	return tasks, nil
}

func (s postgresEvidenceSource) CountPendingOutbox(ctx context.Context, runID string) (int64, error) {
	var count int64
	if err := s.db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_outbox_events
		WHERE workflow_run_id = $1 AND status = 'pending'
	`, runID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending outbox events: %w", err)
	}
	return count, nil
}

func fetchMetrics(ctx context.Context, client *http.Client, metricsURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create metrics request: %w", err)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch metrics: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch metrics: HTTP %d", response.StatusCode)
	}

	var metrics []string
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "goflow_") {
			metrics = append(metrics, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read metrics: %w", err)
	}
	return metrics, nil
}

func renderReport(w io.Writer, report incidentReport) error {
	if _, err := fmt.Fprintf(w, "workflow run: %s\nworkflow: %s\nstatus: %s\n", report.Run.ID, report.Run.WorkflowID, report.Run.Status); err != nil {
		return err
	}
	if len(report.Tasks) == 0 {
		fmt.Fprintln(w, "failed/dead-letter tasks: none")
	} else {
		fmt.Fprintln(w, "failed/dead-letter tasks:")
		for _, task := range report.Tasks {
			reason := strings.Join(strings.Fields(task.FailureReason), " ")
			if reason == "" {
				reason = "no failure reason recorded"
			}
			fmt.Fprintf(w, "- %s (%s, attempts=%d, task_run=%s): %s\n", task.Name, task.Status, task.AttemptCount, task.ID, reason)
		}
	}
	fmt.Fprintf(w, "outbox pending for run: %d\n", report.OutboxPending)
	if report.RedisPending != nil {
		fmt.Fprintf(w, "redis pending for configured group: %d\n", *report.RedisPending)
	} else {
		fmt.Fprintf(w, "redis pending for configured group: unavailable (%s)\n", report.RedisError)
	}
	if report.MetricsError != "" {
		fmt.Fprintf(w, "metrics snapshot: unavailable (%s)\n", report.MetricsError)
	} else if len(report.Metrics) > 0 {
		fmt.Fprintln(w, "metrics snapshot:")
		for _, metric := range report.Metrics {
			fmt.Fprintln(w, "-", metric)
		}
	}

	checks := suggestedChecks(report)
	if len(checks) == 0 {
		fmt.Fprintln(w, "suggested next checks: none")
		return nil
	}
	fmt.Fprintln(w, "suggested next checks:")
	for _, check := range checks {
		fmt.Fprintln(w, "-", check)
	}
	return nil
}

func suggestedChecks(report incidentReport) []string {
	var checks []string
	for _, task := range report.Tasks {
		reason := strings.Join(strings.Fields(task.FailureReason), " ")
		if reason == "" {
			reason = "no failure reason recorded"
		}
		checks = append(checks, fmt.Sprintf("inspect task %s because task run %s is %s; latest evidence: %s", task.Name, task.ID, task.Status, reason))
	}
	if report.OutboxPending > 0 {
		checks = append(checks, fmt.Sprintf("check the outbox dispatcher and Redis because this run has %d pending outbox event(s)", report.OutboxPending))
	}
	if report.RedisPending != nil && *report.RedisPending > 0 {
		checks = append(checks, fmt.Sprintf("inspect active consumers because the configured Redis group has %d pending message(s)", *report.RedisPending))
	}
	if len(checks) == 0 && report.Run.Status != "completed" {
		checks = append(checks, fmt.Sprintf("inspect task-run progress because the workflow status is %s", report.Run.Status))
	}
	return checks
}
