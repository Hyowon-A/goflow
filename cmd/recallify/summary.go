package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type recallifySummary struct {
	WorkflowID            string
	Tasks                 int
	Dependencies          int
	WorkflowRunsStarted   int64
	WorkflowRunsCompleted int64
	WorkflowRunsFailed    int64
	MCQValidationPasses   int64
	TaskAttempts          int64
	Retries               int64
	DeadLetters           int64
	OutboxPending         int64
	P50WorkflowDuration   time.Duration
	P95WorkflowDuration   time.Duration
	Elapsed               time.Duration
}

func waitForRecallifySummary(ctx context.Context, db *pgxpool.Pool, workflowID string, runs int, tasks int, dependencies int, startedAt time.Time) (recallifySummary, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		summary, err := loadRecallifySummary(ctx, db, workflowID, tasks, dependencies, startedAt)
		if err != nil {
			return summary, err
		}
		if summary.WorkflowRunsCompleted+summary.WorkflowRunsFailed == int64(runs) {
			return summary, nil
		}

		select {
		case <-ctx.Done():
			summary.Elapsed = time.Since(startedAt)
			return summary, fmt.Errorf("timeout waiting for %d recallify workflow runs: %w", runs, ctx.Err())
		case <-ticker.C:
		}
	}
}

func loadRecallifySummary(ctx context.Context, db *pgxpool.Pool, workflowID string, tasks int, dependencies int, startedAt time.Time) (recallifySummary, error) {
	summary := recallifySummary{
		WorkflowID:   workflowID,
		Tasks:        tasks,
		Dependencies: dependencies,
		Elapsed:      time.Since(startedAt),
	}

	err := db.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM workflow_runs
		WHERE workflow_id = $1
	`, workflowID).Scan(&summary.WorkflowRunsStarted, &summary.WorkflowRunsCompleted, &summary.WorkflowRunsFailed)
	if err != nil {
		return recallifySummary{}, fmt.Errorf("summarize recallify workflow runs: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_attempts ta
		JOIN task_runs tr ON tr.id = ta.task_run_id
		WHERE tr.workflow_id = $1
	`, workflowID).Scan(&summary.TaskAttempts)
	if err != nil {
		return recallifySummary{}, fmt.Errorf("summarize recallify task attempts: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(GREATEST(tr.attempt_count - 1, 0)), 0),
			COUNT(*) FILTER (WHERE tr.status = 'dead_letter')
		FROM task_runs tr
		WHERE tr.workflow_id = $1
	`, workflowID).Scan(&summary.Retries, &summary.DeadLetters)
	if err != nil {
		return recallifySummary{}, fmt.Errorf("summarize recallify task runs: %w", err)
	}

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_runs tr
		JOIN tasks t ON t.id = tr.task_id
		WHERE tr.workflow_id = $1
			AND t.name = 'validate_mcqs'
			AND tr.status = 'completed'
	`, workflowID).Scan(&summary.MCQValidationPasses)
	if err != nil {
		return recallifySummary{}, fmt.Errorf("summarize recallify validation passes: %w", err)
	}

	durations, err := loadRecallifyWorkflowDurations(ctx, db, workflowID)
	if err != nil {
		return recallifySummary{}, err
	}
	summary.P50WorkflowDuration = percentileDuration(durations, 0.50)
	summary.P95WorkflowDuration = percentileDuration(durations, 0.95)

	err = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM task_outbox_events
		WHERE workflow_id = $1
			AND status = 'pending'
	`, workflowID).Scan(&summary.OutboxPending)
	if err != nil {
		return recallifySummary{}, fmt.Errorf("summarize recallify outbox: %w", err)
	}

	return summary, nil
}

func loadRecallifyWorkflowDurations(ctx context.Context, db *pgxpool.Pool, workflowID string) ([]time.Duration, error) {
	rows, err := db.Query(ctx, `
		SELECT EXTRACT(EPOCH FROM (completed_at - created_at))
		FROM workflow_runs
		WHERE workflow_id = $1
			AND completed_at IS NOT NULL
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("summarize recallify workflow durations: %w", err)
	}
	defer rows.Close()

	var durations []time.Duration
	for rows.Next() {
		var seconds float64
		if err := rows.Scan(&seconds); err != nil {
			return nil, fmt.Errorf("scan recallify workflow duration: %w", err)
		}
		durations = append(durations, time.Duration(seconds*float64(time.Second)))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recallify workflow durations: %w", err)
	}
	return durations, nil
}

func percentileDuration(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func checkRecallifyInvariants(summary recallifySummary, wantRuns int) error {
	if summary.WorkflowRunsStarted != int64(wantRuns) {
		return fmt.Errorf("expected %d workflow runs, got %d", wantRuns, summary.WorkflowRunsStarted)
	}
	if summary.WorkflowRunsCompleted+summary.WorkflowRunsFailed != summary.WorkflowRunsStarted {
		return fmt.Errorf("expected all recallify workflow runs terminal, got completed=%d failed=%d started=%d", summary.WorkflowRunsCompleted, summary.WorkflowRunsFailed, summary.WorkflowRunsStarted)
	}
	if summary.WorkflowRunsFailed != 0 {
		return fmt.Errorf("expected no failed recallify workflow runs, got %d", summary.WorkflowRunsFailed)
	}
	if summary.TaskAttempts == 0 {
		return errors.New("expected recallify task attempts")
	}
	if summary.MCQValidationPasses != summary.WorkflowRunsCompleted {
		return fmt.Errorf("expected %d MCQ validation passes, got %d", summary.WorkflowRunsCompleted, summary.MCQValidationPasses)
	}
	if summary.DeadLetters != 0 {
		return fmt.Errorf("expected no recallify dead letters, got %d", summary.DeadLetters)
	}
	if summary.OutboxPending != 0 {
		return fmt.Errorf("expected no pending recallify outbox events, got %d", summary.OutboxPending)
	}
	return nil
}

func renderRecallifySummary(w io.Writer, summary recallifySummary) error {
	_, err := fmt.Fprintf(w, `workflow: %s
tasks: %d
dependencies: %d
workflow runs started: %d
workflow runs completed: %d
workflow runs failed: %d
MCQ validation passes: %d
task attempts: %d
retries: %d
dead letters: %d
p50 workflow duration: %s
p95 workflow duration: %s
outbox pending: %d
elapsed: %s
`,
		summary.WorkflowID,
		summary.Tasks,
		summary.Dependencies,
		summary.WorkflowRunsStarted,
		summary.WorkflowRunsCompleted,
		summary.WorkflowRunsFailed,
		summary.MCQValidationPasses,
		summary.TaskAttempts,
		summary.Retries,
		summary.DeadLetters,
		summary.P50WorkflowDuration.Round(time.Millisecond),
		summary.P95WorkflowDuration.Round(time.Millisecond),
		summary.OutboxPending,
		summary.Elapsed.Round(time.Millisecond),
	)
	return err
}
