package workflow

import (
	"context"
	"fmt"
	"strings"
)

const taskOutboxClaimLimit = 10

func (r *PostgresRepository) ClaimPendingTaskOutboxEvents(ctx context.Context) ([]TaskOutboxEvent, error) {
	rows, err := r.db.Query(ctx, `
		WITH claimed AS (
			SELECT id
			FROM task_outbox_events
			WHERE status = 'pending'
			ORDER BY created_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE task_outbox_events e
		SET status = 'publishing',
			attempt_count = attempt_count + 1,
			last_error = NULL
		FROM claimed
		WHERE e.id = claimed.id
		RETURNING e.id, e.workflow_id, e.workflow_run_id, e.task_id, e.task_run_id, e.attempt_count, e.status
	`, taskOutboxClaimLimit)
	if err != nil {
		return nil, fmt.Errorf("claim task outbox events: %w", err)
	}
	defer rows.Close()

	var events []TaskOutboxEvent
	for rows.Next() {
		var event TaskOutboxEvent
		if err := rows.Scan(&event.ID, &event.WorkflowID, &event.WorkflowRunID, &event.TaskID, &event.TaskRunID, &event.AttemptCount, &event.Status); err != nil {
			return nil, fmt.Errorf("scan task outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task outbox events: %w", err)
	}
	return events, nil
}

func (r *PostgresRepository) MarkTaskOutboxEventPublished(ctx context.Context, input MarkTaskOutboxEventPublishedInput) error {
	if strings.TrimSpace(input.EventID) == "" || strings.TrimSpace(input.RedisMessageID) == "" {
		return ErrTaskOutboxEventNotFound
	}

	tag, err := r.db.Exec(ctx, `
		UPDATE task_outbox_events
		SET status = 'published',
			redis_message_id = $2,
			published_at = now(),
			last_error = NULL
		WHERE id = $1
			AND status = 'publishing'
	`, strings.TrimSpace(input.EventID), strings.TrimSpace(input.RedisMessageID))
	if err != nil {
		return fmt.Errorf("mark task outbox event published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskOutboxEventNotFound
	}
	return nil
}

func (r *PostgresRepository) RecordTaskOutboxEventFailure(ctx context.Context, input RecordTaskOutboxEventFailureInput) error {
	if strings.TrimSpace(input.EventID) == "" {
		return ErrTaskOutboxEventNotFound
	}

	tag, err := r.db.Exec(ctx, `
		UPDATE task_outbox_events
		SET status = 'pending',
			last_error = $2
		WHERE id = $1
			AND status = 'publishing'
	`, strings.TrimSpace(input.EventID), input.LastError)
	if err != nil {
		return fmt.Errorf("record task outbox event failure: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskOutboxEventNotFound
	}
	return nil
}
