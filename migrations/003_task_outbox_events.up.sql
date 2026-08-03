BEGIN;

CREATE TABLE task_outbox_events (
    id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    workflow_run_id UUID NOT NULL,
    task_id UUID NOT NULL,
    task_run_id UUID NOT NULL,
    event_type VARCHAR NOT NULL DEFAULT 'task_run_queued',
    status VARCHAR NOT NULL DEFAULT 'pending',
    redis_message_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    CONSTRAINT pk_task_outbox_events PRIMARY KEY (id),

    CONSTRAINT fk_task_outbox_events_task_run
        FOREIGN KEY (task_run_id)
        REFERENCES task_runs (id),

    CONSTRAINT chk_task_outbox_events_type
        CHECK (event_type = 'task_run_queued'),

    CONSTRAINT chk_task_outbox_events_status
        CHECK (status IN ('pending', 'publishing', 'published')),

    CONSTRAINT chk_task_outbox_events_attempt_count
        CHECK (attempt_count >= 0),

    CONSTRAINT chk_task_outbox_events_published
        CHECK (
            status <> 'published'
            OR (redis_message_id IS NOT NULL AND published_at IS NOT NULL)
        )
);

CREATE UNIQUE INDEX uq_task_outbox_events_unpublished_task_run
    ON task_outbox_events (task_run_id, event_type)
    WHERE status <> 'published';

CREATE INDEX idx_task_outbox_events_pending
    ON task_outbox_events (created_at, id)
    WHERE status = 'pending';

COMMIT;
