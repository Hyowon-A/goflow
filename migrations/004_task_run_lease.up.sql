BEGIN;

ALTER TABLE task_runs
    ADD COLUMN locked_by TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX idx_task_runs_expired
    ON task_runs (lease_expires_at, id)
    WHERE status = 'running'
        AND lease_expires_at IS NOT NULL;

COMMIT;
