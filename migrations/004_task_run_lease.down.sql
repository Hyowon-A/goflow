BEGIN;

DROP INDEX IF EXISTS idx_task_runs_expired;

ALTER TABLE task_runs
    DROP COLUMN IF EXISTS last_heartbeat_at,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS locked_by;

COMMIT;
