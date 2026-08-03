BEGIN;

DROP INDEX IF EXISTS idx_task_outbox_events_pending;
DROP INDEX IF EXISTS uq_task_outbox_events_unpublished_task_run;
DROP TABLE IF EXISTS task_outbox_events;

COMMIT;
