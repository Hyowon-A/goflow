BEGIN;

DROP INDEX uq_workflow_runs_idempotency;

ALTER TABLE workflow_runs
    DROP CONSTRAINT chk_workflow_runs_idempotency_hash;

ALTER TABLE workflow_runs
    DROP COLUMN request_hash,
    DROP COLUMN idempotency_key;

COMMIT;
