BEGIN;

ALTER TABLE workflow_runs
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN request_hash TEXT;

ALTER TABLE workflow_runs
    ADD CONSTRAINT chk_workflow_runs_idempotency_hash
    CHECK (idempotency_key IS NULL OR request_hash IS NOT NULL);

CREATE UNIQUE INDEX uq_workflow_runs_idempotency
    ON workflow_runs (workflow_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMIT;
