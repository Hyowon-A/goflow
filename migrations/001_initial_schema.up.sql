BEGIN;

CREATE TYPE workflow_run_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed'
);

CREATE TYPE task_run_status AS ENUM (
    'pending',
    'queued',
    'running',
    'retry_wait',
    'completed',
    'failed',
    'dead_letter'
);

CREATE TYPE task_attempt_status AS ENUM (
    'running',
    'completed',
    'failed'
);

CREATE TABLE workflows (
    id UUID NOT NULL,
    name VARCHAR NOT NULL,
    input_schema JSONB,
    output_schema JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_workflows PRIMARY KEY (id)
);

CREATE TABLE tasks (
    id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    name VARCHAR NOT NULL,
    executor_type VARCHAR NOT NULL,
    config JSONB,
    input_schema JSONB,
    output_schema JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_tasks PRIMARY KEY (id),

    CONSTRAINT fk_tasks_workflow
        FOREIGN KEY (workflow_id)
        REFERENCES workflows (id),

    CONSTRAINT uq_tasks_workflow_name
        UNIQUE (workflow_id, name),

    CONSTRAINT uq_tasks_workflow_id
        UNIQUE (workflow_id, id)
);

CREATE TABLE task_dependencies (
    workflow_id UUID NOT NULL,
    predecessor_task_id UUID NOT NULL,
    successor_task_id UUID NOT NULL,

    CONSTRAINT pk_task_dependencies
        PRIMARY KEY (predecessor_task_id, successor_task_id),

    CONSTRAINT chk_task_dependencies_not_self
        CHECK (predecessor_task_id <> successor_task_id),

    CONSTRAINT fk_task_dependencies_predecessor
        FOREIGN KEY (workflow_id, predecessor_task_id)
        REFERENCES tasks (workflow_id, id),

    CONSTRAINT fk_task_dependencies_successor
        FOREIGN KEY (workflow_id, successor_task_id)
        REFERENCES tasks (workflow_id, id)
);

CREATE TABLE workflow_runs (
    id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    status workflow_run_status NOT NULL DEFAULT 'pending',
    input JSONB,
    output JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT pk_workflow_runs PRIMARY KEY (id),

    CONSTRAINT fk_workflow_runs_workflow
        FOREIGN KEY (workflow_id)
        REFERENCES workflows (id),

    CONSTRAINT uq_workflow_runs_workflow_id
        UNIQUE (workflow_id, id),

    CONSTRAINT chk_workflow_runs_timestamp_order
        CHECK (
            completed_at IS NULL
            OR started_at IS NULL
            OR completed_at >= started_at
        )
);

CREATE TABLE task_runs (
    id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    workflow_run_id UUID NOT NULL,
    task_id UUID NOT NULL,
    status task_run_status NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    input JSONB,
    output JSONB,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT pk_task_runs PRIMARY KEY (id),

    CONSTRAINT uq_task_runs_workflow_run_task
        UNIQUE (workflow_run_id, task_id),

    CONSTRAINT chk_task_runs_attempt_count
        CHECK (attempt_count >= 0),

    CONSTRAINT chk_task_runs_timestamp_order
        CHECK (
            completed_at IS NULL
            OR started_at IS NULL
            OR completed_at >= started_at
        ),

    CONSTRAINT fk_task_runs_workflow_run
        FOREIGN KEY (workflow_id, workflow_run_id)
        REFERENCES workflow_runs (workflow_id, id),

    CONSTRAINT fk_task_runs_task
        FOREIGN KEY (workflow_id, task_id)
        REFERENCES tasks (workflow_id, id)
);

CREATE TABLE task_attempts (
    id UUID NOT NULL,
    task_run_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL,
    status task_attempt_status NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    failure_reason TEXT,

    CONSTRAINT pk_task_attempts PRIMARY KEY (id),

    CONSTRAINT fk_task_attempts_task_run
        FOREIGN KEY (task_run_id)
        REFERENCES task_runs (id),

    CONSTRAINT uq_task_attempts_task_run_number
        UNIQUE (task_run_id, attempt_number),

    CONSTRAINT chk_task_attempts_number_positive
        CHECK (attempt_number > 0),

    CONSTRAINT chk_task_attempts_timestamp_order
        CHECK (
            completed_at IS NULL
            OR completed_at >= started_at
        ),

    CONSTRAINT chk_task_attempts_failure_reason
        CHECK (
            status = 'failed'
            OR failure_reason IS NULL
        )
);

CREATE INDEX idx_task_dependencies_successor
    ON task_dependencies (successor_task_id);

CREATE INDEX idx_workflow_runs_workflow_created
    ON workflow_runs (workflow_id, created_at);

CREATE INDEX idx_workflow_runs_status
    ON workflow_runs (status);

CREATE INDEX idx_task_runs_workflow_task
    ON task_runs (workflow_id, task_id);

CREATE INDEX idx_task_runs_status_next_retry
    ON task_runs (status, next_retry_at);

CREATE INDEX idx_task_attempts_status
    ON task_attempts (status);

COMMIT;