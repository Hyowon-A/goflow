BEGIN;

DROP TABLE task_attempts;
DROP TABLE task_runs;
DROP TABLE workflow_runs;
DROP TABLE task_dependencies;
DROP TABLE tasks;
DROP TABLE workflows;

DROP TYPE task_attempt_status;
DROP TYPE task_run_status;
DROP TYPE workflow_run_status;

COMMIT;
