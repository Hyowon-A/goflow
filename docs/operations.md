# GoFlow Operations Checks

This page collects local failure-injection checks and inspection queries for
PostgreSQL, Redis and the metrics endpoint.

Run these commands from the repository root. They assume the Docker Compose
services and local `.env.example` defaults: PostgreSQL on port `5433`, Redis on
port `6379`, API on port `8081`, stream `goflow:tasks` and consumer group
`goflow-workers`.

## Failure Injection

These checks are manual because they stop local services or require timing
around worker processes.

### Redis Unavailable During Outbox Dispatch

Start PostgreSQL and the API, but leave Redis stopped:

```sh
make postgres-up
docker compose stop redis
make api
```

In another terminal, create and run a workflow through the API. The scheduler
commits queued task runs and the outbox dispatcher records publish failures
when Redis is unavailable.

Inspect pending outbox rows:

```sh
docker compose exec postgres psql -U goflow -d goflow -c \
  "SELECT status, count(*) FROM task_outbox_events GROUP BY status ORDER BY status;"
```

If the API is exposing metrics, confirm the pending gauge is non-zero:

```sh
curl -sS http://localhost:8081/metrics | rg 'goflow_outbox_pending'
```

Restart Redis and a worker, then confirm the backlog drains:

```sh
make redis-up
make worker
```

Re-run the outbox status query. `pending` should fall to `0` after the worker
process dispatches the backlog.

### Worker Stopped During Task Execution

Create a workflow with one long `sleep` task, for example with
`{"duration":"60s"}`, and start one run. Start the worker, wait until the task
run is `running`, then stop the worker process.

Wait longer than `WORKER_LEASE_DURATION`, then restart the worker. The worker
recovery loop should find the expired lease and either requeue the task run or
move it to `dead_letter` if attempts are exhausted.

Inspect expired running leases while the worker is stopped:

```sh
docker compose exec postgres psql -U goflow -d goflow -c \
  "SELECT id, workflow_run_id, task_id, locked_by, lease_expires_at FROM task_runs WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now() ORDER BY lease_expires_at, id;"
```

After restart, confirm the row is no longer an expired running lease and inspect
the final state:

```sh
docker compose exec postgres psql -U goflow -d goflow -c \
  "SELECT status, count(*) FROM task_runs GROUP BY status ORDER BY status;"
```

### Duplicate Redis Message Delivery

Create and start a workflow run, then copy one task message from Redis or from
the corresponding task run IDs. Publish a second message with the same logical
IDs:

```sh
docker compose exec redis redis-cli XADD goflow:tasks '*' \
  schema_version 1 \
  workflow_id '<workflow_id>' \
  workflow_run_id '<workflow_run_id>' \
  task_id '<task_id>' \
  task_run_id '<task_run_id>'
```

Run the worker. If the task run is already `running`, `completed`, `failed` or
`dead_letter`, the duplicate message should be acknowledged without creating a
new attempt.

Inspect the attempts for the task run:

```sh
docker compose exec postgres psql -U goflow -d goflow -c \
  "SELECT ta.task_run_id, ta.attempt_number, ta.status, ta.failure_reason FROM task_attempts ta JOIN task_runs tr ON tr.id = ta.task_run_id WHERE tr.workflow_run_id = '<workflow_run_id>' ORDER BY ta.task_run_id, ta.attempt_number;"
```

### Retry Exhaustion

Create a `random_fail` task with forced failure and a retry policy:

```json
{
  "name": "always_fail",
  "executor_type": "random_fail",
  "config": {
    "failure_probability": 1,
    "retry": {
      "max_attempts": 2,
      "initial_delay": "1s",
      "multiplier": 1
    }
  }
}
```

Create a workflow run and run the worker long enough for the first failure,
retry queueing and second failure. The task run should end in `dead_letter`,
and the workflow run should finalize as `failed`.

Inspect recent dead-letter rows:

```sh
docker compose exec postgres psql -U goflow -d goflow -c \
  "SELECT workflow_run_id, task_id, id, attempt_count FROM task_runs WHERE status = 'dead_letter' ORDER BY completed_at DESC NULLS LAST, id LIMIT 20;"
```

## Inspection Queries

### PostgreSQL

Outbox status counts:

```sql
SELECT status, count(*)
FROM task_outbox_events
GROUP BY status
ORDER BY status;
```

Expired running leases:

```sql
SELECT id, workflow_run_id, task_id, locked_by, lease_expires_at
FROM task_runs
WHERE status = 'running'
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= now()
ORDER BY lease_expires_at, id;
```

Recent dead-letter task runs:

```sql
SELECT workflow_run_id, task_id, id, attempt_count
FROM task_runs
WHERE status = 'dead_letter'
ORDER BY completed_at DESC NULLS LAST, id
LIMIT 20;
```

Task attempts for one workflow run:

```sql
SELECT ta.task_run_id, ta.attempt_number, ta.status, ta.failure_reason
FROM task_attempts ta
JOIN task_runs tr ON tr.id = ta.task_run_id
WHERE tr.workflow_run_id = '<workflow_run_id>'
ORDER BY ta.task_run_id, ta.attempt_number;
```

### Redis

Stream length:

```sh
docker compose exec redis redis-cli XLEN goflow:tasks
```

Consumer groups:

```sh
docker compose exec redis redis-cli XINFO GROUPS goflow:tasks
```

Pending entries for the worker group:

```sh
docker compose exec redis redis-cli XPENDING goflow:tasks goflow-workers
```

### Metrics

Full Prometheus text output:

```sh
curl -sS http://localhost:8081/metrics
```

Selected operational metrics:

```sh
curl -sS http://localhost:8081/metrics | rg 'goflow_outbox_pending|goflow_worker_lease_recoveries_total'
```
