package metrics

type Type string

const (
	Counter Type = "counter"
	Gauge   Type = "gauge"
)

type Definition struct {
	Name string
	Type Type
	Help string
}

var Definitions = []Definition{
	{Name: "goflow_workflow_runs_started_total", Type: Counter, Help: "Workflow runs created."},
	{Name: "goflow_workflow_runs_completed_total", Type: Counter, Help: "Workflow runs finalized as completed."},
	{Name: "goflow_workflow_runs_failed_total", Type: Counter, Help: "Workflow runs finalized as failed."},
	{Name: "goflow_task_runs_queued_total", Type: Counter, Help: "Task runs moved to queued."},
	{Name: "goflow_task_runs_completed_total", Type: Counter, Help: "Task runs completed successfully."},
	{Name: "goflow_task_runs_dead_lettered_total", Type: Counter, Help: "Task runs moved to dead_letter."},
	{Name: "goflow_task_attempts_completed_total", Type: Counter, Help: "Task attempts completed successfully."},
	{Name: "goflow_task_attempts_failed_total", Type: Counter, Help: "Task attempts failed."},
	{Name: "goflow_outbox_published_total", Type: Counter, Help: "Task outbox events published to Redis."},
	{Name: "goflow_outbox_publish_failures_total", Type: Counter, Help: "Task outbox publish failures."},
	{Name: "goflow_worker_lease_heartbeats_total", Type: Counter, Help: "Successful worker lease heartbeats."},
	{Name: "goflow_worker_lease_heartbeat_failures_total", Type: Counter, Help: "Failed worker lease heartbeats."},
	{Name: "goflow_worker_lease_recoveries_total", Type: Counter, Help: "Expired task-run leases recovered."},
	{Name: "goflow_worker_late_completions_rejected_total", Type: Counter, Help: "Late worker completions rejected."},
	{Name: "goflow_worker_messages_acknowledged_total", Type: Counter, Help: "Redis task messages acknowledged by workers."},
	{Name: "goflow_worker_messages_left_pending_total", Type: Counter, Help: "Redis task messages intentionally left pending."},
	{Name: "goflow_outbox_pending", Type: Gauge, Help: "Pending task outbox events."},
	{Name: "goflow_task_runs_running", Type: Gauge, Help: "Task runs currently running."},
	{Name: "goflow_task_runs_expired_leases", Type: Gauge, Help: "Running task runs with expired leases."},
}
