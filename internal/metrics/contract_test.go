package metrics

import "testing"

func TestDefinitionsMatchDay15Contract(t *testing.T) {
	want := []Definition{
		{Name: "goflow_workflow_runs_started_total", Type: Counter},
		{Name: "goflow_workflow_runs_completed_total", Type: Counter},
		{Name: "goflow_workflow_runs_failed_total", Type: Counter},
		{Name: "goflow_task_runs_queued_total", Type: Counter},
		{Name: "goflow_task_runs_completed_total", Type: Counter},
		{Name: "goflow_task_runs_dead_lettered_total", Type: Counter},
		{Name: "goflow_task_attempts_completed_total", Type: Counter},
		{Name: "goflow_task_attempts_failed_total", Type: Counter},
		{Name: "goflow_outbox_published_total", Type: Counter},
		{Name: "goflow_outbox_publish_failures_total", Type: Counter},
		{Name: "goflow_worker_lease_heartbeats_total", Type: Counter},
		{Name: "goflow_worker_lease_heartbeat_failures_total", Type: Counter},
		{Name: "goflow_worker_lease_recoveries_total", Type: Counter},
		{Name: "goflow_worker_late_completions_rejected_total", Type: Counter},
		{Name: "goflow_worker_messages_acknowledged_total", Type: Counter},
		{Name: "goflow_worker_messages_left_pending_total", Type: Counter},
		{Name: "goflow_outbox_pending", Type: Gauge},
		{Name: "goflow_task_runs_running", Type: Gauge},
		{Name: "goflow_task_runs_expired_leases", Type: Gauge},
	}

	if len(Definitions) != len(want) {
		t.Fatalf("expected %d metric definitions, got %d", len(want), len(Definitions))
	}
	seen := map[string]bool{}
	for i, definition := range Definitions {
		if definition.Name != want[i].Name || definition.Type != want[i].Type {
			t.Fatalf("definition %d = %#v, want name=%q type=%q", i, definition, want[i].Name, want[i].Type)
		}
		if definition.Help == "" {
			t.Fatalf("definition %s missing help", definition.Name)
		}
		if seen[definition.Name] {
			t.Fatalf("duplicate metric definition %q", definition.Name)
		}
		seen[definition.Name] = true
	}
}
