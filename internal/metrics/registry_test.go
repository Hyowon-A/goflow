package metrics

import (
	"context"
	"sync"
	"testing"
)

func TestRegistryConcurrentIncrements(t *testing.T) {
	registry := NewRegistry()
	const workers = 20
	const increments = 100

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				registry.Inc("goflow_workflow_runs_started_total")
			}
		}()
	}
	wg.Wait()

	if got := registry.counterValue("goflow_workflow_runs_started_total"); got != workers*increments {
		t.Fatalf("expected counter %d, got %d", workers*increments, got)
	}
}

func TestRegistryIgnoresUnknownMetrics(t *testing.T) {
	registry := NewRegistry()

	registry.Inc("unknown_total")
	registry.Gauge("unknown_gauge", func(context.Context) (int64, error) { return 1, nil })

	if got := registry.counterValue("unknown_total"); got != 0 {
		t.Fatalf("expected unknown counter to stay 0, got %d", got)
	}
	if got, err := registry.gaugeValue(context.Background(), "unknown_gauge"); err != nil || got != 0 {
		t.Fatalf("expected unknown gauge to stay empty, got value=%d err=%v", got, err)
	}
}

func TestRegistryGaugeCallsProvider(t *testing.T) {
	registry := NewRegistry()
	calls := 0
	registry.Gauge("goflow_outbox_pending", func(context.Context) (int64, error) {
		calls++
		return 3, nil
	})

	got, err := registry.gaugeValue(context.Background(), "goflow_outbox_pending")
	if err != nil {
		t.Fatalf("gauge value: %v", err)
	}
	if got != 3 || calls != 1 {
		t.Fatalf("expected gauge value 3 and one call, got value=%d calls=%d", got, calls)
	}
}
