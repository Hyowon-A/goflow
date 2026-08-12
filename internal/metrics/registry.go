package metrics

import (
	"context"
	"fmt"
	"sync"
)

type GaugeFunc func(context.Context) (int64, error)

type Registry struct {
	mu       sync.RWMutex
	counters map[string]uint64
	gauges   map[string]GaugeFunc
}

func NewRegistry() *Registry {
	return &Registry{
		counters: map[string]uint64{},
		gauges:   map[string]GaugeFunc{},
	}
}

func (r *Registry) Inc(name string) {
	r.Add(name, 1)
}

func (r *Registry) Add(name string, delta uint64) {
	if r == nil || !knownMetric(name, Counter) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name] += delta
}

func (r *Registry) Gauge(name string, fn GaugeFunc) {
	if r == nil || fn == nil || !knownMetric(name, Gauge) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = fn
}

func (r *Registry) counterValue(name string) uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[name]
}

func (r *Registry) gaugeValue(ctx context.Context, name string) (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("metrics registry is nil")
	}
	r.mu.RLock()
	fn := r.gauges[name]
	r.mu.RUnlock()
	if fn == nil {
		return 0, nil
	}
	return fn(ctx)
}

func knownMetric(name string, metricType Type) bool {
	for _, definition := range Definitions {
		if definition.Name == name && definition.Type == metricType {
			return true
		}
	}
	return false
}
