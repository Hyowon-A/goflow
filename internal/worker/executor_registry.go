package worker

import (
	"errors"
	"strings"
)

var ErrUnknownExecutorType = errors.New("unknown_executor_type")

type ExecutorRegistry map[string]Executor

func NewExecutorRegistry(executors map[string]Executor) ExecutorRegistry {
	registered := make(map[string]Executor, len(executors))
	for executorType, executor := range executors {
		name := strings.TrimSpace(executorType)
		if name == "" || executor == nil {
			continue
		}
		registered[name] = executor
	}

	return registered
}

func (r ExecutorRegistry) Resolve(executorType string) (Executor, error) {
	if r == nil {
		return nil, ErrUnknownExecutorType
	}

	executor, ok := r[strings.TrimSpace(executorType)]
	if !ok {
		return nil, ErrUnknownExecutorType
	}

	return executor, nil
}
