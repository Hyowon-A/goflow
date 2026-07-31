package worker

import "context"

const (
	ExecutorTypeSleep      = "sleep"
	ExecutorTypeLog        = "log"
	ExecutorTypeRandomFail = "random_fail"
)

type ExecutionInput struct {
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	TaskRunID     string
	ExecutorType  string
	Config        map[string]any
	TaskRunInput  map[string]any
}

type ExecutionResult struct {
	Output        map[string]any
	FailureReason string
	Retryable     bool
}

type Executor interface {
	Execute(ctx context.Context, input ExecutionInput) (ExecutionResult, error)
}
