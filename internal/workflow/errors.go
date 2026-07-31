package workflow

import "errors"

var (
	ErrNotImplemented           = errors.New("not_implemented")
	ErrValidation               = errors.New("validation_error")
	ErrWorkflowNotFound         = errors.New("workflow_not_found")
	ErrInvalidTaskReference     = errors.New("invalid_task_reference")
	ErrDuplicateTaskName        = errors.New("duplicate_task_name")
	ErrDuplicateDependency      = errors.New("duplicate_dependency")
	ErrSelfDependency           = errors.New("self_dependency")
	ErrDependencyCycle          = errors.New("dependency_cycle")
	ErrEmptyWorkflow            = errors.New("empty_workflow")
	ErrUnknownWorkflowRunStatus = errors.New("unknown_workflow_run_status")
	ErrUnknownTaskRunStatus     = errors.New("unknown_task_run_status")
	ErrUnknownTaskAttemptStatus = errors.New("unknown_task_attempt_status")
	ErrInvalidTransition        = errors.New("invalid_transition")
	ErrTaskRunNotClaimable      = errors.New("task_run_not_claimable")
	ErrTaskRunNotFound          = errors.New("task_run_not_found")
	ErrTaskAttemptNotFound      = errors.New("task_attempt_not_found")
	ErrTaskRunExecutionNotFound = errors.New("task_run_execution_not_found")
)
