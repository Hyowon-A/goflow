package workflow

import "errors"

var (
	ErrNotImplemented       = errors.New("not_implemented")
	ErrValidation           = errors.New("validation_error")
	ErrWorkflowNotFound     = errors.New("workflow_not_found")
	ErrInvalidTaskReference = errors.New("invalid_task_reference")
	ErrDuplicateTaskName    = errors.New("duplicate_task_name")
	ErrDuplicateDependency  = errors.New("duplicate_dependency")
	ErrSelfDependency       = errors.New("self_dependency")
	ErrEmptyWorkflow        = errors.New("empty_workflow")
)
