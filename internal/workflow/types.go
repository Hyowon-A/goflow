package workflow

type CreateWorkflowInput struct {
	Name         string
	InputSchema  map[string]any
	OutputSchema map[string]any
}

type Workflow struct {
	ID           string
	Name         string
	InputSchema  map[string]any
	OutputSchema map[string]any
}

type CreateTaskInput struct {
	Name         string
	ExecutorType string
	Config       map[string]any
	InputSchema  map[string]any
	OutputSchema map[string]any
}

type Task struct {
	ID           string
	WorkflowID   string
	Name         string
	ExecutorType string
	Config       map[string]any
	InputSchema  map[string]any
	OutputSchema map[string]any
}

type CreateDependencyInput struct {
	PredecessorTaskID string
	SuccessorTaskID   string
}

type Dependency struct {
	WorkflowID        string
	PredecessorTaskID string
	SuccessorTaskID   string
}

type CreateWorkflowRunInput struct {
	Input map[string]any
}

type WorkflowRun struct {
	ID         string
	WorkflowID string
	Status     string
	Input      map[string]any
}

type TaskRun struct {
	ID            string
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	Status        TaskRunStatus
}

type ClaimTaskRunInput struct {
	TaskRunID string
	WorkerID  string
}
