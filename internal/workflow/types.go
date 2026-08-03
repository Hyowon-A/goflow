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

type TaskAttempt struct {
	ID            string
	TaskRunID     string
	AttemptNumber uint
	Status        TaskAttemptStatus
}

type CompleteTaskAttemptInput struct {
	TaskAttemptID string
	TaskRunID     string
	Success       bool
	Output        map[string]any
	FailureReason string
}

type CompleteTaskAttemptResult struct {
	TaskAttempt TaskAttempt
	TaskRun     TaskRun
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
	Input          map[string]any
	IdempotencyKey string
}

type WorkflowRun struct {
	ID                string
	WorkflowID        string
	Status            string
	Input             map[string]any
	IdempotencyReused bool
}

type TaskRun struct {
	ID            string
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	Status        TaskRunStatus
}

type LoadTaskRunStatusInput struct {
	TaskRunID string
}

type ClaimTaskRunInput struct {
	TaskRunID string
	WorkerID  string
}

type LoadTaskRunExecutionInput struct {
	TaskRunID     string
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
}

type TaskRunExecution struct {
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	TaskRunID     string
	ExecutorType  string
	Config        map[string]any
	TaskRunInput  map[string]any
}

type TaskOutboxEvent struct {
	ID            string
	WorkflowID    string
	WorkflowRunID string
	TaskID        string
	TaskRunID     string
	AttemptCount  int
	Status        string
}

type MarkTaskOutboxEventPublishedInput struct {
	EventID        string
	RedisMessageID string
}

type RecordTaskOutboxEventFailureInput struct {
	EventID   string
	LastError string
}
