package db

import "time"

// WorkflowDefinitionStore defines CRUD operations for workflow definitions.
type WorkflowDefinitionStore interface {
	Create(wf *WorkflowDefinition) error
	Get(id string) (*WorkflowDefinition, error)
	GetByName(name, namespace, projectID string) (*WorkflowDefinition, error)
	List(namespace, projectID string) ([]*WorkflowDefinition, error)
	Update(wf *WorkflowDefinition) error
	Delete(id string) error
}

// ActionDefinitionStore defines CRUD operations for action definitions.
type ActionDefinitionStore interface {
	Create(a *ActionDefinition) error
	Get(id string) (*ActionDefinition, error)
	GetByName(name, namespace, projectID string) (*ActionDefinition, error)
	List(namespace, projectID string) ([]*ActionDefinition, error)
	Update(a *ActionDefinition) error
	Delete(id string) error
}

// WorkflowExecutionStore defines CRUD operations for workflow executions.
type WorkflowExecutionStore interface {
	Create(wf *WorkflowExecution) error
	Get(id string) (*WorkflowExecution, error)
	List(projectID string) ([]*WorkflowExecution, error)
	Update(wf *WorkflowExecution) error
	UpdateState(id string, curState, newState string) (*WorkflowExecution, error)
	Delete(id string) error
}

// TaskExecutionStore defines CRUD operations for task executions.
type TaskExecutionStore interface {
	Create(t *TaskExecution) error
	Get(id string) (*TaskExecution, error)
	List(workflowExecutionID, state string) ([]*TaskExecution, error)
	GetByUniqueKey(workflowExecutionID, uniqueKey, state string) ([]*TaskExecution, error)
	Update(t *TaskExecution) error
	UpdateState(id string, curState, newState string) (*TaskExecution, error)
	Count(workflowExecutionID, state string) (int, error)
	Delete(id string) error
}

// ActionExecutionStore defines CRUD operations for action executions.
type ActionExecutionStore interface {
	Create(a *ActionExecution) error
	Get(id string) (*ActionExecution, error)
	List(taskExecutionID, state string) ([]*ActionExecution, error)
	Update(a *ActionExecution) error
	Delete(id string) error
}

// EnvironmentStore defines CRUD operations for environments.
type EnvironmentStore interface {
	Create(e *Environment) error
	Get(id string) (*Environment, error)
	GetByName(name, projectID string) (*Environment, error)
	List(projectID string) ([]*Environment, error)
	Update(e *Environment) error
	Delete(id string) error
}

// CronTriggerStore defines CRUD for cron trigger definitions.
type CronTriggerStore interface {
	Create(ct *CronTrigger) error
	Get(id string) (*CronTrigger, error)
	GetByName(name string) (*CronTrigger, error)
	List() ([]*CronTrigger, error)
	Update(ct *CronTrigger) error
	Delete(id string) error
}

// Store is the aggregate database interface that combines all entity stores.
type Store interface {
	WorkflowDefinitions() WorkflowDefinitionStore
	ActionDefinitions() ActionDefinitionStore
	WorkflowExecutions() WorkflowExecutionStore
	TaskExecutions() TaskExecutionStore
	ActionExecutions() ActionExecutionStore
	Environments() EnvironmentStore
	CronTriggers() CronTriggerStore

	// Lock management (named locks for concurrency control).
	AcquireLock(lockName string) error
	ReleaseLock(lockName string) error

	// Transaction management.
	RunInTransaction(fn func(Store) error) error
}

// NotFoundError is returned when a requested entity is not found.
type NotFoundError struct {
	Entity string
	ID     string
}

func (e *NotFoundError) Error() string {
	return e.Entity + " not found: " + e.ID
}

// ConflictError is returned on duplicate key conflicts.
type ConflictError struct {
	Entity string
	Field  string
	Value  string
}

func (e *ConflictError) Error() string {
	return e.Entity + " conflict: " + e.Field + "=" + e.Value
}

// ExecutionFieldSizeError is returned when a field exceeds configured size limit.
type ExecutionFieldSizeError struct {
	Field string
	Size  int64
	Limit int64
}

func (e *ExecutionFieldSizeError) Error() string {
	return "field " + e.Field + " exceeds size limit"
}

// Now returns the current UTC time.
func Now() time.Time {
	return time.Now().UTC()
}
