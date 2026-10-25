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

// ScheduledJobStore defines CRUD + job-store-ops for persistent delayed jobs.
// Mirrors Python's db_api.create_scheduled_job / get_scheduled_jobs_to_start /
// update_scheduled_job / delete_scheduled_job.
type ScheduledJobStore interface {
	Create(j *ScheduledJob) error
	Get(id string) (*ScheduledJob, error)
	List() ([]*ScheduledJob, error)
	// GetDueJobs returns all jobs where execute_at <= executeCutoff that are
	// NOT captured (captured_at IS NULL) OR whose captured_at is older than
	// (now - captureTimeout) — i.e. stuck captures (previous scheduler crash).
	// batchSize = 0 means all.
	// executeCutoff — Python's (time - pickup_job_after). Only jobs whose
	//   execute_at has passed this cutoff are considered for DB pickup.
	//   heapDispatcher handles everything between executeCutoff and now.
	// now — current wall-clock time. Used ONLY for the captured_at lock check.
	GetDueJobs(executeCutoff time.Time, now time.Time, captureTimeout time.Duration, batchSize int) ([]*ScheduledJob, error)
	// Capture marks a job as captured for execution. Uses optimistic lock:
	// UPDATE ... SET captured_at = NOW() WHERE id = ? AND captured_at = oldCapturedAt.
	// Returns true if this call captured the job (affected rows == 1).
	Capture(id string, oldCapturedAt *time.Time, now time.Time) (bool, error)
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

// WorkbookStore defines CRUD for workbooks.
type WorkbookStore interface {
	Create(wb *Workbook) error
	Get(id string) (*Workbook, error)
	GetByName(name, namespace, projectID string) (*Workbook, error)
	List(namespace, projectID string) ([]*Workbook, error)
	Update(wb *Workbook) error
	Delete(id string) error
}

// DynamicActionDefinitionStore defines CRUD for dynamic action definitions.
type DynamicActionDefinitionStore interface {
	Create(d *DynamicActionDefinition) error
	Get(id string) (*DynamicActionDefinition, error)
	GetByName(name, namespace, projectID string) (*DynamicActionDefinition, error)
	List(namespace, projectID string) ([]*DynamicActionDefinition, error)
	Update(d *DynamicActionDefinition) error
	Delete(id string) error
}

// EventTriggerStore defines CRUD for event triggers.
type EventTriggerStore interface {
	Create(e *EventTrigger) error
	Get(id string) (*EventTrigger, error)
	GetByExchangeTopicEvent(exchange, topic, event, projectID string) (*EventTrigger, error)
	List() ([]*EventTrigger, error)
	Update(e *EventTrigger) error
	Delete(id string) error
}

// ResourceMemberStore defines CRUD for resource members.
type ResourceMemberStore interface {
	Create(m *ResourceMember) error
	Get(id string) (*ResourceMember, error)
	ListByResource(resourceID, resourceType string) ([]*ResourceMember, error)
	Update(m *ResourceMember) error
	Delete(id string) error
}

// CodeSourceStore defines CRUD for code sources.
type CodeSourceStore interface {
	Create(c *CodeSource) error
	Get(id string) (*CodeSource, error)
	GetByName(name, namespace, projectID string) (*CodeSource, error)
	List(namespace, projectID string) ([]*CodeSource, error)
	Update(c *CodeSource) error
	Delete(id string) error
}

// MistralMetricsStore defines CRUD for metrics key/value.
type MistralMetricsStore interface {
	GetByName(name string) (*MistralMetrics, error)
	Set(name, value string) error
	Delete(name string) error
}

// Store is the aggregate database interface that combines all entity stores.
type Store interface {
	WorkflowDefinitions() WorkflowDefinitionStore
	ActionDefinitions() ActionDefinitionStore
	WorkflowExecutions() WorkflowExecutionStore
	TaskExecutions() TaskExecutionStore
	ActionExecutions() ActionExecutionStore
	Environments() EnvironmentStore
	ScheduledJobs() ScheduledJobStore
	CronTriggers() CronTriggerStore
	Workbooks() WorkbookStore
	DynamicActionDefinitions() DynamicActionDefinitionStore
	EventTriggers() EventTriggerStore
	ResourceMembers() ResourceMemberStore
	CodeSources() CodeSourceStore
	MistralMetrics() MistralMetricsStore

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
