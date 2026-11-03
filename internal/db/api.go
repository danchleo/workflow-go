// Package db — database abstraction.
//
// All CRUD goes through gorm/gen (compiled type-safe DAO, zero drift risk).
// The Store interface aggregates XxxStore sub-interfaces; sqlStore
// implements them by delegating to gen query.Query — every call is
// a thin wrapper like:  s.store.Q().TableNameV2.WithContext(ctx).Where(...).First()
//
// Transaction + locks are also on Store (gen has no equivalent).
package db

import (
	"time"

	"github.com/danchleo/workflow-go/gen/query"
)

// Store lifecycle + aggregates all entity stores.
//
// CRUD:   via XxxStore() sub-interfaces (internally delegate to gen query.Query).
// Locks:  per-goroutine reentrant mutexes (Python-aligned named_lock model).
// Tx:     RawLock (process-level) + optional real SQL tx (PG/MySQL only).
type Store interface {
	// Q is the raw gorm/gen entry — callers that want the full gen API
	// (complex Where, Pluck, custom Clauses, etc.) use this directly.
	Q() *query.Query

	// --- CRUD stores ---
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
	Metrics() MetricsStore

	// --- Locks ---
	Lock(name string) ReentrantLock
	AcquireLock(lockName string) error
	ReleaseLock(lockName string) error

	// --- Transaction ---
	RunInTransaction(fn func(Store) error) error
}

// --- Entity store sub-interfaces (same as before; implementations delegate to gen) ---

type WorkflowDefinitionStore interface {
	Create(wf *WorkflowDefinition) error
	Get(id string) (*WorkflowDefinition, error)
	GetByName(name, namespace, projectID string) (*WorkflowDefinition, error)
	List(namespace, projectID string) ([]*WorkflowDefinition, error)
	Update(wf *WorkflowDefinition) error
	Delete(id string) error
}

type ActionDefinitionStore interface {
	Create(a *ActionDefinition) error
	Get(id string) (*ActionDefinition, error)
	GetByName(name, namespace, projectID string) (*ActionDefinition, error)
	List(namespace, projectID string) ([]*ActionDefinition, error)
	Update(a *ActionDefinition) error
	Delete(id string) error
}

type WorkflowExecutionStore interface {
	Create(wf *WorkflowExecution) error
	Get(id string) (*WorkflowExecution, error)
	List(projectID string) ([]*WorkflowExecution, error)
	Update(wf *WorkflowExecution) error
	UpdateState(id string, curState, newState string) (*WorkflowExecution, error)
	Delete(id string) error
}

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

type ActionExecutionStore interface {
	Create(a *ActionExecution) error
	Get(id string) (*ActionExecution, error)
	List(taskExecutionID, state string) ([]*ActionExecution, error)
	Update(a *ActionExecution) error
	Delete(id string) error
}

type EnvironmentStore interface {
	Create(e *Environment) error
	Get(id string) (*Environment, error)
	GetByName(name, projectID string) (*Environment, error)
	List(projectID string) ([]*Environment, error)
	Update(e *Environment) error
	Delete(id string) error
}

type ScheduledJobStore interface {
	Create(j *ScheduledJob) error
	Get(id string) (*ScheduledJob, error)
	List() ([]*ScheduledJob, error)
	GetDueJobs(executeCutoff, now time.Time, captureTimeout time.Duration, batchSize int) ([]*ScheduledJob, error)
	Capture(id string, oldCapturedAt *time.Time, now time.Time) (bool, error)
	Delete(id string) error
}

type CronTriggerStore interface {
	Create(ct *CronTrigger) error
	Get(id string) (*CronTrigger, error)
	GetByName(name string) (*CronTrigger, error)
	List() ([]*CronTrigger, error)
	Update(ct *CronTrigger) error
	Delete(id string) error
}

type WorkbookStore interface {
	Create(wb *Workbook) error
	Get(id string) (*Workbook, error)
	GetByName(name, namespace, projectID string) (*Workbook, error)
	List(namespace, projectID string) ([]*Workbook, error)
	Update(wb *Workbook) error
	Delete(id string) error
}

type DynamicActionDefinitionStore interface {
	Create(d *DynamicActionDefinition) error
	Get(id string) (*DynamicActionDefinition, error)
	GetByName(name, namespace, projectID string) (*DynamicActionDefinition, error)
	List(namespace, projectID string) ([]*DynamicActionDefinition, error)
	Update(d *DynamicActionDefinition) error
	Delete(id string) error
}

type EventTriggerStore interface {
	Create(e *EventTrigger) error
	Get(id string) (*EventTrigger, error)
	GetByExchangeTopicEvent(exchange, topic, event, projectID string) (*EventTrigger, error)
	List() ([]*EventTrigger, error)
	Update(e *EventTrigger) error
	Delete(id string) error
}

type ResourceMemberStore interface {
	Create(m *ResourceMember) error
	Get(id string) (*ResourceMember, error)
	ListByResource(resourceID, resourceType string) ([]*ResourceMember, error)
	Update(m *ResourceMember) error
	Delete(id string) error
}

type CodeSourceStore interface {
	Create(c *CodeSource) error
	Get(id string) (*CodeSource, error)
	GetByName(name, namespace, projectID string) (*CodeSource, error)
	List(namespace, projectID string) ([]*CodeSource, error)
	Update(c *CodeSource) error
	Delete(id string) error
}

type MetricsStore interface {
	GetByName(name string) (*Metrics, error)
	Set(name, value string) error
	Delete(name string) error
}

// --- Errors ---

type NotFoundError struct {
	Entity string
	ID     string
}

func (e *NotFoundError) Error() string {
	return e.Entity + " not found: " + e.ID
}

type ConflictError struct {
	Entity string
	Field  string
	Value  string
}

func (e *ConflictError) Error() string {
	return e.Entity + " conflict: " + e.Field + "=" + e.Value
}

type ExecutionFieldSizeError struct {
	Field string
	Size  int64
	Limit int64
}

func (e *ExecutionFieldSizeError) Error() string {
	return "field " + e.Field + " exceeds size limit"
}

func Now() time.Time { return time.Now().UTC() }
