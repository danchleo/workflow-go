// Package db defines database models, interfaces, and implementations.
//
// Ported from mistral-master/mistral/db/v2/sqlalchemy/models.py
//
// Core execution entity relationships:
//
//	WorkflowExecution (1) ──< (N) TaskExecution (1) ──< (N) ActionExecution
//	        │                        └──< (N) WorkflowExecution (sub-workflow)
//	        └── root_execution_id (self-referencing for sub-workflows)
//
// Definition entities store the YAML DSL parsed specifications.
package db

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// --- Validation constants (mirror Python SQLAlchemy validators) ---

// MaxStateInfoLen caps state_info string length. Python's SQLAlchemy column
// uses a 65535-length TEXT; we cap at 32768 to keep logs readable.
const MaxStateInfoLen = 32768

// MaxNameLen caps name fields.
const MaxNameLen = 255

// validateName checks that a workflow/task/action name doesn't contain
// whitespace (Python has a @validates hook for this).
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("name length %d exceeds max %d", len(name), MaxNameLen)
	}
	if strings.ContainsAny(name, " \t\n") {
		return fmt.Errorf("name %q must not contain whitespace", name)
	}
	return nil
}

// truncateStateInfo auto-truncates state_info strings that are too long.
// Python has this via SQLAlchemy before_insert hook.
func truncateStateInfo(info interface{}) interface{} {
	if s, ok := info.(string); ok && len(s) > MaxStateInfoLen {
		return s[:MaxStateInfoLen] + "...(truncated)"
	}
	return info
}

// ValidateWorkflowDefinition validates before save.
func (w *WorkflowDefinition) Validate() error {
	return validateName(w.Name)
}

// ValidateWorkflowExecution validates before save.
func (w *WorkflowExecution) Validate() {
	w.StateInfo = truncateStateInfo(w.StateInfo)
}

// ValidateTaskExecution validates before save.
func (t *TaskExecution) Validate() {
	t.StateInfo = truncateStateInfo(t.StateInfo)
}

// ValidateActionExecution validates before save.
func (a *ActionExecution) Validate() {
	a.StateInfo = truncateStateInfo(a.StateInfo)
}

// GenerateID returns a new UUID string.
func GenerateID() string {
	return uuid.New().String()
}

// --- Definition models ---

// WorkflowDefinition stores a workflow's DSL definition.
type WorkflowDefinition struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Namespace    string                 `json:"namespace"`
	Definition   string                 `json:"definition"` // Raw YAML
	Spec         map[string]interface{} `json:"spec"`       // Parsed spec
	Tags         []string               `json:"tags"`
	IsSystem     bool                   `json:"is_system"`
	ProjectID    string                 `json:"project_id"`
	Scope        string                 `json:"scope"`
	WorkbookName string                 `json:"workbook_name"`
	Checksum     string                 `json:"checksum"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// ActionDefinition stores a custom action's definition.
type ActionDefinition struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Namespace    string                 `json:"namespace"`
	Description  string                 `json:"description"`
	Input        string                 `json:"input"`
	ActionClass  string                 `json:"action_class"`
	Attributes   map[string]interface{} `json:"attributes"`
	IsSystem     bool                   `json:"is_system"`
	ProjectID    string                 `json:"project_id"`
	Scope        string                 `json:"scope"`
	WorkbookName string                 `json:"workbook_name"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// Workbook stores a workbook definition.
type Workbook struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace"`
	Definition string    `json:"definition"`
	Tags       []string  `json:"tags"`
	IsSystem   bool      `json:"is_system"`
	ProjectID  string    `json:"project_id"`
	Scope      string    `json:"scope"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Environment stores environment variables for workflow execution.
type Environment struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Variables   map[string]interface{} `json:"variables"`
	ProjectID   string                 `json:"project_id"`
	Scope       string                 `json:"scope"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

// --- Execution models ---

// WorkflowExecution represents a running or completed workflow execution.
type WorkflowExecution struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	WorkflowName      string                 `json:"workflow_name"`
	WorkflowNamespace string                 `json:"workflow_namespace"`
	WorkflowID        string                 `json:"workflow_id"`
	Spec              map[string]interface{} `json:"spec"`
	State             string                 `json:"state"`
	StateInfo         interface{}            `json:"state_info"`
	Tags              []string               `json:"tags"`
	Accepted          bool                   `json:"accepted"`
	Input             map[string]interface{} `json:"input"`
	Output            map[string]interface{} `json:"output"`
	Params            map[string]interface{} `json:"params"`
	Context           map[string]interface{} `json:"context"`
	RuntimeContext    map[string]interface{} `json:"runtime_context"`
	TaskExecutionID   string                 `json:"task_execution_id"`
	RootExecutionID   string                 `json:"root_execution_id"`
	ProjectID         string                 `json:"project_id"`
	Scope             string                 `json:"scope"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// TaskExecution represents a task within a workflow execution.
type TaskExecution struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name"`
	Description         string                 `json:"description"`
	WorkflowExecutionID string                 `json:"workflow_execution_id"`
	WorkflowName        string                 `json:"workflow_name"`
	WorkflowNamespace   string                 `json:"workflow_namespace"`
	WorkflowID          string                 `json:"workflow_id"`
	Spec                map[string]interface{} `json:"spec"`
	State               string                 `json:"state"`
	StateInfo           interface{}            `json:"state_info"`
	Tags                []string               `json:"tags"`
	Accepted            bool                   `json:"accepted"`
	UniqueKey           string                 `json:"unique_key"`
	Type                string                 `json:"type"` // ACTION or WORKFLOW
	StartedAt           *time.Time             `json:"started_at"`
	FinishedAt          *time.Time             `json:"finished_at"`
	Processed           bool                   `json:"processed"`
	HasNextTasks        bool                   `json:"has_next_tasks"`
	NextTasks           [][]interface{}        `json:"next_tasks"`
	ErrorHandled        bool                   `json:"error_handled"`
	InContext           map[string]interface{} `json:"in_context"`
	Published           map[string]interface{} `json:"published"`
	Output              map[string]interface{} `json:"output"`
	RuntimeContext      map[string]interface{} `json:"runtime_context"`
	ProjectID           string                 `json:"project_id"`
	Scope               string                 `json:"scope"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`

	// Child executions (populated from relationships).
	ActionExecutions   []*ActionExecution   `json:"-"`
	WorkflowExecutions []*WorkflowExecution `json:"-"`
}

// Executions returns child executions based on task type.
func (t *TaskExecution) Executions() []interface{} {
	if t.Type == "WORKFLOW" {
		result := make([]interface{}, len(t.WorkflowExecutions))
		for i, wf := range t.WorkflowExecutions {
			result[i] = wf
		}
		return result
	}
	result := make([]interface{}, len(t.ActionExecutions))
	for i, a := range t.ActionExecutions {
		result[i] = a
	}
	return result
}

// ActionExecution represents a single action invocation.
type ActionExecution struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	WorkflowName      string                 `json:"workflow_name"`
	WorkflowNamespace string                 `json:"workflow_namespace"`
	WorkflowID        string                 `json:"workflow_id"`
	TaskExecutionID   string                 `json:"task_execution_id"`
	State             string                 `json:"state"`
	StateInfo         interface{}            `json:"state_info"`
	Accepted          bool                   `json:"accepted"`
	Input             map[string]interface{} `json:"input"`
	Output            map[string]interface{} `json:"output"`
	RuntimeContext    map[string]interface{} `json:"runtime_context"`
	LastHeartbeat     *time.Time             `json:"last_heartbeat"`
	IsSync            *bool                  `json:"is_sync"`
	ProjectID         string                 `json:"project_id"`
	Scope             string                 `json:"scope"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// ScheduledJob is a persistent delayed-job record — the Go equivalent of
// Python's scheduled_jobs_v2 table (mistral/db/v2/sqlalchemy/models.py).
// On Schedule() we persist to DB first, then push to in-memory heap.
// On Start() we recover from DB. On crash, jobs survive to be picked up
// by jobStoreChecker on next start.
//
// Key fields that enable multi-process safety:
//
//	captured_at — optimistic lock. UPDATE ... WHERE captured_at = <old>.
//	If another scheduler already captured it, UPDATE affects 0 rows → skip.
type ScheduledJob struct {
	ID         string                 `json:"id"`
	JobType    string                 `json:"job_type"` // e.g. "fail_if_incomplete", "retry_create"
	JobArgs    map[string]interface{} `json:"job_args"`
	ExecuteAt  time.Time              `json:"execute_at"`
	CapturedAt *time.Time             `json:"captured_at,omitempty"` // nil = not yet captured
	Key        string                 `json:"key"`
	CreatedAt  time.Time              `json:"created_at"`
	// AuthCtx carries auth/tenant context from the caller that scheduled the job.
	// The scheduler does not interpret it — it is simply forwarded back to
	// handlers when the job fires, mirroring Python's auth_ctx propagation.
	// Typical contents: {"trust_id": "...", "project_id": "...", "namespace": "..."}.
	AuthCtx map[string]interface{} `json:"auth_ctx,omitempty"`
}

// CronTrigger stores cron trigger definitions.
type CronTrigger struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name"`
	Pattern             string                 `json:"pattern"`
	IntervalSeconds     int                    `json:"interval_seconds"` // simple interval (v1)
	FirstExecutionTime  *time.Time             `json:"first_execution_time"`
	NextExecutionTime   time.Time              `json:"next_execution_time"`
	WorkflowName        string                 `json:"workflow_name"`
	RemainingExecutions int                    `json:"remaining_executions"` // 0 = unlimited
	WorkflowID          string                 `json:"workflow_id"`
	WorkflowParams      map[string]interface{} `json:"workflow_params"`
	WorkflowInput       map[string]interface{} `json:"workflow_input"`
	ProjectID           string                 `json:"project_id"`
	Scope               string                 `json:"scope"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`
}

// NamedLock mirrors Python mistral/db/v2/sqlalchemy/models.py NamedLock.
//
// Used as a distributed mutex across scheduler workers: a worker acquires
// the lock by attempting to Insert a row with unique (Name) — two workers
// can only succeed with one INSERT; the other must wait for the first to
// Delete + commit. In Go's GORM layer the same semantics hold via GORM's
// unique index + transactional isolation. The table should never contain
// committed rows — all usage is around transactional play.
type NamedLock struct {
	ID        string    `json:"id"`
	Name      string    `json:"name" gorm:"uniqueIndex"`
	CreatedAt time.Time `json:"created_at"`
}

// Validate checks invariants.
func (nl *NamedLock) Validate() error {
	if nl.Name == "" {
		return fmt.Errorf("NamedLock.name required")
	}
	return nil
}

// DynamicActionDefinition mirrors Python mistral/db/v2/sqlalchemy/models.py
// DynamicActionDefinition. Registers a dynamically-loaded action at runtime.
// In Python this pairs with CodeSource (a code blob). The Go version is
// simplified: no CodeSource model (embedded code is a separate problem) —
// just metadata about a registered dynamic action so the engine can find it
// by (name, namespace, project).
type DynamicActionDefinition struct {
	ID           string    `json:"id"`
	Name         string    `json:"name" gorm:"uniqueIndex:uniq_dyn_action"`
	Namespace    string    `json:"namespace,omitempty" gorm:"uniqueIndex:uniq_dyn_action"`
	ProjectID    string    `json:"project_id" gorm:"uniqueIndex:uniq_dyn_action"`
	Scope        string    `json:"scope,omitempty"`
	ClassName    string    `json:"class_name"`
	CodeSourceID string    `json:"code_source_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (d *DynamicActionDefinition) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("DynamicActionDefinition.name required")
	}
	return nil
}

// EventTrigger mirrors Python mistral/db/v2/sqlalchemy/models.py EventTrigger.
// Maps an external event (exchange + topic + event) to a workflow definition.
// The engine's event handler calls events.Subscribe() for each row.
type EventTrigger struct {
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	ProjectID      string                 `json:"project_id" gorm:"uniqueIndex:uniq_evt_trigger"`
	Scope          string                 `json:"scope,omitempty"`
	WorkflowID     string                 `json:"workflow_id" gorm:"uniqueIndex:uniq_evt_trigger"`
	WorkflowParams map[string]interface{} `json:"workflow_params,omitempty"`
	WorkflowInput  map[string]interface{} `json:"workflow_input,omitempty"`
	Exchange       string                 `json:"exchange" gorm:"uniqueIndex:uniq_evt_trigger"`
	Topic          string                 `json:"topic" gorm:"uniqueIndex:uniq_evt_trigger"`
	Event          string                 `json:"event" gorm:"uniqueIndex:uniq_evt_trigger"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

func (e *EventTrigger) Validate() error {
	if e.WorkflowID == "" {
		return fmt.Errorf("EventTrigger.workflow_id required")
	}
	if e.Exchange == "" || e.Topic == "" || e.Event == "" {
		return fmt.Errorf("EventTrigger requires exchange, topic, event")
	}
	return nil
}

// ResourceMember mirrors Python mistral/db/v2/sqlalchemy/models.py ResourceMember.
// Tracks who has what role on a resource (workflow, workbook, action).
type ResourceMember struct {
	ID           string    `json:"id"`
	ResourceID   string    `json:"resource_id" gorm:"uniqueIndex:uniq_member"`
	ResourceType string    `json:"resource_type" gorm:"uniqueIndex:uniq_member"` // "workflow" / "workbook" / "action"
	ProjectID    string    `json:"project_id"`
	MemberID     string    `json:"member_id" gorm:"uniqueIndex:uniq_member"`
	Status       string    `json:"status"` // "pending" / "accepted"
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (r *ResourceMember) Validate() error {
	if r.ResourceID == "" || r.MemberID == "" {
		return fmt.Errorf("ResourceMember requires resource_id and member_id")
	}
	if r.ResourceType == "" {
		r.ResourceType = "workflow"
	}
	if r.Status == "" {
		r.Status = "pending"
	}
	return nil
}

// Result is the standard action/workflow result type.
type Result struct {
	Data   interface{} `json:"data,omitempty"`
	Error  interface{} `json:"error,omitempty"`
	Cancel bool        `json:"cancel,omitempty"`
}

// IsSuccess reports whether this result represents success.
func (r *Result) IsSuccess() bool {
	return r.Error == nil && !r.Cancel
}

// IsCancel reports whether this result represents cancellation.
func (r *Result) IsCancel() bool {
	return r.Cancel
}

// ToDict converts to map representation.
func (r *Result) ToDict() map[string]interface{} {
	result := make(map[string]interface{})
	if r.Data != nil {
		result["result"] = r.Data
	}
	if r.Error != nil {
		result["error"] = r.Error
	}
	return result
}

// FromDict populates Result fields from a map (used by HttpEngineClient).
func (r *Result) FromDict(d map[string]interface{}) {
	if v, ok := d["result"]; ok {
		r.Data = v
	} else if v, ok := d["data"]; ok {
		r.Data = v
	}
	if v, ok := d["error"]; ok {
		r.Error = v
	}
	if v, ok := d["cancel"]; ok {
		if b, ok := v.(bool); ok {
			r.Cancel = b
		}
	}
}

// CodeSource mirrors Python mistral/db/v2/sqlalchemy/models.py CodeSource.
// Stores a blob of source code paired with DynamicActionDefinition —
// the Go engine rarely loads this at runtime (user-defined dynamic actions
// are compiled out-of-band), but the model is required so that dynamic
// action metadata stays round-trip compatible with the Python API surface.
type CodeSource struct {
	ID        string    `json:"id"`
	Name      string    `json:"name" gorm:"uniqueIndex:uniq_code_source"`
	Namespace string    `json:"namespace,omitempty" gorm:"uniqueIndex:uniq_code_source"`
	Content   string    `json:"content"`
	Version   int       `json:"version"`
	Tags      []string  `json:"tags"`
	ProjectID string    `json:"project_id" gorm:"uniqueIndex:uniq_code_source"`
	Scope     string    `json:"scope,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (c *CodeSource) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("CodeSource.name required")
	}
	return nil
}

// MistralMetrics mirrors Python mistral/db/v2/sqlalchemy/models.py MistralMetrics.
// Single-row-style key/value for maintenance mode and other engine-wide toggles.
// Known keys (from Python api/controllers/maintenance.py): "maintenance".
type MistralMetrics struct {
	ID    int    `json:"id" gorm:"primaryKey;autoIncrement"`
	Name  string `json:"name" gorm:"uniqueIndex"`
	Value string `json:"value"`
}
