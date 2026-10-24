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
	"time"

	"github.com/google/uuid"
)

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

// CronTrigger stores cron trigger definitions.
type CronTrigger struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name"`
	Pattern             string                 `json:"pattern"`
	FirstExecutionTime  *time.Time             `json:"first_execution_time"`
	NextExecutionTime   time.Time              `json:"next_execution_time"`
	WorkflowName        string                 `json:"workflow_name"`
	RemainingExecutions int                    `json:"remaining_executions"`
	WorkflowID          string                 `json:"workflow_id"`
	WorkflowParams      map[string]interface{} `json:"workflow_params"`
	WorkflowInput       map[string]interface{} `json:"workflow_input"`
	ProjectID           string                 `json:"project_id"`
	Scope               string                 `json:"scope"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`
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
