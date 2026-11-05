// Package sqlmodel: full-schema GORM models that mirror Python reference
// mistral/db/v2/sqlalchemy/models.py exactly column-for-column.
//
// This package is the single source of truth for table columns, sizes, and
// indexes. gorm/gen (via ApplyBasic) reads these structs and emits type-safe
// DAO into gen/query/. AutoMigrate in internal/db/sql.go consumes the same
// structs to create matching tables.
//
// JSON-typed Python columns (JsonDictType, JsonLongDictType, JsonListType,
// JsonMediumDictType) map to Go map/slice fields with
// `gorm:"serializer:json;type:text"` — GORM transparently marshals them
// to TEXT columns and unmarshals on read.
//
// Column type sizes follow Python SQLAlchemy (String(255), String(36),
// MediumText, etc.) so a Python Mistral instance and this Go service
// can share the same database.
package sqlmodel

import (
	"time"
)

// =============================================================================
// Base column bundles (mirror Python MistralModelBase / MistralSecureModelBase)
// =============================================================================

// TSTimestampColumns holds created_at + updated_at — every table has these via
// oslo_models.TimestampMixin in Python's MistralModelBase.
type TSTimestampColumns struct {
	CreatedAt time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt *time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at,omitempty"`
}

// SSecureColumns holds project_id + scope — every MistralSecureModelBase
// subclass gets these.
type SSecureColumns struct {
	ProjectID string `gorm:"column:project_id;size:80;default:''" json:"project_id"`
	Scope     string `gorm:"column:scope;size:80;default:'private'" json:"scope"`
}

// JD is the Go mirror of Python's JsonDictType / JsonLongDictType / JsonMediumDictType.
// GORM serializes it via `serializer:json`.
type JD = map[string]interface{}

// JL is the Go mirror of Python's JsonListType.
type JL = []interface{}

// =============================================================================
// Definition tables (Definition → MistralSecureModelBase)
// =============================================================================

type GWorkbookModel struct {
	ID         string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name       string `gorm:"column:name;size:255" json:"name"`
	Namespace  string `gorm:"column:namespace;size:255" json:"namespace"`
	Definition string `gorm:"column:definition;type:text" json:"definition"`
	Spec       JD     `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	Tags       JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	IsSystem   bool   `gorm:"column:is_system" json:"is_system"`
	SSecureColumns
	TSTimestampColumns
}

func (GWorkbookModel) TableName() string { return "workbooks_v2" }

type GWfDefModel struct {
	ID           string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name         string `gorm:"column:name;size:255" json:"name"`
	Namespace    string `gorm:"column:namespace;size:255" json:"namespace"`
	Definition   string `gorm:"column:definition;type:text" json:"definition"`
	Spec         JD     `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	Tags         JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	IsSystem     bool   `gorm:"column:is_system" json:"is_system"`
	Checksum     string `gorm:"column:checksum;size:32" json:"checksum"`
	WorkbookName string `gorm:"column:workbook_name;size:255" json:"workbook_name"`
	SSecureColumns
	TSTimestampColumns
}

func (GWfDefModel) TableName() string { return "workflow_definitions_v2" }

type GActDefModel struct {
	ID           string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name         string `gorm:"column:name;size:255" json:"name"`
	Namespace    string `gorm:"column:namespace;size:255" json:"namespace"`
	Definition   string `gorm:"column:definition;type:text" json:"definition"`
	Spec         JD     `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	Tags         JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	IsSystem     bool   `gorm:"column:is_system" json:"is_system"`
	WorkbookName string `gorm:"column:workbook_name;size:255" json:"workbook_name"`
	Description  string `gorm:"column:description;type:text" json:"description"`
	Input        string `gorm:"column:input;type:text" json:"input"`
	ActionClass  string `gorm:"column:action_class;size:200" json:"action_class"`
	Attributes   JD     `gorm:"column:attributes;type:text;serializer:json" json:"attributes"`
	SSecureColumns
	TSTimestampColumns
}

func (GActDefModel) TableName() string { return "action_definitions_v2" }

// =============================================================================
// Execution tables (Execution → MistralSecureModelBase)
// =============================================================================

type GWfExModel struct {
	ID                string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name              string `gorm:"column:name;size:255" json:"name"`
	Description       string `gorm:"column:description;size:255" json:"description"`
	WorkflowName      string `gorm:"column:workflow_name;size:255" json:"workflow_name"`
	WorkflowNamespace string `gorm:"column:workflow_namespace;size:255" json:"workflow_namespace"`
	WorkflowID        string `gorm:"column:workflow_id;size:80" json:"workflow_id"`
	State             string `gorm:"column:state;size:20" json:"state"`
	StateInfo         string `gorm:"column:state_info;type:text" json:"state_info"`
	Tags              JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	RuntimeContext    JD     `gorm:"column:runtime_context;type:text;serializer:json" json:"runtime_context"`

	Spec            JD     `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	Accepted        bool   `gorm:"column:accepted;default:false" json:"accepted"`
	Input           JD     `gorm:"column:input;type:text;serializer:json" json:"input"`
	Output          JD     `gorm:"column:output;type:text;serializer:json" json:"output"`
	Params          JD     `gorm:"column:params;type:text;serializer:json" json:"params"`
	Context         JD     `gorm:"column:context;type:text;serializer:json" json:"context"`
	TaskExecutionID string `gorm:"column:task_execution_id;size:36" json:"task_execution_id"`
	RootExecutionID string `gorm:"column:root_execution_id;size:36" json:"root_execution_id"`
	SSecureColumns
	TSTimestampColumns
}

func (GWfExModel) TableName() string { return "workflow_executions_v2" }

type GTaskExModel struct {
	ID                string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name              string `gorm:"column:name;size:255" json:"name"`
	Description       string `gorm:"column:description;size:255" json:"description"`
	WorkflowName      string `gorm:"column:workflow_name;size:255" json:"workflow_name"`
	WorkflowNamespace string `gorm:"column:workflow_namespace;size:255" json:"workflow_namespace"`
	WorkflowID        string `gorm:"column:workflow_id;size:80" json:"workflow_id"`
	State             string `gorm:"column:state;size:20" json:"state"`
	StateInfo         string `gorm:"column:state_info;type:text" json:"state_info"`
	Tags              JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	RuntimeContext    JD     `gorm:"column:runtime_context;type:text;serializer:json" json:"runtime_context"`

	Spec                JD         `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	ActionSpec          JD         `gorm:"column:action_spec;type:text;serializer:json" json:"action_spec"`
	UniqueKey           string     `gorm:"column:unique_key;size:255;uniqueIndex" json:"unique_key"`
	Type                string     `gorm:"column:type;size:10" json:"type"`
	StartedAt           *time.Time `gorm:"column:started_at" json:"started_at,omitempty"`
	FinishedAt          *time.Time `gorm:"column:finished_at" json:"finished_at,omitempty"`
	Processed           bool       `gorm:"column:processed;default:false" json:"processed"`
	HasNextTasks        bool       `gorm:"column:has_next_tasks;default:false" json:"has_next_tasks"`
	NextTasks           JL         `gorm:"column:next_tasks;type:text;serializer:json" json:"next_tasks"`
	ErrorHandled        bool       `gorm:"column:error_handled;default:false" json:"error_handled"`
	InContext           JD         `gorm:"column:in_context;type:text;serializer:json" json:"in_context"`
	Published           JD         `gorm:"column:published;type:text;serializer:json" json:"published"`
	Output              JD         `gorm:"column:output;type:text;serializer:json" json:"output"`
	WorkflowExecutionID string     `gorm:"column:workflow_execution_id;size:36;index" json:"workflow_execution_id"`
	SSecureColumns
	TSTimestampColumns
}

func (GTaskExModel) TableName() string { return "task_executions_v2" }

type GActExModel struct {
	ID                string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name              string `gorm:"column:name;size:255" json:"name"`
	Description       string `gorm:"column:description;size:255" json:"description"`
	WorkflowName      string `gorm:"column:workflow_name;size:255" json:"workflow_name"`
	WorkflowNamespace string `gorm:"column:workflow_namespace;size:255" json:"workflow_namespace"`
	WorkflowID        string `gorm:"column:workflow_id;size:80" json:"workflow_id"`
	State             string `gorm:"column:state;size:20" json:"state"`
	StateInfo         string `gorm:"column:state_info;type:text" json:"state_info"`
	Tags              JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	RuntimeContext    JD     `gorm:"column:runtime_context;type:text;serializer:json" json:"runtime_context"`

	Spec            JD         `gorm:"column:spec;type:text;serializer:json" json:"spec"`
	Accepted        bool       `gorm:"column:accepted;default:false" json:"accepted"`
	Input           JD         `gorm:"column:input;type:text;serializer:json" json:"input"`
	Output          JD         `gorm:"column:output;type:text;serializer:json" json:"output"`
	TaskExecutionID string     `gorm:"column:task_execution_id;size:36;index" json:"task_execution_id"`
	LastHeartbeat   *time.Time `gorm:"column:last_heartbeat" json:"last_heartbeat,omitempty"`
	IsSync          *bool      `gorm:"column:is_sync" json:"is_sync,omitempty"`
	SSecureColumns
	TSTimestampColumns
}

func (GActExModel) TableName() string { return "action_executions_v2" }

// =============================================================================
// Other tables
// =============================================================================

type GEnvModel struct {
	ID          string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name        string `gorm:"column:name;size:200" json:"name"`
	Description string `gorm:"column:description;type:text" json:"description"`
	Variables   JD     `gorm:"column:variables;type:text;serializer:json" json:"variables"`
	SSecureColumns
	TSTimestampColumns
}

func (GEnvModel) TableName() string { return "environments_v2" }

type GCronTriggerModel struct {
	ID                  string     `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name                string     `gorm:"column:name;size:200" json:"name"`
	Pattern             string     `gorm:"column:pattern;size:100;default:'0 0 30 2 0'" json:"pattern"`
	IntervalSeconds     int        `gorm:"column:interval_seconds;default:0" json:"interval_seconds"`
	FirstExecutionTime  *time.Time `gorm:"column:first_execution_time" json:"first_execution_time,omitempty"`
	NextExecutionTime   time.Time  `gorm:"column:next_execution_time;not null;index" json:"next_execution_time"`
	WorkflowName        string     `gorm:"column:workflow_name;size:255" json:"workflow_name"`
	RemainingExecutions int        `gorm:"column:remaining_executions" json:"remaining_executions"`
	WorkflowID          string     `gorm:"column:workflow_id;size:36" json:"workflow_id"`
	WorkflowParams      JD         `gorm:"column:workflow_params;type:text;serializer:json" json:"workflow_params"`
	WorkflowInput       JD         `gorm:"column:workflow_input;type:text;serializer:json" json:"workflow_input"`
	TrustID             string     `gorm:"column:trust_id;size:80" json:"trust_id"`
	SSecureColumns
	TSTimestampColumns
}

func (GCronTriggerModel) TableName() string { return "cron_triggers_v2" }

type GScheduledJobModel struct {
	ID                    string     `gorm:"column:id;size:36;primaryKey" json:"id"`
	RunAfter              int        `gorm:"column:run_after" json:"run_after"`
	TargetFactoryFuncName string     `gorm:"column:target_factory_func_name;size:200" json:"target_factory_func_name"`
	FuncName              string     `gorm:"column:func_name;size:80;not null" json:"func_name"`
	FuncArgs              JD         `gorm:"column:func_args;type:text;serializer:json" json:"func_args"`
	FuncArgSerializers    JD         `gorm:"column:func_arg_serializers;type:text;serializer:json" json:"func_arg_serializers"`
	AuthCtx               JD         `gorm:"column:auth_ctx;type:text;serializer:json" json:"auth_ctx"`
	ExecuteAt             time.Time  `gorm:"column:execute_at;not null;index" json:"execute_at"`
	CapturedAt            *time.Time `gorm:"column:captured_at" json:"captured_at,omitempty"`
	Key                   string     `gorm:"column:key;size:250;index" json:"key"`
	CreatedAt             time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
}

func (GScheduledJobModel) TableName() string { return "scheduled_jobs_v2" }

type GNamedLockModel struct {
	ID   string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name string `gorm:"column:name;size:255;uniqueIndex" json:"name"`
	TSTimestampColumns
}

func (GNamedLockModel) TableName() string { return "named_locks" }

type GDynamicActionDefModel struct {
	ID             string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name           string `gorm:"column:name;size:255" json:"name"`
	Namespace      string `gorm:"column:namespace;size:255" json:"namespace"`
	ClassName      string `gorm:"column:class_name;size:255" json:"class_name"`
	CodeSourceName string `gorm:"column:code_source_name;size:255" json:"code_source_name"`
	CodeSourceID   string `gorm:"column:code_source_id;size:36;not null" json:"code_source_id"`
	SSecureColumns
	TSTimestampColumns
}

func (GDynamicActionDefModel) TableName() string { return "dynamic_action_definitions" }

type GCodeSourceModel struct {
	ID        string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name      string `gorm:"column:name;size:255" json:"name"`
	Content   string `gorm:"column:content;type:text" json:"content"`
	Version   int    `gorm:"column:version" json:"version"`
	Namespace string `gorm:"column:namespace;size:255" json:"namespace"`
	Tags      JL     `gorm:"column:tags;type:text;serializer:json" json:"tags"`
	SSecureColumns
	TSTimestampColumns
}

func (GCodeSourceModel) TableName() string { return "code_sources" }

type GEventTriggerModel struct {
	ID             string `gorm:"column:id;size:36;primaryKey" json:"id"`
	Name           string `gorm:"column:name;size:200" json:"name"`
	WorkflowID     string `gorm:"column:workflow_id;size:36" json:"workflow_id"`
	WorkflowParams JD     `gorm:"column:workflow_params;type:text;serializer:json" json:"workflow_params"`
	WorkflowInput  JD     `gorm:"column:workflow_input;type:text;serializer:json" json:"workflow_input"`
	Exchange       string `gorm:"column:exchange;size:80;not null" json:"exchange"`
	Topic          string `gorm:"column:topic;size:80;not null" json:"topic"`
	Event          string `gorm:"column:event;size:80;not null" json:"event"`
	TrustID        string `gorm:"column:trust_id;size:80" json:"trust_id"`
	SSecureColumns
	TSTimestampColumns
}

func (GEventTriggerModel) TableName() string { return "event_triggers_v2" }

type GResourceMemberModel struct {
	ID           string `gorm:"column:id;size:36;primaryKey" json:"id"`
	ResourceID   string `gorm:"column:resource_id;size:80;not null" json:"resource_id"`
	ResourceType string `gorm:"column:resource_type;size:50;not null;default:'workflow'" json:"resource_type"`
	SSecureColumns
	MemberID string `gorm:"column:member_id;size:80;not null" json:"member_id"`
	Status   string `gorm:"column:status;size:20;not null;default:'pending'" json:"status"`
	TSTimestampColumns
}

func (GResourceMemberModel) TableName() string { return "resource_members_v2" }

type GMetricsModel struct {
	ID    int    `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Name  string `gorm:"column:name;size:255;uniqueIndex;not null" json:"name"`
	Value string `gorm:"column:value;size:255;not null" json:"value"`
	TSTimestampColumns
}

func (GMetricsModel) TableName() string { return "mistral_metrics" }

type GDelayedCallModel struct {
	ID              string    `gorm:"column:id;size:36;primaryKey" json:"id"`
	FactoryMethod   string    `gorm:"column:factory_method_path;size:200" json:"factory_method_path"`
	TargetMethod    string    `gorm:"column:target_method_name;size:80;not null" json:"target_method_name"`
	MethodArguments JD        `gorm:"column:method_arguments;type:text;serializer:json" json:"method_arguments"`
	Serializers     JD        `gorm:"column:serializers;type:text;serializer:json" json:"serializers"`
	Key             string    `gorm:"column:key;size:250" json:"key"`
	AuthContext     JD        `gorm:"column:auth_context;type:text;serializer:json" json:"auth_context"`
	ExecutionTime   time.Time `gorm:"column:execution_time;not null;index" json:"execution_time"`
	Processing      bool      `gorm:"column:processing;default:false;not null" json:"processing"`
	TSTimestampColumns
}

func (GDelayedCallModel) TableName() string { return "delayed_calls_v2" }
