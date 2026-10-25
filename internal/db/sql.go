// Package db: SQL/GORM-backed Store implementation.
//
// This mirrors Python mistral-master/mistral/db/v2/sqlalchemy/api.py
// but uses gorm.io/gorm + gorm.io/driver/sqlite for the Go ecosystem.
// All interface{} fields (RuntimeContext, Input, Output, etc.) are
// serialized as JSON text columns.
package db

import (
	"encoding/json"
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewSQLStore creates a GORM-backed Store using SQLite (file or :memory:).
// Example: NewSQLStore("/tmp/mistral.db") or NewSQLStore(":memory:")
func NewSQLStore(dsn string) (Store, error) {
	var gormDB *gorm.DB
	var err error

	if dsn == ":memory:" {
		// SQLite :memory: opens a fresh empty DB per connection.
		// Force a single connection pool so AutoMigrate tables are visible
		// to all queries. file::memory:?cache=shared also works but requires
		// the URI form which some drivers handle inconsistently.
		gormDB, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
		if err != nil {
			return nil, fmt.Errorf("open sqlite :memory:: %w", err)
		}
		sqlDB, _ := gormDB.DB()
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	} else {
		gormDB, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
		if err != nil {
			return nil, fmt.Errorf("open sqlite %s: %w", dsn, err)
		}
	}
	if err := gormDB.AutoMigrate(
		&gWorkflowDef{}, &gActionDef{}, &gWfEx{}, &gTaskEx{}, &gActEx{}, &gEnv{}, &gCronTrigger{}, &gScheduledJob{},
		&gWorkbook{}, &gDynamicActionDef{}, &gEventTrigger{}, &gResourceMember{}, &gNamedLock{}, &gCodeSource{}, &gMistralMetrics{},
	); err != nil {
		return nil, fmt.Errorf("autmigrate: %w", err)
	}
	return &sqlStore{db: gormDB}, nil
}

// sqlStore is the top-level aggregate that implements Store.
type sqlStore struct {
	db        *gorm.DB
	locks     map[string]bool
	locksChan map[string]chan struct{}
}

func (s *sqlStore) WorkflowDefinitions() WorkflowDefinitionStore { return &sqlWfDefStoreSQL{db: s.db} }
func (s *sqlStore) ActionDefinitions() ActionDefinitionStore     { return &sqlActDefStoreSQL{db: s.db} }
func (s *sqlStore) WorkflowExecutions() WorkflowExecutionStore   { return &sqlWfExStoreSQL{db: s.db} }
func (s *sqlStore) TaskExecutions() TaskExecutionStore           { return &sqlTaskExStoreSQL{db: s.db} }
func (s *sqlStore) ActionExecutions() ActionExecutionStore       { return &sqlActExStoreSQL{db: s.db} }
func (s *sqlStore) Environments() EnvironmentStore               { return &sqlEnvStoreSQL{db: s.db} }
func (s *sqlStore) ScheduledJobs() ScheduledJobStore             { return &sqlScheduledJobStore{db: s.db} }
func (s *sqlStore) CronTriggers() CronTriggerStore               { return &sqlCronTriggerStore{db: s.db} }
func (s *sqlStore) Workbooks() WorkbookStore                     { return &sqlWorkbookStore{db: s.db} }
func (s *sqlStore) DynamicActionDefinitions() DynamicActionDefinitionStore {
	return &sqlDynamicActionDefStore{db: s.db}
}
func (s *sqlStore) EventTriggers() EventTriggerStore     { return &sqlEventTriggerStore{db: s.db} }
func (s *sqlStore) ResourceMembers() ResourceMemberStore { return &sqlResourceMemberStore{db: s.db} }
func (s *sqlStore) CodeSources() CodeSourceStore         { return &sqlCodeSourceStore{db: s.db} }
func (s *sqlStore) MistralMetrics() MistralMetricsStore  { return &sqlMistralMetricsStore{db: s.db} }

func (s *sqlStore) AcquireLock(name string) error {
	// Use an in-process named channel — good enough for :memory: SQLite
	// and single-process file SQLite. For multi-process coordination,
	// replace with a real advisory lock.
	if s.locks == nil {
		s.locks = make(map[string]bool)
		s.locksChan = make(map[string]chan struct{})
	}
	ch, ok := s.locksChan[name]
	if !ok {
		ch = make(chan struct{}, 1)
		s.locksChan[name] = ch
	}
	select {
	case ch <- struct{}{}:
		s.locks[name] = true
		return nil
	default:
		return fmt.Errorf("lock %s already held", name)
	}
}
func (s *sqlStore) ReleaseLock(name string) error {
	if s.locksChan == nil {
		return fmt.Errorf("lock %s not acquired", name)
	}
	ch, ok := s.locksChan[name]
	if !ok {
		return fmt.Errorf("lock %s not acquired", name)
	}
	select {
	case <-ch:
		s.locks[name] = false
	default:
	}
	return nil
}
func (s *sqlStore) RunInTransaction(fn func(Store) error) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		txn := &sqlStore{db: tx, locks: s.locks, locksChan: s.locksChan}
		return fn(txn)
	})
}

// --- JSON helpers: serialize interface{} to/from TEXT columns ---

func jsonMarshal(v interface{}) string {
	if v == nil {
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}
func jsonUnmarshal(s string, out interface{}) {
	if s == "" {
		return
	}
	_ = json.Unmarshal([]byte(s), out)
}

// --- Blob-pattern GORM models (defers detailed mapping to sql_stores.go) ---

type gWorkbook struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gWorkbook) TableName() string { return "workbooks" }

type gDynamicActionDef struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gDynamicActionDef) TableName() string { return "dynamic_action_definitions" }

type gEventTrigger struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gEventTrigger) TableName() string { return "event_triggers" }

type gResourceMember struct {
	ID   string `gorm:"primaryKey"`
	Data string
}

func (gResourceMember) TableName() string { return "resource_members" }

type gNamedLock struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"uniqueIndex"`
}

func (gNamedLock) TableName() string { return "named_locks" }

type gCodeSource struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gCodeSource) TableName() string { return "code_sources" }

type gMistralMetrics struct {
	ID    int    `gorm:"primaryKey;autoIncrement"`
	Name  string `gorm:"uniqueIndex"`
	Value string
}

func (gMistralMetrics) TableName() string { return "mistral_metrics" }
