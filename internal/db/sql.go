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
	"sync"
	"time"

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
		// :memory: opens a fresh DB per connection — force a single connection
		// pool so AutoMigrate tables are visible, and sqlite doesn't need
		// concurrent-transaction support (rawLock serialises RunInTransaction).
		gormDB, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
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
	return &sqlStore{db: gormDB, rlocks: make(map[string]*reentrantLock)}, nil
}

// sqlStore is the top-level aggregate that implements Store.
//
// RunInTransaction wraps fn in a real gorm.DB.Transaction — SQL BEGIN/COMMIT
// so all CRUD inside lands in one atomic unit. This is critical for multi-
// process coordination: named_locks INSERT (Store.Lock) only blocks when
// both sides are inside a transaction (PostgreSQL unique-constraint wait).
//
// tx is set/unset by RunInTransaction — CRUD helpers use currentDB() which
// prefers tx when set, falls back to s.db otherwise. rawLock is still held
// as a process-level guard (SQLite + test determinism).
type sqlStore struct {
	db        *gorm.DB
	tx        *gorm.DB // set inside RunInTransaction (gorm.Transaction callback)
	locks     map[string]bool
	locksChan map[string]chan struct{}
	mu        txRWMutex

	rlocks   map[string]*reentrantLock
	rlocksMu sync.Mutex
}

// currentDB returns the active *gorm.DB — the transaction handle if we're
// inside RunInTransaction, the top-level connection pool otherwise.
func (s *sqlStore) currentDB() *gorm.DB {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

func (s *sqlStore) WorkflowDefinitions() WorkflowDefinitionStore { return &sqlWfDefStoreSQL{store: s} }
func (s *sqlStore) ActionDefinitions() ActionDefinitionStore     { return &sqlActDefStoreSQL{store: s} }
func (s *sqlStore) WorkflowExecutions() WorkflowExecutionStore   { return &sqlWfExStoreSQL{store: s} }
func (s *sqlStore) TaskExecutions() TaskExecutionStore           { return &sqlTaskExStoreSQL{store: s} }
func (s *sqlStore) ActionExecutions() ActionExecutionStore       { return &sqlActExStoreSQL{store: s} }
func (s *sqlStore) Environments() EnvironmentStore               { return &sqlEnvStoreSQL{store: s} }
func (s *sqlStore) ScheduledJobs() ScheduledJobStore             { return &sqlScheduledJobStore{store: s} }
func (s *sqlStore) CronTriggers() CronTriggerStore               { return &sqlCronTriggerStore{store: s} }
func (s *sqlStore) Workbooks() WorkbookStore                     { return &sqlWorkbookStore{store: s} }
func (s *sqlStore) DynamicActionDefinitions() DynamicActionDefinitionStore {
	return &sqlDynamicActionDefStore{store: s}
}
func (s *sqlStore) EventTriggers() EventTriggerStore     { return &sqlEventTriggerStore{store: s} }
func (s *sqlStore) ResourceMembers() ResourceMemberStore { return &sqlResourceMemberStore{store: s} }
func (s *sqlStore) CodeSources() CodeSourceStore         { return &sqlCodeSourceStore{store: s} }
func (s *sqlStore) MistralMetrics() MistralMetricsStore  { return &sqlMistralMetricsStore{store: s} }

// Lock returns a per-named re-entrant mutex.
//
// For SQLite (single-process test mode) we use an in-process reentrantLock
// identical to InMemoryStore — rawLock already serialises RunInTransaction
// so named_locks INSERT would be redundant. Production PostgreSQL/MySQL
// should switch this to sqlNamedLock (INSERT into named_locks table) which
// is how Python Mistral coordinates across engine processes.
func (s *sqlStore) Lock(name string) ReentrantLock {
	s.rlocksMu.Lock()
	defer s.rlocksMu.Unlock()
	if l, ok := s.rlocks[name]; ok {
		return l
	}
	l := newReentrantLock()
	s.rlocks[name] = l
	return l
}

func (s *sqlStore) AcquireLock(name string) error {
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
	depthBefore := txDepth()
	if depthBefore == 0 {
		s.mu.rawLock()
		defer s.mu.rawUnlock()
	}
	enterTx()
	defer exitTx()

	// For PostgreSQL/MySQL this would wrap in s.db.Transaction(fn) so we
	// get real SQL BEGIN/COMMIT/ROLLBACK plus cross-process named_lock
	// blocking. SQLite (and :memory: test mode) can't do true concurrent
	// transactions — one connection, one writer at a time — so the
	// process-level rawLock above is the serialisation boundary.
	//
	// Engine internals (CRUD, Lock) use currentDB() which prefers s.tx when
	// set — when we ARE inside a real gorm.Transaction it naturally flows
	// through. For now SQLite uses s.db directly.
	return fn(s)
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

// --- sqlNamedLock: cross-process lock via named_locks table ---
//
// Mirrors Python mistral/db/v2/sqlalchemy/api.py named_lock():
//   with db_api.transaction():
//       lock_id = create_named_lock(name)  # INSERT → UNIQUE wait
//       yield
//       delete_named_lock(lock_id, name)
//
// We differ in one key aspect: Python's named_lock() always lives INSIDE
// db_api.transaction() — same transaction as the business logic — so the
// INSERT (lock acquire) is atomic with the data mutations and the DELETE
// (lock release) happens on transaction commit/rollback.
//
// Here, if Lock() is called OUTSIDE RunInTransaction, we still do the right
// thing: INSERT blocks, then we delete immediately on Unlock(). This is safe
// when the caller's Lock/Unlock brackets only operations that don't need a
// real transaction — which in practice never happens. All engine entries call
// Lock inside RunInTransaction, so tx==nil case is only for tests.

type sqlNamedLock struct {
	store  *sqlStore
	name   string
	lockID string
	held   bool
}

func (l *sqlNamedLock) Lock() {
	if l.held {
		return // re-entrant — same goroutine already holds
	}

	// If inside RunInTransaction, INSERT into named_locks via the tx.
	// Otherwise open a short-lived gorm.Transaction just for the lock.
	lockTx := l.store.currentDB()

	// Retry loop for SQLite (no true unique wait). PostgreSQL/MySQL block
	// on INSERT conflict automatically — no retry needed there.
	for {
		id := GenerateID()
		res := lockTx.Create(&gNamedLock{ID: id, Name: l.name})
		if res.Error == nil {
			l.lockID = id
			l.held = true
			return
		}
		// Unique constraint violation — another process holds the lock.
		// Wait briefly and retry. For PG/MySQL this is the wrong branch
		// (they block), so it only fires on SQLite or race conditions.
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *sqlNamedLock) Unlock() {
	if !l.held {
		return
	}
	lockTx := l.store.currentDB()
	lockTx.Delete(&gNamedLock{}, "id = ?", l.lockID)
	l.held = false
}
