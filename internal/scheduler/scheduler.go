// Package scheduler provides cron-based scheduling of workflow triggers.
// Ported from mistral-master/mistral/scheduler/default_scheduler.py
package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/db"
)

// Scheduler manages time-based workflow execution triggers.
type Scheduler interface {
	// Start begins scheduling loops.
	Start()
	// Stop halts scheduling loops.
	Stop()
	// RunOnce does one pass of checking for scheduled jobs.
	RunOnce()
}

// DefaultScheduler is a straightforward ticker-based scheduler.
type DefaultScheduler struct {
	store   db.Store
	engine  WorkflowStarter
	interval time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// WorkflowStarter abstracts the engine's StartWorkflow method.
type WorkflowStarter interface {
	StartWorkflow(wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error)
}

// NewDefaultScheduler creates a DefaultScheduler.
func NewDefaultScheduler(store db.Store, engine WorkflowStarter, interval time.Duration) *DefaultScheduler {
	return &DefaultScheduler{
		store:    store,
		engine:   engine,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the scheduler loop in a goroutine.
func (s *DefaultScheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.RunOnce()
			}
		}
	}()
}

// Stop halts the scheduler.
func (s *DefaultScheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// RunOnce checks for cron triggers whose next_execution_time is due and fires them.
func (s *DefaultScheduler) RunOnce() {
	// Cron triggers are stored in the database. In a real implementation we'd query for
	// triggers where next_execution_time <= now(), fire workflows, and compute next_execution_time.
	log.Println("scheduler.RunOnce() — cron trigger evaluation (stub)")
}
