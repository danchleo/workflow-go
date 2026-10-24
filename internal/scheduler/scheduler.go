// Package scheduler provides cron-based scheduling of workflow triggers.
// Ported from mistral-master/mistral/scheduler/default_scheduler.py
package scheduler

import (
	"context"
	"fmt"
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
	store    db.Store
	engine   WorkflowStarter
	interval time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// WorkflowStarter abstracts the engine's StartWorkflow method.
type WorkflowStarter interface {
	StartWorkflow(ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error)
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
// Mirrors Python default_scheduler._process_store_jobs but simplified: no persistent
// job queue, just cron triggers with next_execution_time directly in the trigger.
func (s *DefaultScheduler) RunOnce() {
	triggers, err := s.store.CronTriggers().List()
	if err != nil {
		log.Printf("scheduler: list cron triggers: %v", err)
		return
	}

	now := time.Now()
	var due []*db.CronTrigger

	for _, ct := range triggers {
		// remaining == 0 means unlimited; >0 means countdown.
		if ct.RemainingExecutions < 0 {
			continue // disabled
		}
		if now.Before(ct.NextExecutionTime) {
			continue // not due yet
		}
		due = append(due, ct)
	}

	for _, ct := range due {
		// Resolve workflow definition.
		var wfDefID, wfNamespace string
		if ct.WorkflowID != "" {
			wfDefID = ct.WorkflowID
		} else if ct.WorkflowName != "" {
			wfDef, err := s.store.WorkflowDefinitions().GetByName(ct.WorkflowName, "", "")
			if err != nil {
				log.Printf("scheduler: cron trigger %s: wf %s not found: %v", ct.Name, ct.WorkflowName, err)
				continue
			}
			wfDefID = wfDef.ID
			wfNamespace = wfDef.Namespace
		} else {
			log.Printf("scheduler: cron trigger %s: no workflow id or name", ct.Name)
			continue
		}

		input := ct.WorkflowInput
		if input == nil {
			input = make(map[string]interface{})
		}
		params := ct.WorkflowParams
		if params == nil {
			params = make(map[string]interface{})
		}

		wfEx, err := s.engine.StartWorkflow(context.Background(), wfDefID, wfNamespace, "", input,
			fmt.Sprintf("cron trigger %s", ct.Name), params)
		if err != nil {
			log.Printf("scheduler: cron trigger %s: StartWorkflow failed: %v", ct.Name, err)
			continue
		}
		log.Printf("scheduler: cron trigger %s fired → wfEx=%s", ct.Name, wfEx.ID)

		// Advance the trigger.
		interval := time.Duration(ct.IntervalSeconds) * time.Second
		if interval <= 0 {
			interval = time.Minute // fallback
		}
		ct.NextExecutionTime = now.Add(interval)
		if ct.RemainingExecutions > 0 {
			ct.RemainingExecutions--
			if ct.RemainingExecutions == 0 {
				// Countdown exhausted → delete.
				if err := s.store.CronTriggers().Delete(ct.ID); err != nil {
					log.Printf("scheduler: delete cron trigger %s: %v", ct.Name, err)
				}
				continue
			}
		}
		if err := s.store.CronTriggers().Update(ct); err != nil {
			log.Printf("scheduler: update cron trigger %s: %v", ct.Name, err)
		}
	}
}
