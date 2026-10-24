package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/db"
)

// WorkflowStarter abstracts the engine's StartWorkflow method.
// Used only by CronRunner; the core Scheduler has no domain knowledge.
type WorkflowStarter interface {
	StartWorkflow(ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error)
}

// CronRunner handles DB-backed cron triggers — it is a separate struct so
// the core scheduler stays pure. Mirrors Python mistral.scheduler's cron
// processing loop but simplified (interval-based, not cron-expression).
type CronRunner struct {
	store    db.Store
	engine   WorkflowStarter
	interval time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewCronRunner creates a CronRunner.
func NewCronRunner(store db.Store, engine WorkflowStarter, interval time.Duration) *CronRunner {
	return &CronRunner{
		store:    store,
		engine:   engine,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the cron check loop in a goroutine.
func (c *CronRunner) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.RunOnce()
			}
		}
	}()
}

// Stop halts the cron loop.
func (c *CronRunner) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// RunOnce checks for cron triggers whose next_execution_time is due and fires them.
func (c *CronRunner) RunOnce() {
	triggers, err := c.store.CronTriggers().List()
	if err != nil {
		log.Printf("cron-runner: list cron triggers: %v", err)
		return
	}

	now := time.Now()
	var due []*db.CronTrigger

	for _, ct := range triggers {
		if ct.RemainingExecutions < 0 {
			continue // disabled
		}
		if now.Before(ct.NextExecutionTime) {
			continue
		}
		due = append(due, ct)
	}

	for _, ct := range due {
		var wfDefID, wfNamespace string
		if ct.WorkflowID != "" {
			wfDefID = ct.WorkflowID
		} else if ct.WorkflowName != "" {
			wfDef, err := c.store.WorkflowDefinitions().GetByName(ct.WorkflowName, "", "")
			if err != nil {
				log.Printf("cron-runner: trigger %s: wf %s not found: %v", ct.Name, ct.WorkflowName, err)
				continue
			}
			wfDefID = wfDef.ID
			wfNamespace = wfDef.Namespace
		} else {
			log.Printf("cron-runner: trigger %s: no workflow id or name", ct.Name)
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

		wfEx, err := c.engine.StartWorkflow(context.Background(), wfDefID, wfNamespace, "", input,
			fmt.Sprintf("cron trigger %s", ct.Name), params)
		if err != nil {
			log.Printf("cron-runner: trigger %s: StartWorkflow failed: %v", ct.Name, err)
			continue
		}
		log.Printf("cron-runner: trigger %s fired → wfEx=%s", ct.Name, wfEx.ID)

		interval := time.Duration(ct.IntervalSeconds) * time.Second
		if interval <= 0 {
			interval = time.Minute
		}
		ct.NextExecutionTime = now.Add(interval)
		if ct.RemainingExecutions > 0 {
			ct.RemainingExecutions--
			if ct.RemainingExecutions == 0 {
				if err := c.store.CronTriggers().Delete(ct.ID); err != nil {
					log.Printf("cron-runner: delete trigger %s: %v", ct.Name, err)
				}
				continue
			}
		}
		if err := c.store.CronTriggers().Update(ct); err != nil {
			log.Printf("cron-runner: update trigger %s: %v", ct.Name, err)
		}
	}
}
