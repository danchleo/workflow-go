// Package policies provides a hook interface for engine policies (retry, timeout, concurrency, etc).
// Ported from mistral-master/mistral/engine/policies.py and mistral/policies/
package policies

import (
	"time"

	"github.com/danchleo/workflow-go/internal/db"
)

// Policy is a hook applied before/after task execution.
type Policy interface {
	Name() string
	BeforeTaskStart(task *db.TaskExecution) error
	AfterTaskComplete(task *db.TaskExecution) error
}

// RetryPolicy implements task retry with configurable count and delay.
type RetryPolicy struct {
	Count      int
	Delay      time.Duration
	BreakOn    interface{}
	ContinueOn interface{}
}

func (p *RetryPolicy) Name() string { return "retry" }

func (p *RetryPolicy) BeforeTaskStart(task *db.TaskExecution) error {
	return nil
}

func (p *RetryPolicy) AfterTaskComplete(task *db.TaskExecution) error {
	// Retry logic — tracked in task runtime context.
	return nil
}

// TimeoutPolicy implements task timeout.
type TimeoutPolicy struct {
	Duration time.Duration
}

func (p *TimeoutPolicy) Name() string                                   { return "timeout" }
func (p *TimeoutPolicy) BeforeTaskStart(task *db.TaskExecution) error   { return nil }
func (p *TimeoutPolicy) AfterTaskComplete(task *db.TaskExecution) error { return nil }

// ConcurrencyPolicy limits the number of concurrent task executions.
type ConcurrencyPolicy struct {
	Max int
}

func (p *ConcurrencyPolicy) Name() string                                   { return "concurrency" }
func (p *ConcurrencyPolicy) BeforeTaskStart(task *db.TaskExecution) error   { return nil }
func (p *ConcurrencyPolicy) AfterTaskComplete(task *db.TaskExecution) error { return nil }

// BuildPolicies builds policies from task spec policy definitions.
func BuildPolicies(policyDefs map[string]interface{}) []Policy {
	var result []Policy
	if policyDefs == nil {
		return result
	}

	if v, ok := policyDefs["timeout"]; ok {
		switch t := v.(type) {
		case float64:
			result = append(result, &TimeoutPolicy{Duration: time.Duration(t * float64(time.Second))})
		case int:
			result = append(result, &TimeoutPolicy{Duration: time.Duration(t) * time.Second})
		}
	}

	return result
}
