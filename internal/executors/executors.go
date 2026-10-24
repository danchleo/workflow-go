// Package executors defines the action execution interface and default local executor.
// Ported from mistral-master/mistral/executors/
package executors

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/actions"
	"github.com/mistral/workflow/internal/db"
)

// Executor is the interface that action executors must implement.
// Ported from mistral-master/mistral/executors/base.py
type Executor interface {
	// RunAction runs an action and returns the result.
	// async_: if true, run without blocking. For sync actions this still returns immediately.
	RunAction(action actions.Action, actionExID string, safeRerun bool, execCtx map[string]interface{}, target string, async bool, timeout time.Duration) (*db.Result, error)
}

// LocalExecutor runs actions in-process (synchronous).
// Ported from mistral-master/mistral/executors/default_executor.py
type LocalExecutor struct{}

// NewLocalExecutor creates a LocalExecutor.
func NewLocalExecutor() *LocalExecutor { return &LocalExecutor{} }

func (e *LocalExecutor) RunAction(action actions.Action, actionExID string, safeRerun bool, execCtx map[string]interface{}, target string, async bool, timeout time.Duration) (*db.Result, error) {
	input := make(map[string]interface{})
	if actionExID != "" {
		// Input could come from the action execution — we keep it simple here.
	}

	if timeout > 0 {
		done := make(chan *db.Result, 1)
		errChan := make(chan error, 1)

		go func() {
			result, err := action.Run(input)
			if err != nil {
				errChan <- err
				return
			}
			done <- result
		}()

		select {
		case <-time.After(timeout):
			return &db.Result{Error: fmt.Sprintf("action timed out after %v", timeout)}, nil
		case result := <-done:
			return result, nil
		case err := <-errChan:
			return nil, err
		}
	}

	return action.Run(input)
}

// RemoteExecutor is a stub for RPC-based remote execution.
// Ported from mistral-master/mistral/executors/remote_executor.py
type RemoteExecutor struct {
	endpoint string
	mu       sync.Mutex
}

// NewRemoteExecutor creates a RemoteExecutor.
func NewRemoteExecutor(endpoint string) *RemoteExecutor {
	return &RemoteExecutor{endpoint: endpoint}
}

func (e *RemoteExecutor) RunAction(action actions.Action, actionExID string, safeRerun bool, execCtx map[string]interface{}, target string, async bool, timeout time.Duration) (*db.Result, error) {
	log.Printf("remote executor placeholder: action=%s, target=%s", action.Name(), target)
	return &db.Result{Data: "placeholder — remote execution not available"}, nil
}

// Registry stores executor implementations by type name.
var (
	defaultExecutor Executor = NewLocalExecutor()
	executorsMu     sync.RWMutex
)

// SetDefaultExecutor sets the default executor.
func SetDefaultExecutor(e Executor) {
	executorsMu.Lock()
	defaultExecutor = e
	executorsMu.Unlock()
}

// GetDefaultExecutor returns the default executor.
func GetDefaultExecutor() Executor {
	executorsMu.RLock()
	defer executorsMu.RUnlock()
	return defaultExecutor
}
