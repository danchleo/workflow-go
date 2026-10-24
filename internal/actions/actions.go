// Package actions defines the action interface and standard built-in actions.
// Ported from mistral-master/mistral/actions/std_actions.py
package actions

import (
	"fmt"
	"time"

	"github.com/mistral/workflow/internal/db"
)

// Action is the interface that all Mistral actions must implement.
// Ported from mistral_lib.actions.Action
type Action interface {
	// Run executes the action.
	Run(input map[string]interface{}) (*db.Result, error)
	// Name returns the action name.
	Name() string
	// IsSync reports whether the action is synchronous (blocking).
	IsSync() bool
}

// ActionDescriptor describes an action that can be instantiated.
type ActionDescriptor struct {
	Name        string
	Description string
	ActionClass string
	Attributes  map[string]interface{}
	IsSystem    bool
	Namespace   string

	// Action factory — creates a new Action instance with resolved attributes.
	Factory func() Action
}

// --- Standard built-in actions ---

// EchoAction just returns its configured output. Good for testing.
type EchoAction struct {
	Output interface{}
	Delay  float64
}

func NewEchoAction() Action {
	return &EchoAction{Output: "Hello, Mistral!"}
}

func (a *EchoAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.Delay > 0 {
		time.Sleep(time.Duration(a.Delay * float64(time.Second)))
	}
	return &db.Result{Data: a.Output}, nil
}

func (a *EchoAction) Name() string { return "std.echo" }
func (a *EchoAction) IsSync() bool { return true }

// NoopAction does nothing and returns nil.
type NoopAction struct{}

func NewNoopAction() Action { return &NoopAction{} }

func (a *NoopAction) Run(input map[string]interface{}) (*db.Result, error) {
	return &db.Result{}, nil
}
func (a *NoopAction) Name() string { return "std.noop" }
func (a *NoopAction) IsSync() bool { return true }

// AsyncNoopAction is an asynchronous noop (just returns immediately with success).
type AsyncNoopAction struct {
	NoopAction
}

func (a *AsyncNoopAction) Name() string { return "std.async_noop" }
func (a *AsyncNoopAction) IsSync() bool { return false }

// NewAsyncNoopAction creates an async noop action.
func NewAsyncNoopAction() Action { return &AsyncNoopAction{} }

// FailAction always fails — useful for testing error handling.
type FailAction struct {
	ErrorData interface{}
}

func NewFailAction() Action {
	return &FailAction{ErrorData: nil}
}

func (a *FailAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.ErrorData != nil {
		return &db.Result{Error: a.ErrorData}, nil
	}
	return &db.Result{Error: "Fail action expected error"}, fmt.Errorf("fail action expected error")
}
func (a *FailAction) Name() string { return "std.fail" }
func (a *FailAction) IsSync() bool { return true }

// SleepAction waits for the given number of seconds.
type SleepAction struct {
	Seconds float64
}

func NewSleepAction() Action {
	return &SleepAction{}
}

func (a *SleepAction) Run(input map[string]interface{}) (*db.Result, error) {
	seconds := a.Seconds
	if seconds <= 0 {
		if s, ok := input["seconds"].(float64); ok {
			seconds = s
		}
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
	return &db.Result{Data: fmt.Sprintf("slept for %v seconds", seconds)}, nil
}
func (a *SleepAction) Name() string { return "std.sleep" }
func (a *SleepAction) IsSync() bool { return true }

// WaitSSHAction simulates SSH wait. In the Python version this polls until a pattern appears.
// For Go we keep it simple: sleep then return.
type WaitSSHAction struct {
	Host    string
	Port    int
	Timeout float64
}

func NewWaitSSHAction() Action {
	return &WaitSSHAction{Port: 22, Timeout: 300}
}

func (a *WaitSSHAction) Run(input map[string]interface{}) (*db.Result, error) {
	// Simplified: just sleep for a configurable amount.
	time.Sleep(time.Duration(a.Timeout * float64(time.Second)))
	return &db.Result{Data: "SSH wait complete"}, nil
}
func (a *WaitSSHAction) Name() string { return "std.wait_ssh" }
func (a *WaitSSHAction) IsSync() bool { return false }

// ActionRegistry stores registered action descriptors.
type ActionRegistry struct {
	mu      syncLock
	actions map[string]*ActionDescriptor
}

// NewActionRegistry creates a new registry with built-in actions pre-registered.
func NewActionRegistry() *ActionRegistry {
	r := &ActionRegistry{
		actions: make(map[string]*ActionDescriptor),
	}
	r.RegisterBuiltinActions()
	return r
}

// RegisterBuiltinActions registers all standard built-in actions.
func (r *ActionRegistry) RegisterBuiltinActions() {
	r.Register(&ActionDescriptor{
		Name:        "std.echo",
		Description: "Returns configured output (testing helper)",
		IsSystem:    true,
		Factory: func() Action {
			return NewEchoAction()
		},
	})

	r.Register(&ActionDescriptor{
		Name:        "std.noop",
		Description: "Does nothing and returns nil",
		IsSystem:    true,
		Factory: func() Action {
			return NewNoopAction()
		},
	})

	r.Register(&ActionDescriptor{
		Name:        "std.async_noop",
		Description: "Async noop for testing",
		IsSystem:    true,
		Factory: func() Action {
			return NewAsyncNoopAction()
		},
	})

	r.Register(&ActionDescriptor{
		Name:        "std.fail",
		Description: "Always fails",
		IsSystem:    true,
		Factory: func() Action {
			return NewFailAction()
		},
	})

	r.Register(&ActionDescriptor{
		Name:        "std.sleep",
		Description: "Sleeps for N seconds",
		IsSystem:    true,
		Factory: func() Action {
			return NewSleepAction()
		},
	})

	r.Register(&ActionDescriptor{
		Name:        "std.wait_ssh",
		Description: "Simulates SSH readiness wait",
		IsSystem:    true,
		Factory: func() Action {
			return NewWaitSSHAction()
		},
	})
}

// Register registers an action descriptor.
func (r *ActionRegistry) Register(d *ActionDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[d.Name] = d
}

// Get looks up an action descriptor by name.
func (r *ActionRegistry) Get(name string) (*ActionDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.actions[name]
	return d, ok
}

// Find looks up an action descriptor by name, trying namespace variants.
func (r *ActionRegistry) Find(name, namespace string) (*ActionDescriptor, bool) {
	// Try full name first.
	if d, ok := r.Get(name); ok {
		return d, ok
	}
	// Try with namespace prefix.
	if namespace != "" {
		if d, ok := r.Get(namespace + "." + name); ok {
			return d, ok
		}
	}
	return nil, false
}

// List returns all registered action descriptors.
func (r *ActionRegistry) List() []*ActionDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*ActionDescriptor, 0, len(r.actions))
	for _, d := range r.actions {
		result = append(result, d)
	}
	return result
}

// syncLock is a tiny simple read/write lock wrapper.
type syncLock struct {
	ch chan struct{}
}

func (s *syncLock) Lock() {
	if s.ch == nil {
		s.ch = make(chan struct{}, 1)
	}
	s.ch <- struct{}{}
}
func (s *syncLock) Unlock()  { <-s.ch }
func (s *syncLock) RLock()   { s.Lock() }
func (s *syncLock) RUnlock() { s.Unlock() }
