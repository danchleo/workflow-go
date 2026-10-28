// Package executors defines the action execution interface and executors.
// Ported from mistral-master/mistral/executors/
//
// Executor hierarchy mirrors Python:
//
//	Executor (interface)
//	  └── LocalExecutor   — in-process (Python default_executor.py)
//	  └── RemoteExecutor  — HTTP RPC to a remote worker (Python remote_executor.py)
//
// Python's action flow:
//
//	action_desc.instantiate(input_dict, wf_ctx) → action instance (carries input)
//	executor.run_action(action, ...) → action.run(action_ctx)
//
// Go's action flow (actions.Action is stateless — input passed to Run):
//
//	dispatcher loads action from registry, builds action + passes inputDict separately
//	executor.RunAction(action, inputDict, ...) → action.Run(inputDict)
package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/db"
)

// Executor is the interface that action executors must implement.
// Ported from mistral-master/mistral/executors/base.py
type Executor interface {
	// RunAction runs an action and returns the result.
	//
	// Go differs from Python in that inputDict is passed explicitly — Python
	// bakes input into the action instance via action_desc.instantiate(input, ctx).
	// Go actions are stateless so input travels as a separate parameter.
	RunAction(
		action actions.Action, inputDict map[string]interface{},
		actionExID string, safeRerun bool, execCtx map[string]interface{},
		target string, timeout time.Duration,
	) (*db.Result, error)
}

// LocalExecutor runs actions in-process (synchronous).
// Ported from mistral-master/mistral/executors/default_executor.py
type LocalExecutor struct{}

// NewLocalExecutor creates a LocalExecutor.
func NewLocalExecutor() *LocalExecutor { return &LocalExecutor{} }

// RunAction runs the action locally.
func (e *LocalExecutor) RunAction(
	action actions.Action, inputDict map[string]interface{},
	actionExID string, safeRerun bool, execCtx map[string]interface{},
	target string, timeout time.Duration,
) (*db.Result, error) {
	if inputDict == nil {
		inputDict = make(map[string]interface{})
	}

	if timeout > 0 {
		done := make(chan *db.Result, 1)
		errChan := make(chan error, 1)

		go func() {
			result, err := action.Run(inputDict)
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

	return action.Run(inputDict)
}

// RemoteExecutor calls an HTTP executor server to run the action on a remote worker.
// Ported from mistral-master/mistral/executors/remote_executor.py which uses
// oslo.messaging RPC. Go uses a simple HTTP POST + JSON to avoid pulling in
// a full message broker.
//
// Request JSON body:
//
//	{
//	  "action":       action.Name(),
//	  "input":        inputDict,
//	  "action_ex_id": actionExID,
//	  "safe_rerun":   safeRerun,
//	  "exec_ctx":     execCtx,
//	  "target":       target,
//	  "timeout_sec":  int(timeout.Seconds()),
//	}
//
// Response JSON body:
//
//	{"data": "...", "error": "..."}   (one or the other, never both)
type RemoteExecutor struct {
	endpoint   string // base URL, e.g. "http://executor.example.com:8952"
	httpClient *http.Client
	mu         sync.Mutex
}

// NewRemoteExecutor creates a RemoteExecutor pointing at the given HTTP endpoint.
func NewRemoteExecutor(endpoint string) *RemoteExecutor {
	return &RemoteExecutor{
		endpoint:   endpoint,
		httpClient: &http.Client{},
	}
}

// runActionRequest is the JSON contract between engine and remote executor.
type runActionRequest struct {
	Action     string                 `json:"action"`
	Input      map[string]interface{} `json:"input"`
	ActionExID string                 `json:"action_ex_id"`
	SafeRerun  bool                   `json:"safe_rerun"`
	ExecCtx    map[string]interface{} `json:"exec_ctx"`
	Target     string                 `json:"target,omitempty"`
	TimeoutSec int                    `json:"timeout_sec"`
}

// runActionResponse is what the remote executor server returns.
type runActionResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

// RunAction marshals the action request as JSON and POSTs it to the remote worker.
func (e *RemoteExecutor) RunAction(
	action actions.Action, inputDict map[string]interface{},
	actionExID string, safeRerun bool, execCtx map[string]interface{},
	target string, timeout time.Duration,
) (*db.Result, error) {
	reqBody := runActionRequest{
		Action:     action.Name(),
		Input:      inputDict,
		ActionExID: actionExID,
		SafeRerun:  safeRerun,
		ExecCtx:    execCtx,
		Target:     target,
	}
	if timeout > 0 {
		reqBody.TimeoutSec = int(timeout.Seconds())
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("remote executor marshal: %w", err)
	}

	url := e.endpoint + "/v1/actions/run"

	reqCtx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(reqCtx, timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("remote executor new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("remote executor HTTP call: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("remote executor read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &db.Result{
			Error: fmt.Sprintf("remote executor HTTP %d: %s", resp.StatusCode, string(respBody)),
		}, nil
	}

	var parsed runActionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return &db.Result{
			Error: fmt.Sprintf("remote executor unmarshal: %v, raw=%s", err, string(respBody)),
		}, nil
	}

	if parsed.Error != "" {
		return &db.Result{Error: parsed.Error}, nil
	}
	return &db.Result{Data: parsed.Data}, nil
}

// --- Registry + default executor ---

var (
	defaultExecutor Executor = NewLocalExecutor()
	executorsMu     sync.RWMutex
	executorTypes   = map[string]func() Executor{
		"local":  func() Executor { return NewLocalExecutor() },
		"remote": func() Executor { return NewRemoteExecutor("http://localhost:8952") },
	}
)

// RegisterExecutorType adds a named constructor to the registry.
// Mirrors Python's stevedore DriverManager('mistral.executors', exec_type).
func RegisterExecutorType(name string, factory func() Executor) {
	executorsMu.Lock()
	executorTypes[name] = factory
	executorsMu.Unlock()
}

// SetExecutorType picks a built-in executor by name ("local", "remote").
// If "remote", the default endpoint http://localhost:8952 is used — call
// SetRemoteEndpoint() beforehand to point at a real worker.
func SetExecutorType(name string) error {
	executorsMu.RLock()
	factory, ok := executorTypes[name]
	executorsMu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown executor type %q (known: local, remote)", name)
	}
	executorsMu.Lock()
	defaultExecutor = factory()
	executorsMu.Unlock()
	log.Printf("executor: set type=%s", name)
	return nil
}

// SetRemoteEndpoint updates the RemoteExecutor base URL if that's the current
// default executor. Has no effect if the current executor isn't a RemoteExecutor.
func SetRemoteEndpoint(endpoint string) {
	executorsMu.Lock()
	defer executorsMu.Unlock()
	if re, ok := defaultExecutor.(*RemoteExecutor); ok {
		re.endpoint = endpoint
		log.Printf("executor: remote endpoint → %s", endpoint)
	}
}

// SetDefaultExecutor overrides with a custom Executor instance.
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

// GetExecutorType returns the name of the current default executor ("local", "remote",
// or "custom" if an instance was registered via SetDefaultExecutor).
func GetExecutorType() string {
	executorsMu.RLock()
	defer executorsMu.RUnlock()
	switch defaultExecutor.(type) {
	case *LocalExecutor:
		return "local"
	case *RemoteExecutor:
		return "remote"
	default:
		return "custom"
	}
}
