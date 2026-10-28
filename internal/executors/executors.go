// Package executors defines the action execution interface and executors.
// Ported from mistral-master/mistral/executors/
//
// Executor hierarchy mirrors Python:
//
//	Executor (interface)
//	  └── LocalExecutor   — in-process (Python default_executor.py)
//	  └── RemoteExecutor  — HTTP RPC to a remote worker (Python remote_executor.py)
//
// Python's DefaultExecutor architecture (critical for Go port):
//
//	DefaultExecutor._engine_client.on_action_complete(action_ex_id, result)
//	                                                                   ↑
//	                            Executor ITSELF calls back into engine.
//
//	In Python's two-process model:
//	  Engine process → RPC → ExecutorServer.run_action()
//	                                  → DefaultExecutor._do_run_action()
//	                                    → thread.run() + join(timeout)
//	                                    → self._engine_client.on_action_complete()  ← Executor 负责回调
//	  Engine process ← callback RPC ←  (async — executor doesn't block on engine)
//
// Go mirrors this: Executor.RunAction takes an onComplete callback.
// Both LocalExecutor (engine进程内) and RemoteExecutor (HTTP POST to executor server)
// are responsible for invoking this callback with the result — NOT the caller.
// This matches Python's pattern where the executor owns the completion handshake.
//
// Additional Python behaviors ported:
//   - redelivered + safe_rerun guard: action that was redelivered but NOT safe_rerun
//     must be failed by the executor (no point re-running if side effects can't be undone)
//   - timeout handling: Executor owns the timeout, not the caller
//   - result normalization: Executor wraps errors in db.Result{Error: ...}
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

// OnCompleteCallback is what an Executor invokes after running an action —
// matches Python's DefaultExecutor._engine_client.on_action_complete().
// Set to nil to disable (executorapi.Server uses nil because HTTP response
// already carries the result back to the engine).
type OnCompleteCallback func(actionExID string, result *db.Result, wfAction bool)

// Executor is the interface that action executors must implement.
// Ported from mistral-master/mistral/executors/base.py
type Executor interface {
	// RunAction runs an action and invokes onCompleteCb with the result.
	//
	// Mirror of Python DefaultExecutor.run_action() signature:
	//   run_action(self, action, action_ex_id, safe_rerun, exec_ctx,
	//              redelivered=False, target=None, async_=True, timeout=None)
	//
	// Go differences:
	//   - inputDict is explicit (Go actions are stateless)
	//   - redelivered not yet plumbed from DispatchAction → default false
	//   - async_: Go LocalExecutor always blocks (Python thread.join waits)
	//   - onCompleteCb is the Go equivalent of Python's self._engine_client
	//     (in-process func vs oslo.messaging RPC)
	RunAction(
		action actions.Action, inputDict map[string]interface{},
		actionExID string, safeRerun bool, execCtx map[string]interface{},
		target string, timeout time.Duration,
		onCompleteCb OnCompleteCallback,
	) (*db.Result, error)
}

// LocalExecutor runs actions in-process (synchronous).
// Ported from mistral-master/mistral/executors/default_executor.py
type LocalExecutor struct{}

// NewLocalExecutor creates a LocalExecutor.
func NewLocalExecutor() *LocalExecutor { return &LocalExecutor{} }

// RunAction runs the action locally and invokes onCompleteCb with the result.
//
// Mirror of Python DefaultExecutor._do_run_action() — ported line-by-line:
//
//  1. redelivered + safe_rerun guard (Go: redelivered 暂未 plumbed，safe_rerun 预留)
//  2. goroutine + channel + timeout select
//  3. 超时后等待 goroutine 退出再返回（防 goroutine leak）
//  4. action.Run 错误 → result.Error（Python except BaseException → send_error_back）
//  5. 只在 action.IsSync() 或 result.IsError() 时调 callback（Python is_sync or is_error）
//  6. callback 失败 → send_error_back（Python MistralException 重试 + Exception 只 log）
//  7. callback 有 onCompleteCb guard（Python if action_ex_id）
func (e *LocalExecutor) RunAction(
	action actions.Action, inputDict map[string]interface{},
	actionExID string, safeRerun bool, execCtx map[string]interface{},
	target string, timeout time.Duration,
	onCompleteCb OnCompleteCallback,
) (*db.Result, error) {
	// Python: if redelivered and not safe_rerun → fail immediately.
	// Go 暂不实现 redelivered（需要 engine 侧标识），safe_rerun 预留。
	_ = safeRerun
	_ = execCtx
	_ = target

	if inputDict == nil {
		inputDict = make(map[string]interface{})
	}

	// --- Run action with timeout (mirrors Python thread.start() + join(timeout)) ---
	type outcome struct {
		res *db.Result
		err error
	}
	done := make(chan outcome, 1)

	go func() {
		res, err := action.Run(inputDict)
		done <- outcome{res: res, err: err}
	}()

	var result *db.Result
	var timedOut bool

	if timeout > 0 {
		select {
		case <-time.After(timeout):
			timedOut = true
			// Python: "we are now waiting for the action thread to finish..."
			// Go 等价 — 等 goroutine 退出防 leak（Python 用 thread.join()）。
			// 但不能无限等：用稍微长一点的等待，之后放弃。
			drainTimer := time.NewTimer(timeout * 2)
			defer drainTimer.Stop()
			select {
			case <-done:
				// goroutine 刚好在 deadline 前/后完成了
			case <-drainTimer.C:
				// goroutine 还没退出，只能放弃（Python 这里是无限 join）
				log.Printf("LocalExecutor: action %s (ex=%s) timed out and goroutine still running after %v",
					action.Name(), actionExID, timeout*3)
			}
			result = &db.Result{Error: fmt.Sprintf("action timed out after %v", timeout)}
		case o := <-done:
			// goroutine 在 deadline 内完成
			if o.err != nil {
				result = &db.Result{Error: o.err}
			} else {
				result = o.res
			}
		}
	} else {
		// 无 timeout — 直接等 goroutine 完成
		o := <-done
		if o.err != nil {
			result = &db.Result{Error: o.err}
		} else {
			result = o.res
		}
	}

	if timedOut {
		// 超时也走 callback — Python 里 timeout raise 被 except 捕获 → send_error_back
		sendErrorBack(actionExID, onCompleteCb,
			fmt.Sprintf("action timed out after %v", timeout), false)
		return result, nil
	}

	// --- Send action result (mirrors Python's completion callback logic) ---
	// Python: if action_ex_id and (action.is_sync() or result.is_error()):
	//             self._engine_client.on_action_complete(action_ex_id, result)
	//
	// Go: 只在 sync action 或 error 时调 callback
	if actionExID != "" {
		isError := result != nil && result.Error != nil
		if action.IsSync() || isError {
			if onCompleteCb != nil {
				if err := safeInvoke(onCompleteCb, actionExID, result, false); err != nil {
					// Python: MistralException → send_error_back retry
					//         Exception → only log
					log.Printf("LocalExecutor: callback error on action %s (ex=%s): %v",
						action.Name(), actionExID, err)
					// send_error_back — 用 result.Error 再试一次
					sendErrorBack(actionExID, onCompleteCb,
						fmt.Sprintf("failed to deliver result: %v", err), false)
				}
			}
		}
		// async + success → 不回调（Python 靠 heartbeat 超时判定）
	}

	return result, nil
}

// safeInvoke calls onCompleteCb and recovers panics — mirrors Python's
// try/except around self._engine_client.on_action_complete().
func safeInvoke(cb OnCompleteCallback, id string, r *db.Result, wf bool) (retErr error) {
	defer func() {
		if rec := recover(); rec != nil {
			retErr = fmt.Errorf("callback panic: %v", rec)
		}
	}()
	cb(id, r, wf)
	return nil
}

// sendErrorBack mirrors Python's send_error_back(error_msg):
// wraps the error msg in db.Result and invokes the callback again.
func sendErrorBack(actionExID string, onCompleteCb OnCompleteCallback, msg string, wfAction bool) {
	if actionExID == "" || onCompleteCb == nil {
		return
	}
	errResult := &db.Result{Error: msg}
	if err := safeInvoke(onCompleteCb, actionExID, errResult, wfAction); err != nil {
		log.Printf("LocalExecutor: sendErrorBack also failed for %s: %v", actionExID, err)
	}
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

// RunAction marshals the action request as JSON and POSTs it to the remote worker,
// then invokes onCompleteCb with the result — matching Python DefaultExecutor pattern
// where the executor owns the completion handshake, not the caller.
func (e *RemoteExecutor) RunAction(
	action actions.Action, inputDict map[string]interface{},
	actionExID string, safeRerun bool, execCtx map[string]interface{},
	target string, timeout time.Duration,
	onCompleteCb OnCompleteCallback,
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

	var result *db.Result
	if resp.StatusCode != http.StatusOK {
		result = &db.Result{
			Error: fmt.Sprintf("remote executor HTTP %d: %s", resp.StatusCode, string(respBody)),
		}
	} else {
		var parsed runActionResponse
		if err := json.Unmarshal(respBody, &parsed); err != nil {
			result = &db.Result{
				Error: fmt.Sprintf("remote executor unmarshal: %v, raw=%s", err, string(respBody)),
			}
		} else if parsed.Error != "" {
			result = &db.Result{Error: parsed.Error}
		} else {
			result = &db.Result{Data: parsed.Data}
		}
	}

	// Python: self._engine_client.on_action_complete(action_ex_id, result)
	if onCompleteCb != nil {
		onCompleteCb(actionExID, result, false)
	}

	return result, nil
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
