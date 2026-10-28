// Package executors defines the action execution interface and executors.
// Ported from mistral-master/mistral/executors/
//
// Python 架构（关键设计，必须对齐）：
//
//	class DefaultExecutor(base.Executor):
//	    def __init__(self):
//	        self._engine_client = rpc.get_engine_client()   # ← 构造时注入 engine client
//
//	    def run_action(self, action, action_ex_id, safe_rerun, exec_ctx, ...):
//	        ...
//	        if action_ex_id and (action.is_sync() or result.is_error()):
//	            self._engine_client.on_action_complete(     # ← 内部自己回调
//	                action_ex_id, result, async_=True
//	            )
//	        return result
//
// Python 关键点：
//   1. Executor **构造时**就持有 EngineClient（rpc.get_engine_client() 单例）
//   2. Executor.RunAction **没有 callback 参数** — 签名是纯返回 (*Result, error)
//   3. RunAction **内部**通过 self._engine_client.on_action_complete() 回调
//      （本地进程里是 LocalEngineClient 直接内调；远程进程里是 RPC EngineClient）
//   4. 回调条件：action_ex_id 非空 AND (action.is_sync() OR result.is_error())
//   5. 回调异常处理：MistralException → send_error_back retry；其他 Exception → 只 log
//
// Go 完全照搬这个设计：
//   - DefaultExecutor 持有 rpc.EngineClient（构造时注入）
//   - RunAction 签名：RunAction(action, input, exID, safeRerun, execCtx, target, timeout) (*db.Result, error)
//   - RunAction 内部调 e.engineClient.OnActionComplete(ctx, exID, result, false)
//   - 回调条件、异常处理完全镜像 Python
//
// 两种进程部署：
//
//   场景 A — 单进程 (workflowd --server all):
//     DefaultExecutor 构造时传 rpc.NewLocalEngineClient(engine)
//     回调 = 直接内调 engine.OnActionComplete()
//
//   场景 B — 多进程 (engine + executor 分开):
//     engine 进程:   DefaultExecutor(rpc.NewLocalEngineClient(engine))
//                    → 回调内调 + 可用 RemoteExecutor POST 到 executor server
//     executor 进程: DefaultExecutor(rpc.NewHttpEngineClient("http://engine:8951"))
//                    → HTTP 回调 engine /rpc/on_action_complete
//     executorapi.Server: 用 DefaultExecutor(nil) — nil client 意味着 HTTP response 已经返回
//
// 注意：Go 有 RemoteExecutor（engine 进程 POST 到 executor server），Python 没有这个。
// Python 用的是 oslo.messaging redelivery 机制。Go RemoteExecutor 是 HTTP transport 层，
// 它 POST 后等 HTTP response 返回 result，**不负责回调**。回调是 DefaultExecutor 内部的事。
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
	"github.com/danchleo/workflow-go/internal/rpc"
)

// Executor is the interface that action executors must implement.
// Ported from mistral-master/mistral/executors/base.py
//
// Go 签名与 Python 的差异：
//   + context.Context ctx — Go 标准
//   + inputDict map[string]interface{} — Python 把 input 烤进 action 实例，Go actions 是 stateless
//   - onCompleteCb callback — Python 也没有！Executor 内部自己持有 engine client
type Executor interface {
	// RunAction runs an action and returns the result.
	// If this executor has an engineClient (DefaultExecutor), it also
	// calls OnActionComplete internally — mirroring Python's
	// self._engine_client.on_action_complete(action_ex_id, result).
	RunAction(
		ctx context.Context,
		action actions.Action, inputDict map[string]interface{},
		actionExID string, safeRerun bool, execCtx map[string]interface{},
		target string, timeout time.Duration,
	) (*db.Result, error)
}

// DefaultExecutor is the built-in executor that runs actions in-process.
// Ported from mistral-master/mistral/executors/default_executor.py
//
// Critical: DefaultExecutor HOLDS an rpc.EngineClient, injected at construction.
// This matches Python's self._engine_client = rpc.get_engine_client().
// RunAction is responsible for BOTH running the action AND calling back the
// engine via engineClient.OnActionComplete().
type DefaultExecutor struct {
	engineClient rpc.EngineClient
}

// NewDefaultExecutor creates a DefaultExecutor.
// engineClient may be nil (e.g. when used by executorapi.Server which returns
// result via HTTP response instead of callback).
func NewDefaultExecutor(engineClient rpc.EngineClient) *DefaultExecutor {
	return &DefaultExecutor{engineClient: engineClient}
}

// RunAction runs the action and calls back the engine via engineClient.
// Ported line-by-line from Python DefaultExecutor._do_run_action().
//
// Python structure:
//   def run_action(...):
//       action_heartbeat_sender.add_action(action_ex_id)
//       try:
//           return self._do_run_action(...)
//       finally:
//           action_heartbeat_sender.remove_action(action_ex_id)
//
//   def _do_run_action(...):
//       def send_error_back(error_msg): ...    ← closure
//
//       if redelivered and not safe_rerun:     ← guard
//           return send_error_back(msg)
//
//       try:
//           thread.start()                     ← goroutine start
//           thread.join(timeout=timeout)       ← wait with timeout
//           if thread.is_alive():
//               thread.join()                  ← wait for goroutine drain
//               raise Exception("Timeout...")
//           if not isinstance(result, Result): ← normalize
//               result = Result(data=result)
//       except BaseException as e:
//           return send_error_back(msg)        ← all errors go through send_error_back
//
//       try:
//           if action_ex_id and (action.is_sync() or result.is_error()):
//               self._engine_client.on_action_complete(...)  ← CALLBACK
//       except MistralException as e:
//           return send_error_back(msg)        ← retry as error
//       except Exception as e:
//           LOG.exception(msg)                ← just log
//
//       return result
func (e *DefaultExecutor) RunAction(
	ctx context.Context,
	action actions.Action, inputDict map[string]interface{},
	actionExID string, safeRerun bool, execCtx map[string]interface{},
	target string, timeout time.Duration,
) (*db.Result, error) {
	_ = safeRerun
	_ = execCtx
	_ = target

	if inputDict == nil {
		inputDict = make(map[string]interface{})
	}

	// --- send_error_back closure (mirrors Python) ---
	// Python: def send_error_back(error_msg):
	//            error_result = mistral_lib.Result(error=error_msg)
	//            if action_ex_id:
	//                self._engine_client.on_action_complete(action_ex_id, error_result)
	//                return None
	//            return error_result
	sendErrorBack := func(msg string) *db.Result {
		errResult := &db.Result{Error: msg}
		if actionExID != "" {
			e.engineComplete(ctx, actionExID, errResult)
			return nil
		}
		return errResult
	}

	// --- redelivered + safe_rerun guard ---
	// Python: if redelivered and not safe_rerun → fail immediately
	// Go: redelivered not yet plumbed (needs engine to mark it) — skip for now

	// --- Run action with timeout (mirrors thread.start() + join(timeout)) ---
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
			// Python: if thread.is_alive(): thread.join() ← wait for goroutine drain
			drainTimer := time.NewTimer(timeout * 2)
			defer drainTimer.Stop()
			select {
			case <-done:
				// goroutine drained in time
			case <-drainTimer.C:
				log.Printf("DefaultExecutor: action %s (ex=%s) timed out, goroutine still running",
					action.Name(), actionExID)
			}
			result = &db.Result{Error: fmt.Sprintf("action timed out after %v", timeout)}
		case o := <-done:
			if o.err != nil {
				// Python except BaseException → send_error_back
				return sendErrorBack(
					fmt.Sprintf("action %s raised: %v", action.Name(), o.err),
				), nil
			}
			result = o.res
		}
	} else {
		o := <-done
		if o.err != nil {
			return sendErrorBack(
				fmt.Sprintf("action %s raised: %v", action.Name(), o.err),
			), nil
		}
		result = o.res
	}

	// --- Timeout path: Python raises → except → send_error_back ---
	if timedOut {
		return sendErrorBack(
			fmt.Sprintf("action %s timed out after %v", action.Name(), timeout),
		), nil
	}

	// --- Normal completion: send action result (mirrors Python's completion block) ---
	// Python: if action_ex_id and (action.is_sync() or result.is_error()):
	//             self._engine_client.on_action_complete(action_ex_id, result)
	if actionExID != "" {
		isError := result != nil && result.Error != nil
		if action.IsSync() || isError {
			e.engineComplete(ctx, actionExID, result)
		}
		// async + success → no callback (Python relies on heartbeat timeout)
	}

	return result, nil
}

// engineComplete calls engineClient.OnActionComplete with panic recovery + MistralException retry.
// Mirrors Python's try/except around self._engine_client.on_action_complete():
//   except exc.MistralException → send_error_back retry
//   except Exception → only log
func (e *DefaultExecutor) engineComplete(ctx context.Context, actionExID string, result *db.Result) {
	if e.engineClient == nil || actionExID == "" {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			// panic → retry with error result (Python's MistralException path)
			log.Printf("DefaultExecutor: panic on OnActionComplete for %s: %v", actionExID, rec)
			e.engineCompleteError(ctx, actionExID,
				fmt.Sprintf("OnActionComplete panic: %v", rec))
		}
	}()

	if err := e.engineClient.OnActionComplete(ctx, actionExID, result, false); err != nil {
		// Python MistralException → send_error_back retry
		log.Printf("DefaultExecutor: OnActionComplete failed for %s: %v", actionExID, err)
		e.engineCompleteError(ctx, actionExID,
			fmt.Sprintf("OnActionComplete failed: %v", err))
	}
}

// engineCompleteError retries OnActionComplete with an error result.
// Mirrors Python's send_error_back fallback path.
func (e *DefaultExecutor) engineCompleteError(ctx context.Context, actionExID string, msg string) {
	if e.engineClient == nil {
		return
	}
	errResult := &db.Result{Error: msg}
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("DefaultExecutor: send_error_back also panicked for %s: %v", actionExID, rec)
		}
	}()
	if err := e.engineClient.OnActionComplete(ctx, actionExID, errResult, false); err != nil {
		log.Printf("DefaultExecutor: send_error_back also failed for %s: %v", actionExID, err)
	}
}

// RemoteExecutor calls an HTTP executor server to run the action on a remote worker.
// Ported from mistral-master/mistral/executors/remote_executor.py which uses
// oslo.messaging RPC. Go uses a simple HTTP POST + JSON.
//
// 重要：RemoteExecutor 是 engine 进程里的组件，它 POST HTTP 到 executor 进程，
// 等 HTTP response 拿到 result 后返回。RemoteExecutor **不负责回调 engine**——
// 回调是 DefaultExecutor 的事。RemoteExecutor 只做 transport。
//
// Python 里没有对应 RemoteExecutor 的独立类——Python 用 oslo.messaging redelivery
// 机制把消息投递给远程 executor 进程。Go 用 HTTP POST 简化实现。
type RemoteExecutor struct {
	endpoint   string
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

type runActionResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

// RunAction POSTs the action to a remote executor server and returns the result.
// RemoteExecutor does NOT call engineClient — that's DefaultExecutor's job.
func (e *RemoteExecutor) RunAction(
	ctx context.Context,
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

	reqCtx := ctx
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
//
// Python: exe.get_executor(cfg.CONF.executor.type) → stevedore DriverManager
// Go:     executors.SetExecutorType(name) / executors.SetDefaultExecutor(inst)

var (
	defaultExecutor Executor
	executorsMu     sync.RWMutex
	executorTypes   = map[string]func() Executor{
		"local":  func() Executor { return NewDefaultExecutor(nil) },
		"remote": func() Executor { return NewRemoteExecutor("http://localhost:8952") },
	}
)

func init() {
	defaultExecutor = NewDefaultExecutor(nil)
}

// RegisterExecutorType adds a named constructor to the registry.
func RegisterExecutorType(name string, factory func() Executor) {
	executorsMu.Lock()
	executorTypes[name] = factory
	executorsMu.Unlock()
}

// SetExecutorType picks a built-in executor by name ("local", "remote").
// "local" creates a DefaultExecutor with nil engine client — callers should
// later call SetDefaultExecutor with a properly-wired DefaultExecutor(engineClient).
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
// In engine processes, this should be:
//   executors.SetDefaultExecutor(executors.NewDefaultExecutor(rpc.NewLocalEngineClient(engine)))
// In executor processes (if separate):
//   executors.SetDefaultExecutor(executors.NewDefaultExecutor(rpc.NewHttpEngineClient(engineURL)))
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

// GetExecutorType returns the name of the current default executor.
func GetExecutorType() string {
	executorsMu.RLock()
	defer executorsMu.RUnlock()
	switch defaultExecutor.(type) {
	case *DefaultExecutor:
		return "local"
	case *RemoteExecutor:
		return "remote"
	default:
		return "custom"
	}
}
