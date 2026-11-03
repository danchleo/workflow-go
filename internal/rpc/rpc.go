// Package rpc provides an RPC abstraction for cross-process engine communication.
//
// Ported from reference/rpc/
//
// 架构（与 Python 对齐）：
//
//	EngineClient interface — 所有 engine RPC 方法的抽象
//	  ├── LocalEngineClient (定义在 engine 包) — 进程内直接调 engine.Engine
//	  └── HttpEngineClient  (本文件) — HTTP/JSON RPC，用于 executor 进程回调 engine
//
// 为什么 LocalEngineClient 不在 rpc 包？
//
//	LocalEngineClient 依赖 engine.Engine 接口。如果 rpc 包 import engine，
//	而 engine 又 import executors → executors import rpc → 循环依赖！
//	所以 LocalEngineClient 放在 engine 包，rpc 包只留接口 + HttpEngineClient。
//	Python 里 rpc.clients.py 里的 EngineClient 是 RPC client（oslo.messaging），
//	进程内 client 是另一套实现——Go 同理。
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/danchleo/workflow-go/internal/db"
)

// EngineClient is the client side of engine RPC.
// Mirror of Python reference/rpc.clients.EngineClient (oslo.messaging based).
//
// Implementations:
//   - engine.LocalEngineClient  → in-process direct call to engine.Engine
//   - HttpEngineClient          → HTTP POST /rpc/* endpoints (executor process → engine process)
type EngineClient interface {
	StartWorkflow(
		ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{},
		description string, params map[string]interface{},
	) (*db.WorkflowExecution, error)
	StartTask(
		ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{},
		rerun bool, reset bool,
	) error
	OnActionComplete(ctx context.Context, actionExID string, result *db.Result, wfAction bool) error
	PauseWorkflow(ctx context.Context, wfExID string) error
	ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error
	StopWorkflow(ctx context.Context, wfExID string, state string, message string) error
}

// HttpEngineClient is an HTTP/JSON implementation of EngineClient.
// It POSTs JSON payloads to a remote Workflow API server — the HTTP API
// server must expose matching endpoints (/rpc/*).
//
// 用途：executor 进程里的 DefaultExecutor 持有 HttpEngineClient，
// RunAction 完成后 HTTP POST /rpc/on_action_complete 回调 engine 进程。
// Mirror of Python's rpc.EngineClient in a remote process (oslo.messaging based).
type HttpEngineClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHttpEngineClient(baseURL string) *HttpEngineClient {
	return &HttpEngineClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *HttpEngineClient) rpcCall(ctx context.Context, method string, body interface{}, outPtr interface{}) error {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/"+method, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("rpc %s failed (%d): %s", method, resp.StatusCode, string(respBody))
	}
	if outPtr != nil {
		return json.Unmarshal(respBody, outPtr)
	}
	return nil
}

type startWorkflowRequest struct {
	WFDefID     string                 `json:"wf_def_id"`
	WFNamespace string                 `json:"wf_namespace"`
	WFExID      string                 `json:"wf_ex_id"`
	WFInput     map[string]interface{} `json:"wf_input"`
	Description string                 `json:"description"`
	Params      map[string]interface{} `json:"params"`
}

func (c *HttpEngineClient) StartWorkflow(
	ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string,
	params map[string]interface{},
) (*db.WorkflowExecution, error) {
	var result db.WorkflowExecution
	err := c.rpcCall(
		ctx, "start_workflow", startWorkflowRequest{
			WFDefID: wfDefID, WFNamespace: wfNamespace, WFExID: wfExID,
			WFInput: wfInput, Description: description, Params: params,
		}, &result,
	)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *HttpEngineClient) StartTask(
	ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool,
	reset bool,
) error {
	return c.rpcCall(
		ctx, "start_task", map[string]interface{}{
			"task_ex_id":   taskExID,
			"first_run":    firstRun,
			"waiting":      waiting,
			"triggered_by": triggeredBy,
			"rerun":        rerun,
			"reset":        reset,
		}, nil,
	)
}

func (c *HttpEngineClient) OnActionComplete(
	ctx context.Context, actionExID string, result *db.Result, wfAction bool,
) error {
	return c.rpcCall(
		ctx, "on_action_complete", map[string]interface{}{
			"action_ex_id": actionExID,
			"result":       result.ToDict(),
			"wf_action":    wfAction,
		}, nil,
	)
}

func (c *HttpEngineClient) PauseWorkflow(ctx context.Context, wfExID string) error {
	return c.rpcCall(ctx, "pause_workflow", map[string]interface{}{"wf_ex_id": wfExID}, nil)
}

func (c *HttpEngineClient) ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error {
	return c.rpcCall(ctx, "resume_workflow", map[string]interface{}{"wf_ex_id": wfExID, "env": env}, nil)
}

func (c *HttpEngineClient) StopWorkflow(ctx context.Context, wfExID string, state string, message string) error {
	return c.rpcCall(
		ctx, "stop_workflow", map[string]interface{}{"wf_ex_id": wfExID, "state": state, "message": message}, nil,
	)
}

// --- EngineRPCClient — MQ topic-based, 100% Python oslo.messaging ---

// EngineRPCClient implements EngineClient over topic='engine' MQ RPC.
// DefaultExecutor in executor process uses this to call engine.
// Python: rpc.get_engine_client() → EngineClient(topic='engine').
//
// Methods default to AsyncCall (cast, fire-and-forget) — matches Python
// async_=True default. StartWorkflow is SyncCall (needs reply).
type EngineRPCClient struct {
	TopicClient *RPCClient
}

// NewEngineRPCClient creates an EngineRPCClient on topic 'engine'.
func NewEngineRPCClient(t Transport) *EngineRPCClient {
	return &EngineRPCClient{TopicClient: &RPCClient{Transport: t, Topic: "engine"}}
}

func (c *EngineRPCClient) StartWorkflow(
	ctx context.Context, wfDefID, wfNamespace, wfExID string, wfInput map[string]interface{},
	description string, params map[string]interface{},
) (*db.WorkflowExecution, error) {
	var exec db.WorkflowExecution
	err := c.TopicClient.SyncCall(ctx, "start_workflow", map[string]any{
		"wf_def_id": wfDefID, "wf_namespace": wfNamespace,
		"wf_ex_id": wfExID, "wf_input": wfInput,
		"description": description, "params": params,
	}, &exec)
	if err != nil {
		return nil, err
	}
	return &exec, nil
}

func (c *EngineRPCClient) StartTask(
	ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{},
	rerun bool, reset bool,
) error {
	return c.TopicClient.AsyncCall(ctx, "start_task", map[string]any{
		"task_ex_id": taskExID, "first_run": firstRun, "waiting": waiting,
		"triggered_by": triggeredBy, "rerun": rerun, "reset": reset,
	})
}

func (c *EngineRPCClient) OnActionComplete(ctx context.Context, actionExID string, result *db.Result, wfAction bool) error {
	return c.TopicClient.AsyncCall(ctx, "on_action_complete", map[string]any{
		"action_ex_id": actionExID, "result": result.ToDict(), "wf_action": wfAction,
	})
}

func (c *EngineRPCClient) PauseWorkflow(ctx context.Context, wfExID string) error {
	return c.TopicClient.AsyncCall(ctx, "pause_workflow", map[string]any{"wf_ex_id": wfExID})
}

func (c *EngineRPCClient) ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error {
	return c.TopicClient.AsyncCall(ctx, "resume_workflow", map[string]any{"wf_ex_id": wfExID, "env": env})
}

func (c *EngineRPCClient) StopWorkflow(ctx context.Context, wfExID string, state string, message string) error {
	return c.TopicClient.AsyncCall(ctx, "stop_workflow", map[string]any{
		"wf_ex_id": wfExID, "state": state, "message": message,
	})
}

// ExecutorRPCClient wraps RPCClient on topic 'executor'.
// Engine uses this to send run_action to executor — matches Python's
// rpc.get_executor_client().run_action(..., async_=True).
type ExecutorRPCClient struct {
	TopicClient *RPCClient
}

// NewExecutorRPCClient creates an ExecutorRPCClient on topic 'executor'.
func NewExecutorRPCClient(t Transport) *ExecutorRPCClient {
	return &ExecutorRPCClient{TopicClient: &RPCClient{Transport: t, Topic: "executor"}}
}

// RunAction sends run_action to executor topic (async by default, matches Python async_=True).
// Python: self._executor_client.run_action(..., async_=True)
func (c *ExecutorRPCClient) RunAction(
	ctx context.Context, actionName string, input map[string]interface{}, actionExID string,
	safeRerun bool, execCtx map[string]interface{}, target string, timeoutSec int,
) error {
	return c.TopicClient.AsyncCall(ctx, "run_action", map[string]any{
		"action": actionName, "input": input, "action_ex_id": actionExID,
		"safe_rerun": safeRerun, "exec_ctx": execCtx,
		"target": target, "timeout_sec": timeoutSec,
	})
}
