// Package rpc provides an RPC abstraction for cross-process engine communication.
//
// Ported from reference/rpc/
//
// 架构（与 Python 对齐）：
//
//	EngineClient interface — 所有 engine RPC 方法的抽象
//	  ├── engine.LocalEngineClient  — 进程内直接调 engine.Engine
//	  └── EngineRPCClient           — MQ topic='engine' RPC（跨进程）
//
// 为什么 LocalEngineClient 不在 rpc 包？
//
//	LocalEngineClient 依赖 engine.Engine 接口。如果 rpc 包 import engine，
//	而 engine 又 import executors → executors import rpc → 循环依赖！
//	所以 LocalEngineClient 放在 engine 包，rpc 包只留接口 + MQ 实现。
package rpc

import (
	"context"

	"github.com/danchleo/workflow-go/internal/db"
)

// EngineClient is the client side of engine RPC.
// Mirror of Python reference/rpc.clients.EngineClient (oslo.messaging based).
//
// Implementations:
//   - engine.LocalEngineClient   → in-process direct call to engine.Engine
//   - EngineRPCClient            → MQ topic='engine' RPC (跨进程)
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
