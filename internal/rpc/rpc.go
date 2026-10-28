// Package rpc provides an RPC abstraction for cross-process engine communication.
//
// Ported from mistral-master/mistral/rpc/
//
// 架构（与 Python 对齐）：
//
//   EngineClient interface — 所有 engine RPC 方法的抽象
//     ├── LocalEngineClient (定义在 engine 包) — 进程内直接调 engine.Engine
//     └── HttpEngineClient  (本文件) — HTTP/JSON RPC，用于 executor 进程回调 engine
//
// 为什么 LocalEngineClient 不在 rpc 包？
//   LocalEngineClient 依赖 engine.Engine 接口。如果 rpc 包 import engine，
//   而 engine 又 import executors → executors import rpc → 循环依赖！
//   所以 LocalEngineClient 放在 engine 包，rpc 包只留接口 + HttpEngineClient。
//   Python 里 rpc.clients.py 里的 EngineClient 是 RPC client（oslo.messaging），
//   进程内 client 是另一套实现——Go 同理。
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
// Mirror of Python mistral.rpc.clients.EngineClient (oslo.messaging based).
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
// It POSTs JSON payloads to a remote Mistral API server — the HTTP API
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
