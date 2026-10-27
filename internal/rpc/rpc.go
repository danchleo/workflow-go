// Package rpc provides an RPC abstraction for cross-process engine communication.
//
// Ported from mistral-master/mistral/rpc/
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
	"github.com/danchleo/workflow-go/internal/engine"
)

// EngineClient is the client side of engine RPC.
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

// LocalEngineClient is an in-process implementation of EngineClient.
type LocalEngineClient struct {
	engine engine.Engine
}

func NewLocalEngineClient(e engine.Engine) *LocalEngineClient {
	return &LocalEngineClient{engine: e}
}

func (c *LocalEngineClient) StartWorkflow(
	ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string,
	params map[string]interface{},
) (*db.WorkflowExecution, error) {
	return c.engine.StartWorkflow(ctx, wfDefID, wfNamespace, wfExID, wfInput, description, params)
}

func (c *LocalEngineClient) StartTask(
	ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool,
	reset bool,
) error {
	return c.engine.StartTask(ctx, taskExID, firstRun, waiting, triggeredBy, rerun, reset)
}

func (c *LocalEngineClient) OnActionComplete(
	ctx context.Context, actionExID string, result *db.Result, wfAction bool,
) error {
	return c.engine.OnActionComplete(ctx, actionExID, result, wfAction)
}

func (c *LocalEngineClient) PauseWorkflow(ctx context.Context, wfExID string) error {
	return c.engine.PauseWorkflow(ctx, wfExID)
}

func (c *LocalEngineClient) ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error {
	return c.engine.ResumeWorkflow(ctx, wfExID, env)
}

func (c *LocalEngineClient) StopWorkflow(ctx context.Context, wfExID string, state string, message string) error {
	return c.engine.StopWorkflow(ctx, wfExID, state, message)
}

// --- HTTP Transport ---

// HttpEngineClient is an HTTP/JSON implementation of EngineClient.
// It POSTs JSON payloads to a remote Mistral API server — the HTTP API
// server must expose matching endpoints (/rpc/*).
// This mirrors how Python mistral uses oslo.messaging over JSON-RPC.
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

// rpcCall issues a POST to {baseURL}/rpc/{method} with body as JSON,
// decoding the response into outPtr (which should be *T or **T).
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
