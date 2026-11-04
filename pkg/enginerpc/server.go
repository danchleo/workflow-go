// Package enginerpc exposes the engine as an MQ RPC endpoint — mirrors
// Python reference/engine/engine_server.py exactly.
//
// Python engine_server.py:
//  1. Creates EngineServer(self.engine)
//  2. Creates RPC server via rpc.get_rpc_server_driver()(cfg.CONF.engine)
//  3. Calls rpc_server.register_endpoint(engine_server)
//  4. Every public method is auto-registered as RPC endpoint.
//
// Go mirrors this with RegisterEndpoint — public methods → reflect → handlers,
// mapstructure decodes map[string]any args into typed request structs.
package enginerpc

import (
	"context"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/rpc"
)

// --- Wire-format request structs (1:1 with Python endpoint method params) ---

type startWorkflowRequest struct {
	WFDefID     string                 `mapstructure:"wf_def_id"`
	WFNamespace string                 `mapstructure:"wf_namespace"`
	WFExID      string                 `mapstructure:"wf_ex_id"`
	WFInput     map[string]interface{} `mapstructure:"wf_input"`
	Description string                 `mapstructure:"description"`
	Params      map[string]interface{} `mapstructure:"params"`
}

type startTaskRequest struct {
	TaskExID    string                   `mapstructure:"task_ex_id"`
	FirstRun    bool                     `mapstructure:"first_run"`
	Waiting     bool                     `mapstructure:"waiting"`
	TriggeredBy []map[string]interface{} `mapstructure:"triggered_by"`
	Rerun       bool                     `mapstructure:"rerun"`
	Reset       bool                     `mapstructure:"reset"`
}

type onActionCompleteRequest struct {
	ActionExID string     `mapstructure:"action_ex_id"`
	Result     *db.Result `mapstructure:"result"`
	WFAction   bool       `mapstructure:"wf_action"`
}

type pauseWorkflowRequest struct {
	WFExID string `mapstructure:"wf_ex_id"`
}

type resumeWorkflowRequest struct {
	WFExID string                 `mapstructure:"wf_ex_id"`
	Env    map[string]interface{} `mapstructure:"env"`
}

type stopWorkflowRequest struct {
	WFExID  string `mapstructure:"wf_ex_id"`
	State   string `mapstructure:"state"`
	Message string `mapstructure:"message"`
}

type rerunWorkflowRequest struct {
	TaskExID string                 `mapstructure:"task_ex_id"`
	Reset    bool                   `mapstructure:"reset"`
	Skip     bool                   `mapstructure:"skip"`
	Env      map[string]interface{} `mapstructure:"env"`
}

type onActionUpdateRequest struct {
	WFExID string `mapstructure:"wf_ex_id"`
}

// EngineEndpoint is the object RegisterEndpoint scans for public methods.
// Each public method → one RPC endpoint.
type EngineEndpoint struct {
	Engine engine.Engine
}

func (e *EngineEndpoint) StartWorkflow(ctx context.Context, req *startWorkflowRequest) (any, error) {
	return e.Engine.StartWorkflow(ctx, req.WFDefID, req.WFNamespace, req.WFExID, req.WFInput, req.Description, req.Params)
}

func (e *EngineEndpoint) StartTask(ctx context.Context, req *startTaskRequest) error {
	return e.Engine.StartTask(ctx, req.TaskExID, req.FirstRun, req.Waiting, req.TriggeredBy, req.Rerun, req.Reset)
}

func (e *EngineEndpoint) OnActionComplete(ctx context.Context, req *onActionCompleteRequest) error {
	return e.Engine.OnActionComplete(ctx, req.ActionExID, req.Result, req.WFAction)
}

func (e *EngineEndpoint) PauseWorkflow(ctx context.Context, req *pauseWorkflowRequest) error {
	return e.Engine.PauseWorkflow(ctx, req.WFExID)
}

func (e *EngineEndpoint) ResumeWorkflow(ctx context.Context, req *resumeWorkflowRequest) error {
	return e.Engine.ResumeWorkflow(ctx, req.WFExID, req.Env)
}

func (e *EngineEndpoint) StopWorkflow(ctx context.Context, req *stopWorkflowRequest) error {
	return e.Engine.StopWorkflow(ctx, req.WFExID, req.State, req.Message)
}

func (e *EngineEndpoint) RerunWorkflow(ctx context.Context, req *rerunWorkflowRequest) error {
	return e.Engine.RerunWorkflow(ctx, req.TaskExID, req.Reset, req.Skip, req.Env)
}

func (e *EngineEndpoint) OnActionUpdate(ctx context.Context, req *onActionUpdateRequest) error {
	return e.Engine.OnActionUpdate(ctx, req.WFExID)
}

// NewServer creates an Engine RPC server bound to transport on topic 'engine'.
func NewServer(t rpc.Transport, eng engine.Engine) *rpc.RPCServer {
	s := rpc.NewRPCServer(t, "engine")
	s.RegisterEndpoint(&EngineEndpoint{Engine: eng})
	return s
}
