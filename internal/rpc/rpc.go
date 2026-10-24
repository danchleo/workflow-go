// Package rpc provides an RPC abstraction for cross-process engine communication.
// In this Go rewrite we start with a simple in-process stub that can be
// swapped for gRPC / HTTP / other transports later.
//
// Ported from mistral-master/mistral/rpc/
package rpc

import (
	"context"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/engine"
)

// EngineClient is the client side of engine RPC.
type EngineClient interface {
	StartWorkflow(ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error)
	StartTask(ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error
	OnActionComplete(ctx context.Context, actionExID string, result *db.Result, wfAction bool) error
	PauseWorkflow(ctx context.Context, wfExID string) error
	ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error
	StopWorkflow(ctx context.Context, wfExID string, state string, message string) error
}

// LocalEngineClient is an in-process implementation of EngineClient.
// It calls the engine directly rather than sending RPCs.
type LocalEngineClient struct {
	engine engine.Engine
}

// NewLocalEngineClient wraps an engine instance as an EngineClient.
func NewLocalEngineClient(e engine.Engine) *LocalEngineClient {
	return &LocalEngineClient{engine: e}
}

func (c *LocalEngineClient) StartWorkflow(ctx context.Context, wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error) {
	return c.engine.StartWorkflow(ctx, wfDefID, wfNamespace, wfExID, wfInput, description, params)
}

func (c *LocalEngineClient) StartTask(ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error {
	return c.engine.StartTask(ctx, taskExID, firstRun, waiting, triggeredBy, rerun, reset)
}

func (c *LocalEngineClient) OnActionComplete(ctx context.Context, actionExID string, result *db.Result, wfAction bool) error {
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
