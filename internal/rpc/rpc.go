// Package rpc provides an RPC abstraction for cross-process engine communication.
// In this Go rewrite we start with a simple in-process stub that can be
// swapped for gRPC / HTTP / other transports later.
//
// Ported from mistral-master/mistral/rpc/
package rpc

import (
	"log"

	"github.com/mistral/workflow/internal/engine"
)

// EngineClient is the client side of engine RPC.
type EngineClient interface {
	StartWorkflow(wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (interface{}, error)
	StartTask(taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error
	OnActionComplete(actionExID string, result interface{}, wfAction bool) error
	PauseWorkflow(wfExID string) error
	ResumeWorkflow(wfExID string, env map[string]interface{}) error
	StopWorkflow(wfExID string, state string, message string) error
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

func (c *LocalEngineClient) StartWorkflow(wfDefID, wfNamespace string, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (interface{}, error) {
	return c.engine.StartWorkflow(wfDefID, wfNamespace, wfExID, wfInput, description, params)
}

func (c *LocalEngineClient) StartTask(taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error {
	return c.engine.StartTask(taskExID, firstRun, waiting, triggeredBy, rerun, reset)
}

func (c *LocalEngineClient) OnActionComplete(actionExID string, result interface{}, wfAction bool) error {
	log.Printf("OnActionComplete called via local RPC stub")
	return nil
}

func (c *LocalEngineClient) PauseWorkflow(wfExID string) error {
	return c.engine.PauseWorkflow(wfExID)
}

func (c *LocalEngineClient) ResumeWorkflow(wfExID string, env map[string]interface{}) error {
	return c.engine.ResumeWorkflow(wfExID, env)
}

func (c *LocalEngineClient) StopWorkflow(wfExID string, state string, message string) error {
	return c.engine.StopWorkflow(wfExID, state, message)
}
