// Package executorrpc exposes the executor as an MQ RPC endpoint — mirrors
// Python reference/executors/executor_server.py exactly.
//
// Python ExecutorServer:
//  1. Creates RPC server via rpc.get_rpc_server_driver()(cfg.CONF.executor)
//  2. Calls rpc_server.register_endpoint(self)
//  3. Calls rpc_server.start()
//  4. Every public method becomes an RPC endpoint automatically.
//
// Go mirrors this with RegisterEndpoint — public methods become RPC handlers
// via reflect, map[string]any args → typed request struct via mapstructure.
package executorrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/rpc"
)

// runActionRequest is the wire-format request — 1:1 with Python endpoint's
// run_action parameter list.
type runActionRequest struct {
	Action     string         `mapstructure:"action"`
	Input      map[string]any `mapstructure:"input"`
	ActionExID string         `mapstructure:"action_ex_id"`
	SafeRerun  bool           `mapstructure:"safe_rerun"`
	ExecCtx    map[string]any `mapstructure:"exec_ctx"`
	Target     string         `mapstructure:"target"`
	TimeoutSec int            `mapstructure:"timeout_sec"`
}

// ExecutorEndpoint is the endpoint object passed to RegisterEndpoint.
// Every public method (RunAction here) becomes an RPC handler.
//
//   - Method names are converted PascalCase → snake_case (RunAction → run_action)
//   - Signature: (ctx context.Context, *Req) (any, error)
//   - Args are decoded via mapstructure into *Req — WeaklyTypedInput handles
//     JSON float64 → int automatically.
type ExecutorEndpoint struct {
	Registry *actions.ActionRegistry
	Executor *executors.DefaultExecutor
}

// RunAction is the RPC endpoint — called automatically when topic=executor
// receives method='run_action'.
func (e *ExecutorEndpoint) RunAction(ctx context.Context, req *runActionRequest) (any, error) {
	desc, ok := e.Registry.Get(req.Action)
	if !ok {
		return nil, fmt.Errorf("unknown action %q", req.Action)
	}
	inst := desc.Factory()
	timeout := time.Duration(req.TimeoutSec) * time.Second
	return e.Executor.RunAction(ctx, inst, req.Input, req.ActionExID, req.SafeRerun, req.ExecCtx, req.Target, timeout)
}

// NewServer creates an Executor RPC server bound to transport on topic 'executor'.
func NewServer(t rpc.Transport, registry *actions.ActionRegistry) *rpc.RPCServer {
	s := rpc.NewRPCServer(t, "executor")
	s.RegisterEndpoint(&ExecutorEndpoint{
		Registry: registry,
		Executor: executors.NewDefaultExecutor(nil),
	})
	return s
}
