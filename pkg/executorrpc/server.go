// Package executorrpc exposes the executor as an MQ RPC endpoint — mirrors
// Python reference/executors/executor_server.py exactly.
//
// Python ExecutorServer:
//  1. Creates RPC server via rpc.get_rpc_server_driver()(cfg.CONF.executor)
//  2. Calls rpc_server.register_endpoint(self)
//  3. Calls rpc_server.start()
//  4. Endpoint method: run_action(action, action_ex_id, safe_rerun, exec_ctx,
//     timeout, target) → calls self.executor.run_action() → returns result
//
// Single topic 'executor', single method 'run_action'.
package executorrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/rpc"
)

// NewServer creates an Executor RPC server bound to transport on topic 'executor'.
func NewServer(t rpc.Transport, registry *actions.ActionRegistry) *rpc.RPCServer {
	// Python DefaultExecutor in executor process holds rpc.EngineClient so it
	// can call back on_action_complete. We pass nil here — the executor server
	// is pure: runs action, returns result. Caller (engine) handles callback.
	exec := executors.NewDefaultExecutor(nil)

	s := rpc.NewRPCServer(t, "executor")
	s.Handle("run_action", func(ctx context.Context, args map[string]any) (any, error) {
		actionName := strOf(args, "action")
		input := mapOf(args, "input")
		actionExID := strOf(args, "action_ex_id")
		safeRerun := boolOf(args, "safe_rerun")
		execCtx := mapOf(args, "exec_ctx")
		target := strOf(args, "target")
		timeout := 0 * time.Second
		if to, ok := args["timeout_sec"].(float64); ok {
			timeout = time.Duration(to) * time.Second
		}

		desc, ok := registry.Get(actionName)
		if !ok {
			return nil, fmt.Errorf("unknown action %q", actionName)
		}
		inst := desc.Factory()
		res, err := exec.RunAction(ctx, inst, input, actionExID, safeRerun, execCtx, target, timeout)
		if err != nil {
			return nil, err
		}
		return res, nil
	})
	return s
}

func strOf(m map[string]any, k string) string {
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
func boolOf(m map[string]any, k string) bool {
	if v, ok := m[k]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
func mapOf(m map[string]any, k string) map[string]any {
	if v, ok := m[k]; ok && v != nil {
		if mm, ok := v.(map[string]any); ok {
			return mm
		}
	}
	return nil
}

var _ = fmt.Errorf
