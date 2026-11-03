// Package enginerpc exposes the engine as an MQ RPC endpoint — mirrors
// Python reference/engine/engine_server.py exactly.
//
// Python engine_server.py:
//  1. Creates EngineServer(self.engine)
//  2. Creates RPC server via rpc.get_rpc_server_driver()(cfg.CONF.engine)
//  3. Calls rpc_server.register_endpoint(engine_server)
//  4. Calls rpc_server.start()
//
// Each public method is a @rpc_method — oslo.messaging dispatches by name.
// Go mirrors this: each method becomes server.Handle('name', handler).
package enginerpc

import (
	"context"
	"encoding/json"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/rpc"
)

// NewServer creates an Engine RPC server bound to transport on topic 'engine'.
// This mirrors Python's EngineServer class — its methods become RPC endpoints.
func NewServer(t rpc.Transport, eng engine.Engine) *rpc.RPCServer {
	s := rpc.NewRPCServer(t, "engine")
	s.Handle("start_workflow", func(ctx context.Context, args map[string]any) (any, error) {
		return eng.StartWorkflow(ctx,
			strOf(args, "wf_def_id"), strOf(args, "wf_namespace"),
			strOf(args, "wf_ex_id"),
			mapOf(args, "wf_input"), strOf(args, "description"), mapOf(args, "params"))
	})
	s.Handle("start_task", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.StartTask(ctx, strOf(args, "task_ex_id"),
			boolOf(args, "first_run"), boolOf(args, "waiting"),
			sliceOf(args, "triggered_by"),
			boolOf(args, "rerun"), boolOf(args, "reset"))
	})
	s.Handle("on_action_complete", func(ctx context.Context, args map[string]any) (any, error) {
		res := resultOf(args, "result")
		return nil, eng.OnActionComplete(ctx, strOf(args, "action_ex_id"), res, boolOf(args, "wf_action"))
	})
	s.Handle("pause_workflow", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.PauseWorkflow(ctx, strOf(args, "wf_ex_id"))
	})
	s.Handle("resume_workflow", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.ResumeWorkflow(ctx, strOf(args, "wf_ex_id"), mapOf(args, "env"))
	})
	s.Handle("stop_workflow", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.StopWorkflow(ctx, strOf(args, "wf_ex_id"), strOf(args, "state"), strOf(args, "message"))
	})
	s.Handle("rerun_workflow", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.RerunWorkflow(ctx, strOf(args, "task_ex_id"),
			boolOf(args, "reset"), boolOf(args, "skip"), mapOf(args, "env"))
	})
	s.Handle("on_action_update", func(ctx context.Context, args map[string]any) (any, error) {
		return nil, eng.OnActionUpdate(ctx, strOf(args, "wf_ex_id"))
	})
	return s
}

// --- helpers ---

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
		switch b := v.(type) {
		case bool:
			return b
		case string:
			return b == "true"
		}
	}
	return false
}

func mapOf(m map[string]any, k string) map[string]any {
	if v, ok := m[k]; ok && v != nil {
		if mm, ok := v.(map[string]any); ok {
			return mm
		}
		// fallback: round-trip JSON for nested types
		b, err := json.Marshal(v)
		if err == nil {
			var out map[string]any
			if err := json.Unmarshal(b, &out); err == nil {
				return out
			}
		}
	}
	return nil
}

func sliceOf(m map[string]any, k string) []map[string]any {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out []map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func resultOf(m map[string]any, k string) *db.Result {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var r db.Result
	if err := json.Unmarshal(b, &r); err == nil {
		return &r
	}
	return nil
}
