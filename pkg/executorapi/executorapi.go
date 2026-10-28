// Package executorapi exposes an HTTP endpoint for remote action execution.
//
// This is the Go equivalent of Python mistral's ExecutorServer: the engine
// (or any caller with an HttpEngineClient-compatible interface) POSTs a
// JSON runActionRequest to /v1/actions/run, this server runs the action
// locally via LocalExecutor, and returns the JSON result.
//
// In the default single-process deployment this server is NOT needed — the
// engine uses LocalExecutor directly. But for multi-process deployments
// (engine on node A, executor on node B) the engine creates a RemoteExecutor
// pointing at this server's URL.
//
// Wire protocol mirrors internal/executors:
//
//	POST /v1/actions/run
//	Request:  { action, input, action_ex_id, safe_rerun, exec_ctx, target, timeout_sec }
//	Response: { data } or { error }
package executorapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/executors"
)

// Server wraps a LocalExecutor and exposes it over HTTP.
type Server struct {
	executor executors.Executor
	registry *actions.ActionRegistry
}

// NewServer creates an Executor HTTP server backed by LocalExecutor.
func NewServer() *Server {
	return &Server{
		executor: executors.NewLocalExecutor(),
		registry: actions.NewActionRegistry(),
	}
}

// Router returns the HTTP handler.
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/v1/actions/run", s.handleRunAction)
	return mux
}

// Start launches the HTTP server on addr (blocking).
func (s *Server) Start(addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 300 * time.Second, // actions may take a while
	}
	log.Printf("executor server listening on %s", addr)
	return srv.ListenAndServe()
}

// handleHealth returns a simple OK — used by load balancers / process managers.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "executor"})
}

// runActionRequest mirrors internal/executors.runActionRequest — same JSON contract
// so RemoteExecutor (engine-side) and this server (executor-side) are wire-compatible.
type runActionRequest struct {
	Action     string                 `json:"action"`
	Input      map[string]interface{} `json:"input"`
	ActionExID string                 `json:"action_ex_id"`
	SafeRerun  bool                   `json:"safe_rerun"`
	ExecCtx    map[string]interface{} `json:"exec_ctx"`
	Target     string                 `json:"target,omitempty"`
	TimeoutSec int                    `json:"timeout_sec"`
}

type runActionResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func (s *Server) handleRunAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}

	var body runActionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}

	// Look up action descriptor from registry; Factory creates fresh instance.
	desc, ok := s.registry.Get(body.Action)
	if !ok {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown action: %s", body.Action))
		return
	}
	actionInstance := desc.Factory()

	var timeout time.Duration
	if body.TimeoutSec > 0 {
		timeout = time.Duration(body.TimeoutSec) * time.Second
	}

	log.Printf("executor: running action=%s action_ex_id=%s timeout=%v", body.Action, body.ActionExID, timeout)

	// executorapi.Server 是独立的 executor 进程 — 它不知道 engine 在哪，
	// 所以传 nil callback。Executor server 的作用就是跑 action 然后通过 HTTP response
	// 把 result 返回给 engine（engine 进程拿到 result 后自己调 OnActionComplete）。
	// 这跟 Python ExecutorServer 不同——Python 的 ExecutorServer 持有 engine RPC client，
	// 能自己回调。Go 的 HTTP 协议是同步 request/response，所以 engine 通过 RemoteExecutor
	// 等 HTTP 返回后调 callback。
	result, _ := s.executor.RunAction(
		actionInstance, body.Input, body.ActionExID, body.SafeRerun,
		body.ExecCtx, body.Target, timeout, nil,
	)

	resp := runActionResponse{}
	if result.Error != nil {
		switch e := result.Error.(type) {
		case string:
			resp.Error = e
		case error:
			resp.Error = e.Error()
		default:
			resp.Error = fmt.Sprintf("%v", result.Error)
		}
	} else {
		resp.Data = result.Data
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	log.Printf("executor error %d: %v", code, err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
