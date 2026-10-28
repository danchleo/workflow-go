// Package api provides a minimal REST API for the workflow engine.
// Ported from mistral-master/mistral/api/
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/lang/parser"
	"github.com/danchleo/workflow-go/internal/services"
)

// Server is the Mistral API server.
type Server struct {
	store      db.Store
	engine     engine.Engine
	wfService  *services.WorkflowService
	httpServer *http.Server
}

// NewServer creates a new API server.
func NewServer(store db.Store, eng engine.Engine) *Server {
	return &Server{
		store:     store,
		engine:    eng,
		wfService: services.NewWorkflowService(store),
	}
}

// Router returns the HTTP handler.
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// Health check.
	mux.HandleFunc("/health", s.handleHealth)

	// Workflow definitions.
	mux.HandleFunc("/v2/workflows", s.handleWorkflows)

	// Workflow executions.
	mux.HandleFunc("/v2/executions", s.handleExecutions)
	mux.HandleFunc("/v2/executions/", s.handleExecutionByID)

	// Task executions.
	mux.HandleFunc("/v2/tasks", s.handleTasks)

	// Action executions.
	mux.HandleFunc("/v2/action_executions", s.handleActionExecutions)

	// Engine RPC endpoints (used by HttpEngineClient for cross-process calls).
	mux.HandleFunc("/rpc/", s.handleRPC)

	return mux
}

// RouterEngineOnly returns a minimal handler with /health + /rpc/* only.
// Used by the standalone engine process (cmd/engine) which does NOT need
// the full REST surface (workflow CRUD, execution listing, etc.) — those
// live in the API process.
func (s *Server) RouterEngineOnly() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/rpc/", s.handleRPC)
	return mux
}

// Start starts the HTTP server on the given address.
func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.Router(),
	}
	log.Printf("Mistral API server listening on %s", addr)
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the API server with the given context.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	log.Printf("Shutting down API server...")
	return s.httpServer.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		wfs, err := s.wfService.ListWorkflows("", "")
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, wfs)

	case http.MethodPost:
		var body struct {
			Name       string `json:"name"`
			Definition string `json:"definition"`
			Namespace  string `json:"namespace"`
			ProjectID  string `json:"project_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
			return
		}
		if body.ProjectID == "" {
			body.ProjectID = "default"
		}
		wf, err := s.wfService.CreateWorkflow(body.Name, body.Definition, body.ProjectID, body.Namespace)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusCreated, wf)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		wfExecs, err := s.store.WorkflowExecutions().List("")
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, wfExecs)

	case http.MethodPost:
		var body struct {
			WorkflowName string                 `json:"workflow_name"`
			Input        map[string]interface{} `json:"input"`
			Description  string                 `json:"description"`
			Namespace    string                 `json:"namespace"`
			Params       map[string]interface{} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
			return
		}
		if body.Input == nil {
			body.Input = make(map[string]interface{})
		}
		if body.Params == nil {
			body.Params = make(map[string]interface{})
		}

		wfDef, err := s.store.WorkflowDefinitions().GetByName(body.WorkflowName, body.Namespace, "")
		if err != nil {
			writeError(w, http.StatusNotFound, fmt.Errorf("workflow '%s' not found", body.WorkflowName))
			return
		}

		params := body.Params
		if body.Namespace != "" {
			params["namespace"] = body.Namespace
		}
		if _, ok := params["env"]; !ok {
			params["env"] = make(map[string]interface{})
		}

		wfEx, err := s.engine.StartWorkflow(
			r.Context(), wfDef.ID, body.Namespace, "", body.Input, body.Description, params,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusCreated, wfEx)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleExecutionByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/v2/executions/"):]
	if id == "" {
		http.Error(w, "execution id required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		wfEx, err := s.store.WorkflowExecutions().Get(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, wfEx)

	case http.MethodPut:
		var body struct {
			State   string `json:"state"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if body.State == "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("state required"))
			return
		}
		if err := s.engine.StopWorkflow(r.Context(), id, body.State, body.Message); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		wfEx, _ := s.store.WorkflowExecutions().Get(id)
		writeJSON(w, http.StatusOK, wfEx)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tasks, err := s.store.TaskExecutions().List("", "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) handleActionExecutions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	actions, err := s.store.ActionExecutions().List("", "")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, actions)
}

// --- Helpers ---

// NewDemoWorkflowDef creates an example workflow definition for demos/tests.
func NewDemoWorkflowDef() string {
	return `
version: "2.0"
name: demo_workflow

input:
  - greeting: "Hello"
  - name: "World"

tasks:
  task1:
    action: std.echo
    input:
      output: "<% $.greeting %>, <% $.name %>!"
    publish:
      message: "<% $.task1.result %>"
    on-success:
      - task2

  task2:
    action: std.echo
    input:
      output: "<% $.message %> — done"

output:
  final_message: "<% $.message %>"
`
}

// NewReverseDemoWorkflowDef creates a reverse workflow example.
func NewReverseDemoWorkflowDef() string {
	return `
version: "2.0"
name: demo_reverse

tasks:
  task1:
    action: std.echo
    input:
      output: "step1"

  task2:
    action: std.echo
    input:
      output: "step2"
    requires: [task1]

  task3:
    action: std.echo
    input:
      output: "final"
    requires: [task2]
`
}

// ParseDemoWorkflowDef is a helper — exported for convenience.
func ParseDemoWorkflowDef(yamlDef string) (map[string]interface{}, error) {
	return parser.ParseYAML(yamlDef)
}

// --- Engine RPC handlers (used by HttpEngineClient) ---

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("POST required"))
		return
	}
	// Extract method name from URL path: /rpc/{method}
	method := r.URL.Path[len("/rpc/"):]
	if method == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("missing rpc method"))
		return
	}
	log.Printf("RPC call: %s", method)

	switch method {
	case "start_workflow":
		s.rpcStartWorkflow(w, r)
	case "start_task":
		s.rpcStartTask(w, r)
	case "on_action_complete":
		s.rpcOnActionComplete(w, r)
	case "pause_workflow":
		s.rpcPauseWorkflow(w, r)
	case "resume_workflow":
		s.rpcResumeWorkflow(w, r)
	case "stop_workflow":
		s.rpcStopWorkflow(w, r)
	default:
		writeError(w, http.StatusNotFound, fmt.Errorf("unknown rpc method: %s", method))
	}
}

type rpcStartWorkflowBody struct {
	WFDefID     string                 `json:"wf_def_id"`
	WFNamespace string                 `json:"wf_namespace"`
	WFExID      string                 `json:"wf_ex_id"`
	WFInput     map[string]interface{} `json:"wf_input"`
	Description string                 `json:"description"`
	Params      map[string]interface{} `json:"params"`
}

func (s *Server) rpcStartWorkflow(w http.ResponseWriter, r *http.Request) {
	var body rpcStartWorkflowBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	wfEx, err := s.engine.StartWorkflow(
		r.Context(), body.WFDefID, body.WFNamespace, body.WFExID, body.WFInput, body.Description, body.Params,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, wfEx)
}

func (s *Server) rpcStartTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TaskExID    string                   `json:"task_ex_id"`
		FirstRun    bool                     `json:"first_run"`
		Waiting     bool                     `json:"waiting"`
		TriggeredBy []map[string]interface{} `json:"triggered_by"`
		Rerun       bool                     `json:"rerun"`
		Reset       bool                     `json:"reset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.engine.StartTask(
		r.Context(), body.TaskExID, body.FirstRun, body.Waiting, body.TriggeredBy, body.Rerun, body.Reset,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) rpcOnActionComplete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ActionExID string                 `json:"action_ex_id"`
		Result     map[string]interface{} `json:"result"`
		WFAction   bool                   `json:"wf_action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	res := &db.Result{}
	if body.Result != nil {
		res.FromDict(body.Result)
	}
	if err := s.engine.OnActionComplete(r.Context(), body.ActionExID, res, body.WFAction); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) rpcPauseWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WFExID string `json:"wf_ex_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if err := s.engine.PauseWorkflow(r.Context(), body.WFExID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) rpcResumeWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WFExID string                 `json:"wf_ex_id"`
		Env    map[string]interface{} `json:"env"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if err := s.engine.ResumeWorkflow(r.Context(), body.WFExID, body.Env); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) rpcStopWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WFExID  string `json:"wf_ex_id"`
		State   string `json:"state"`
		Message string `json:"message"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if err := s.engine.StopWorkflow(r.Context(), body.WFExID, body.State, body.Message); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
