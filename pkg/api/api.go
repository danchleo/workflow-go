// Package api provides a minimal REST API for the workflow engine.
// Ported from mistral-master/mistral/api/
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/engine"
	"github.com/mistral/workflow/internal/lang/parser"
	"github.com/mistral/workflow/internal/services"
)

// Server is the Mistral API server.
type Server struct {
	store          db.Store
	engine         engine.Engine
	wfService      *services.WorkflowService
	httpServer     *http.Server
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

		wfEx, err := s.engine.StartWorkflow(wfDef.ID, body.Namespace, "", body.Input, body.Description, params)
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
		if err := s.engine.StopWorkflow(id, body.State, body.Message); err != nil {
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
