package engine

import (
	"fmt"
	"log"

	"github.com/mistral/workflow/internal/commands"
	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/expressions"
	"github.com/mistral/workflow/internal/lang"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"github.com/mistral/workflow/internal/states"
	"github.com/mistral/workflow/internal/workflow"
)

// DefaultEngine is the main engine implementation.
// Ported from mistral-master/mistral/engine/default_engine.py
type DefaultEngine struct {
	store      db.Store
	dispatcher *Dispatcher
}

// NewDefaultEngine creates a DefaultEngine with the given store.
func NewDefaultEngine(store db.Store) *DefaultEngine {
	return &DefaultEngine{
		store:      store,
		dispatcher: NewDispatcher(store),
	}
}

// Store returns the underlying data store.
func (e *DefaultEngine) Store() db.Store { return e.store }

// Dispatcher returns the command dispatcher.
func (e *DefaultEngine) Dispatcher() *Dispatcher { return e.dispatcher }

// StartWorkflow creates a new workflow execution and starts processing.
func (e *DefaultEngine) StartWorkflow(wfDefID, wfNamespace, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error) {
	wfDefs := e.store.WorkflowDefinitions()
	wfDef, err := wfDefs.Get(wfDefID)
	if err != nil {
		return nil, fmt.Errorf("workflow definition not found: %w", err)
	}

	wfSpec, err := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow spec: %w", err)
	}

	// Validate and prepare input.
	if wfInput == nil {
		wfInput = make(map[string]interface{})
	}
	for k, v := range wfSpec.GetInput() {
		if _, ok := wfInput[k]; !ok {
			if v == lang.NotDefined {
				return nil, fmt.Errorf("missing required input: %s", k)
			}
			wfInput[k] = v
		}
	}

	// Create workflow execution.
	wfEx := &db.WorkflowExecution{
		Name:              wfDef.Name,
		Description:       description,
		WorkflowName:      wfDef.Name,
		WorkflowNamespace: wfDef.Namespace,
		WorkflowID:        wfDef.ID,
		Spec:              wfDef.Spec,
		State:             states.RUNNING,
		Input:             wfInput,
		Params:            params,
		Context:           make(map[string]interface{}),
		RuntimeContext:    make(map[string]interface{}),
		ProjectID:         wfDef.ProjectID,
	}
	if wfExID != "" {
		wfEx.ID = wfExID
	}

	// Initialize context with workflow variables.
	for k, v := range wfSpec.GetVars() {
		wfEx.Context[k] = expressions.EvaluateRecursively(v, map[string]interface{}{
			"__env":  params["env"],
			"input":  wfInput,
			"params": params,
		})
	}
	for k, v := range wfInput {
		wfEx.Context[k] = v
	}

	if params["task_execution_id"] != nil {
		if id, ok := params["task_execution_id"].(string); ok {
			wfEx.TaskExecutionID = id
		}
	}
	if params["root_execution_id"] != nil {
		if id, ok := params["root_execution_id"].(string); ok {
			wfEx.RootExecutionID = id
		}
	}

	// Create execution in DB.
	if err := e.store.WorkflowExecutions().Create(wfEx); err != nil {
		return nil, fmt.Errorf("creating workflow execution: %w", err)
	}

	// Find controller and dispatch start commands.
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
	ctrl := workflow.GetController(wfEx, wfSpec, tasks)
	cmds := ctrl.ContinueWorkflow(nil)

	if err := e.dispatcher.DispatchWorkflowCommands(wfEx, cmds); err != nil {
		return wfEx, err
	}

	return wfEx, nil
}

// PauseWorkflow pauses a running workflow.
func (e *DefaultEngine) PauseWorkflow(wfExID string) error {
	wfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil {
		return err
	}
	if states.IsPaused(wfEx.State) {
		return nil
	}
	wfEx.State = states.PAUSED
	return e.store.WorkflowExecutions().Update(wfEx)
}

// ResumeWorkflow resumes a paused workflow.
func (e *DefaultEngine) ResumeWorkflow(wfExID string, env map[string]interface{}) error {
	wfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil {
		return err
	}
	if !states.IsPaused(wfEx.State) {
		return fmt.Errorf("workflow %s is not paused", wfExID)
	}

	if env != nil {
		wfEx.Params["env"] = env
	}

	wfEx.State = states.RUNNING

	// Load workflow spec and tasks.
	wfDef, err := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if err != nil {
		wfEx.RuntimeContext["wf_spec"] = nil
		return e.store.WorkflowExecutions().Update(wfEx)
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	ctrl := workflow.GetController(wfEx, wfSpec, tasks)
	cmds := ctrl.ContinueWorkflow(nil)

	if err := e.store.WorkflowExecutions().Update(wfEx); err != nil {
		return err
	}

	return e.dispatcher.DispatchWorkflowCommands(wfEx, cmds)
}

// StopWorkflow sets a workflow to a terminal state.
func (e *DefaultEngine) StopWorkflow(wfExID string, state string, message string) error {
	wfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil {
		return err
	}
	if !states.IsValidTransition(wfEx.State, state) {
		return fmt.Errorf("invalid state transition: %s -> %s", wfEx.State, state)
	}

	wfEx.State = state
	wfEx.StateInfo = message

	// Evaluate output for terminal states.
	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef != nil {
		wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
		tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
		ctrl := workflow.GetController(wfEx, wfSpec, tasks)
		finalCtx, _ := ctrl.EvaluateWorkflowFinalContext()

		if state == states.SUCCESS {
			if wfSpec.GetOutput() != nil {
				wfEx.Output = evaluateOutput(wfSpec.GetOutput(), finalCtx)
			}
		} else if state == states.ERROR {
			if wfSpec.GetOutputOnError() != nil {
				wfEx.Output = evaluateOutput(wfSpec.GetOutputOnError(), finalCtx)
			}
			if wfEx.Output == nil {
				wfEx.Output = make(map[string]interface{})
			}
			wfEx.Output["result"] = message
		} else if state == states.CANCELLED {
			wfEx.Output = map[string]interface{}{"result": message}
		}
	}

	return e.store.WorkflowExecutions().Update(wfEx)
}

// RerunWorkflow reruns a workflow from a specific task execution.
func (e *DefaultEngine) RerunWorkflow(taskExID string, reset bool, skip bool, env map[string]interface{}) error {
	taskEx, err := e.store.TaskExecutions().Get(taskExID)
	if err != nil {
		return err
	}

	wfEx, err := e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
	if err != nil {
		return err
	}

	if env != nil {
		if wfEx.Params == nil {
			wfEx.Params = make(map[string]interface{})
		}
		wfEx.Params["env"] = env
	}

	// Reset task state.
	if reset {
		taskEx.State = states.IDLE
		taskEx.Processed = false
		taskEx.Output = nil
		taskEx.Published = make(map[string]interface{})
		if err := e.store.TaskExecutions().Update(taskEx); err != nil {
			return err
		}
	}

	wfDef, err := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if err != nil {
		return err
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	wfEx.State = states.RUNNING
	if err := e.store.WorkflowExecutions().Update(wfEx); err != nil {
		return err
	}

	ctrl := workflow.GetController(wfEx, wfSpec, tasks)

	var cmds []commands.WorkflowCommand
	if skip {
		cmds = ctrl.SkipTasks([]*db.TaskExecution{taskEx})
	} else {
		cmds = ctrl.RerunTasks([]*db.TaskExecution{taskEx}, reset)
	}

	return e.dispatcher.DispatchWorkflowCommands(wfEx, cmds)
}

// StartTask runs a task execution.
func (e *DefaultEngine) StartTask(taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error {
	taskEx, err := e.store.TaskExecutions().Get(taskExID)
	if err != nil {
		return err
	}

	if waiting {
		taskEx.State = states.WAITING
		return e.store.TaskExecutions().Update(taskEx)
	}

	// Load spec.
	wfDef, err := e.store.WorkflowDefinitions().Get(taskEx.WorkflowID)
	if err != nil {
		return err
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)

	taskSpec := wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return fmt.Errorf("task spec not found: %s", taskEx.Name)
	}

	// Load workflow execution.
	wfEx, err := e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
	if err != nil {
		return err
	}

	// Set state to RUNNING.
	if firstRun {
		taskEx.State = states.RUNNING
	} else {
		if reset {
			taskEx.State = states.RUNNING
			taskEx.Processed = false
			taskEx.Output = nil
			taskEx.Published = make(map[string]interface{})
		}
		if rerun {
			taskEx.State = states.RUNNING
		}
	}

	// Evaluate action input.
	inputDict := evaluateTaskInput(taskSpec, taskEx.InContext, wfEx.Input, wfEx.Context)
	taskEx.RuntimeContext["resolved_input"] = inputDict

	if err := e.store.TaskExecutions().Update(taskEx); err != nil {
		return err
	}

	log.Printf("task %s (id=%s) started with input: %v", taskEx.Name, taskEx.ID, inputDict)

	return nil
}

// OnActionComplete handles action completion events.
func (e *DefaultEngine) OnActionComplete(actionExID string, result *db.Result, wfAction bool) error {
	var actionEx *db.ActionExecution
	var wfEx *db.WorkflowExecution
	var taskEx *db.TaskExecution
	var err error

	if wfAction {
		wfEx, err = e.store.WorkflowExecutions().Get(actionExID)
		if err != nil {
			return err
		}
		if wfEx.TaskExecutionID != "" {
			taskEx, err = e.store.TaskExecutions().Get(wfEx.TaskExecutionID)
			if err != nil {
				return err
			}
		}
	} else {
		// First try to find as ActionExecution.
		actionEx, err = e.store.ActionExecutions().Get(actionExID)
		if err == nil && actionEx != nil {
			// It's a real action execution.
			if actionEx.TaskExecutionID != "" {
				taskEx, err = e.store.TaskExecutions().Get(actionEx.TaskExecutionID)
				if err != nil {
					return err
				}
				wfEx, err = e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
				if err != nil {
					return err
				}
			}
		} else {
			// Not an ActionExecution — maybe it's a TaskExecution ID directly
			// (simplified flow, skipping ActionExecution creation).
			taskEx, err = e.store.TaskExecutions().Get(actionExID)
			if err != nil {
				return err
			}
			wfEx, err = e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
			if err != nil {
				return err
			}
		}
	}

	if taskEx == nil || wfEx == nil {
		return nil
	}

	// Determine new state.
	newState := states.SUCCESS
	var stateInfo string
	if result != nil {
		if result.IsCancel() {
			newState = states.CANCELLED
			stateInfo = fmt.Sprintf("%v", result.Error)
		} else if result.Error != nil {
			newState = states.ERROR
			stateInfo = fmt.Sprintf("%v", result.Error)
		}
	}

	// Update action output.
	if actionEx != nil {
		actionEx.State = newState
		if result != nil {
			actionEx.Output = result.ToDict()
		}
		actionEx.Accepted = true
		if err := e.store.ActionExecutions().Update(actionEx); err != nil {
			return err
		}
	}

	// Complete the task.
	taskEx.State = newState
	taskEx.StateInfo = stateInfo
	taskEx.Processed = true

	if result != nil && result.Data != nil {
		taskEx.Output = map[string]interface{}{"result": result.Data}
	}

	// Publish variables.
	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef != nil {
		wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
		taskSpec := wfSpec.GetTask(taskEx.Name)
		if taskSpec != nil {
			publish := taskSpec.GetPublish()
			if len(publish) > 0 {
				allTasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
				ctx := workflow.BuildFullContext(wfEx, wfSpec, allTasks)
				evaluated := expressions.EvaluateRecursively(publish, ctx)
				if m, ok := evaluated.(map[string]interface{}); ok {
					taskEx.Published = m
				}
			}
		}
	}

	if err := e.store.TaskExecutions().Update(taskEx); err != nil {
		return err
	}

	// Continue workflow.
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	if wfDef != nil {
		wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
		ctrl := workflow.GetController(wfEx, wfSpec, tasks)
		cmds := ctrl.ContinueWorkflow(taskEx)
		if err := e.dispatcher.DispatchWorkflowCommands(wfEx, cmds); err != nil {
			return err
		}

		// Check if workflow should complete.
		if ctrl.MayCompleteWorkflow(taskEx) {
			if err := e.checkAndCompleteWorkflow(wfEx, ctrl, wfSpec); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkAndCompleteWorkflow checks if all tasks are done and the workflow should complete.
func (e *DefaultEngine) checkAndCompleteWorkflow(wfEx *db.WorkflowExecution, ctrl workflow.WorkflowController, wfSpec *v2.WorkflowSpec) error {
	if states.IsPausedOrCompleted(wfEx.State) {
		return nil
	}

	// Count incomplete tasks.
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
	incomplete := 0
	for _, te := range tasks {
		if !states.IsCompleted(te.State) {
			incomplete++
		}
	}
	if incomplete > 0 {
		return nil
	}

	// All tasks completed.
	finalCtx, _ := ctrl.EvaluateWorkflowFinalContext()

	wfEx.State = states.SUCCESS
	wfEx.Output = evaluateOutput(wfSpec.GetOutput(), finalCtx)

	if ctrl.AnyCancels() {
		wfEx.State = states.CANCELLED
	} else if !ctrl.AllErrorsHandled() {
		wfEx.State = states.ERROR
		wfEx.Output = evaluateOutput(wfSpec.GetOutputOnError(), finalCtx)
		if wfEx.Output == nil {
			wfEx.Output = make(map[string]interface{})
		}
		wfEx.Output["result"] = "One or more tasks failed with unhandled errors"
	}

	wfEx.Accepted = true
	return e.store.WorkflowExecutions().Update(wfEx)
}

// evaluateTaskInput evaluates a task's input dictionary against context.
func evaluateTaskInput(taskSpec *v2.TaskSpec, inContext map[string]interface{}, wfInput, wfContext map[string]interface{}) map[string]interface{} {
	inputSpec := taskSpec.GetInput()
	if len(inputSpec) == 0 {
		return make(map[string]interface{})
	}

	ctx := map[string]interface{}{
		"in_context": inContext,
		"input":      wfInput,
		"env":        wfContext["__env"],
		"context":    wfContext,
	}

	evaluated := expressions.EvaluateRecursively(inputSpec, ctx)
	if m, ok := evaluated.(map[string]interface{}); ok {
		return m
	}
	return nil
}

// buildPublishContext builds the context for publish clause evaluation.
func buildPublishContext(wfEx *db.WorkflowExecution, taskEx *db.TaskExecution) map[string]interface{} {
	// Load all task executions for this workflow.
	tasks, _ := db.NewInMemoryStore().TaskExecutions().List(wfEx.ID, "")
	ctx := workflow.BuildFullContext(wfEx, nil, tasks)
	_ = taskEx // taskEx-specific context additions handled by BuildFullContext.
	return ctx
}

// evaluateOutput evaluates output expressions against context.
func evaluateOutput(output map[string]interface{}, ctx map[string]interface{}) map[string]interface{} {
	if len(output) == 0 {
		return nil
	}
	result := expressions.EvaluateRecursively(output, ctx)
	if m, ok := result.(map[string]interface{}); ok {
		return m
	}
	return nil
}
