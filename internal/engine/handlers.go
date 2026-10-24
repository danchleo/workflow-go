package engine

import (
	"context"
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
	eng := &DefaultEngine{
		store: store,
	}
	eng.dispatcher = NewDispatcher(store, eng)
	return eng
}

// Store returns the underlying data store.
func (e *DefaultEngine) Store() db.Store { return e.store }

// Dispatcher returns the command dispatcher.
func (e *DefaultEngine) Dispatcher() *Dispatcher { return e.dispatcher }

// StartWorkflow creates a new workflow execution and starts processing.
func (e *DefaultEngine) StartWorkflow(ctx context.Context, wfDefID, wfNamespace, wfExID string, wfInput map[string]interface{}, description string, params map[string]interface{}) (*db.WorkflowExecution, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var wfDefs = e.store.WorkflowDefinitions()
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

	// Python Workflow._create_execution: wf_def.workbook_name → runtime_context['wb_name'].
	// This lets sub-workflow resolution stay scoped within the same workbook.
	if wfDef.WorkbookName != "" {
		wfEx.RuntimeContext["wb_name"] = wfDef.WorkbookName
	}
	if v, ok := params["wb_name"].(string); ok && v != "" {
		wfEx.RuntimeContext["wb_name"] = v // explicit param overrides wfDef's
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
func (e *DefaultEngine) PauseWorkflow(ctx context.Context, wfExID string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
func (e *DefaultEngine) ResumeWorkflow(ctx context.Context, wfExID string, env map[string]interface{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
func (e *DefaultEngine) StopWorkflow(ctx context.Context, wfExID string, state string, message string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
func (e *DefaultEngine) RerunWorkflow(ctx context.Context, taskExID string, reset bool, skip bool, env map[string]interface{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
func (e *DefaultEngine) StartTask(ctx context.Context, taskExID string, firstRun bool, waiting bool, triggeredBy []map[string]interface{}, rerun bool, reset bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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

	// Check for with-items FIRST (parallel iterations). This is the container that
	// can hold either action or sub-workflow — Python WithItemsTask inherits RegularTask,
	// so _build_action() per iteration can be WorkflowAction or RegularAction.
	// When both `with-items:` and `workflow:` are present, with-items wins and
	// startWithItemsTask handles the per-iteration dispatch.
	withItems := taskSpec.GetWithItems()
	if len(withItems) > 0 {
		return e.startWithItemsTask(ctx, taskEx, taskSpec, wfEx, withItems, inputDict)
	}

	// Check for sub-workflow (nesting). Mirror of Python RegularTask._build_action
	// → WorkflowAction.schedule(): start a child WorkflowExecution with
	// parent's task_execution_id stashed in params so that when the child
	// completes it can propagate its result back via OnActionComplete(wfAction=true).
	if subWfName := taskSpec.GetWorkflowName(); subWfName != "" {
		// Python engine_utils.resolve_workflow_definition:
		//   parent_wf_ex.runtime_context['wb_name'] → first try wb_name.subWfName
		//   then fall back to bare subWfName
		parentWbName, _ := wfEx.RuntimeContext["wb_name"].(string)
		var wfDef *db.WorkflowDefinition
		var err error
		if parentWbName != "" {
			wfDef, err = e.store.WorkflowDefinitions().GetByName(parentWbName+"."+subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
		}
		if wfDef == nil || err != nil {
			wfDef, err = e.store.WorkflowDefinitions().GetByName(subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
			if err != nil {
				return fmt.Errorf("sub-workflow %q not found: %w", subWfName, err)
			}
		}

		childInput := map[string]interface{}{}
		if len(inputDict) > 0 {
			childInput = inputDict
		} else {
			childInput = wfEx.Input
		}

		rootExID := wfEx.RootExecutionID
		if rootExID == "" {
			rootExID = wfEx.ID
		}

		childParams := map[string]interface{}{
			"env":               wfEx.Params["env"],
			"namespace":         wfEx.WorkflowNamespace,
			"task_execution_id": taskEx.ID,
			"root_execution_id": rootExID,
			"index":             0,
		}

		childEx, err := e.StartWorkflow(ctx, wfDef.ID, wfEx.WorkflowNamespace, "", childInput,
			fmt.Sprintf("nested from %s/%s", wfEx.ID, taskEx.Name), childParams)
		if err != nil {
			return fmt.Errorf("start sub-workflow %q: %w", subWfName, err)
		}
		log.Printf("  sub-workflow %s started: childEx=%s (parent task=%s)", subWfName, childEx.ID, taskEx.ID)

		// Unlike RegularAction which creates an ActionExecution, a WorkflowAction
		// leaves no ActionExecution record — the child WorkflowExecution *is*
		// the "action" completion that will be fed back via wfAction=true.
		return e.store.TaskExecutions().Update(taskEx)
	}

	// Create ActionExecution so that OnActionComplete flows correctly through
	// ActionExecution → TaskExecution → WorkflowExecution chain (port of Python
	// mistral/engine/actions.py:RegularAction.schedule).
	sync := true
	actionEx := &db.ActionExecution{
		Name:              taskSpec.GetActionName(),
		State:             states.RUNNING,
		Input:             inputDict,
		RuntimeContext:    map[string]interface{}{"index": 0, "safe_rerun": true},
		WorkflowNamespace: wfEx.WorkflowNamespace,
		TaskExecutionID:   taskEx.ID,
		WorkflowName:      wfEx.WorkflowName,
		WorkflowID:        wfEx.WorkflowID,
		ProjectID:         wfEx.ProjectID,
		IsSync:            &sync,
	}
	if err := e.store.ActionExecutions().Create(actionEx); err != nil {
		return fmt.Errorf("creating action execution: %w", err)
	}
	// Stash actionEx ID in runtime context for demo/tests convenience.
	taskEx.RuntimeContext["action_ex_id"] = actionEx.ID

	if err := e.store.TaskExecutions().Update(taskEx); err != nil {
		return err
	}

	log.Printf("task %s (id=%s) started with input: %v (action_ex=%s)", taskEx.Name, taskEx.ID, inputDict, actionEx.ID)

	return nil
}

// startWithItemsTask launches parallel iterations — either ActionExecutions or
// nested WorkflowExecutions, depending on whether the task has `action:` or `workflow:`.
// Port of Python WithItemsTask → N×_build_action() (RegularAction or WorkflowAction).
func (e *DefaultEngine) startWithItemsTask(ctx context.Context, taskEx *db.TaskExecution, taskSpec *v2.TaskSpec, wfEx *db.WorkflowExecution, withItems map[string]interface{}, baseInput map[string]interface{}) error {
	// Evaluate each with-items expression to get the actual item lists.
	ctxMap := workflow.BuildFullContext(wfEx, nil, nil)
	withValues := make(map[string][]interface{})
	var minLen int = -1
	for varName, rawExpr := range withItems {
		var val interface{}
		switch v := rawExpr.(type) {
		case string:
			// Expression form: "var in <% $.items %>" → evaluate against context.
			val = expressions.EvaluateRecursively(v, ctxMap)
		case []interface{}:
			// Literal list form (Mistral YAML map syntax): with-items: {item: [a, b]}.
			val = v
		default:
			return fmt.Errorf("with-items value for %q must be string expression or literal list, got %T", varName, rawExpr)
		}
		var items []interface{}
		switch v := val.(type) {
		case []interface{}:
			items = v
		case string:
			items = []interface{}{v}
		default:
			items = []interface{}{v}
		}
		withValues[varName] = items
		if minLen < 0 || len(items) < minLen {
			minLen = len(items)
		}
	}

	if minLen <= 0 {
		taskEx.State = states.SUCCESS
		taskEx.Processed = true
		return e.store.TaskExecutions().Update(taskEx)
	}

	taskEx.RuntimeContext["with_items_total"] = minLen
	taskEx.RuntimeContext["with_items_done_count"] = 0
	taskEx.RuntimeContext["with_items_values"] = withValues

	// Resolve once: is this with-items over actions or over sub-workflows?
	subWfName := taskSpec.GetWorkflowName()
	var subWfDef *db.WorkflowDefinition
	if subWfName != "" {
		// Scope-aware resolution (wb_name first, then global fallback).
		parentWbName, _ := wfEx.RuntimeContext["wb_name"].(string)
		var err error
		if parentWbName != "" {
			subWfDef, err = e.store.WorkflowDefinitions().GetByName(parentWbName+"."+subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
		}
		if subWfDef == nil || err != nil {
			subWfDef, err = e.store.WorkflowDefinitions().GetByName(subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
			if err != nil {
				return fmt.Errorf("sub-workflow %q not found: %w", subWfName, err)
			}
		}
	}

	for i := 0; i < minLen; i++ {
		// Build per-iteration input by injecting with-item variables.
		iterInput := make(map[string]interface{})
		for k, v := range baseInput {
			iterInput[k] = v
		}
		for varName, items := range withValues {
			if i < len(items) {
				iterInput[varName] = items[i]
			}
		}

		if subWfDef != nil {
			// Each iteration is a child WorkflowExecution — Python WorkflowAction per iteration.
			rootExID := wfEx.RootExecutionID
			if rootExID == "" {
				rootExID = wfEx.ID
			}
			iterParams := map[string]interface{}{
				"env":               wfEx.Params["env"],
				"namespace":         wfEx.WorkflowNamespace,
				"task_execution_id": taskEx.ID,
				"root_execution_id": rootExID,
				"index":             i,
			}
			childEx, err := e.StartWorkflow(ctx, subWfDef.ID, wfEx.WorkflowNamespace, "", iterInput,
				fmt.Sprintf("with-items iter %d/%d of %s", i+1, minLen, taskEx.Name), iterParams)
			if err != nil {
				return fmt.Errorf("with-items iter %d sub-workflow: %w", i, err)
			}
			log.Printf("with-items iter %d/%d sub-workflow %s started: childEx=%s", i+1, minLen, subWfName, childEx.ID)
		} else {
			// Standard ActionExecution iteration.
			sync := true
			actionEx := &db.ActionExecution{
				Name:              taskSpec.GetActionName(),
				State:             states.RUNNING,
				Input:             iterInput,
				RuntimeContext:    map[string]interface{}{"index": i, "safe_rerun": true},
				WorkflowNamespace: wfEx.WorkflowNamespace,
				TaskExecutionID:   taskEx.ID,
				WorkflowName:      wfEx.WorkflowName,
				WorkflowID:        wfEx.WorkflowID,
				ProjectID:         wfEx.ProjectID,
				IsSync:            &sync,
			}
			if err := e.store.ActionExecutions().Create(actionEx); err != nil {
				return fmt.Errorf("creating with-items action execution %d: %w", i, err)
			}
			log.Printf("with-items iter %d/%d action created for task %s (action_ex=%s)", i+1, minLen, taskEx.Name, actionEx.ID)
		}
	}

	if err := e.store.TaskExecutions().Update(taskEx); err != nil {
		return err
	}
	log.Printf("with-items task %s started (%d parallel iterations)", taskEx.Name, minLen)
	return nil
}

// OnActionComplete handles action completion events.
func (e *DefaultEngine) OnActionComplete(ctx context.Context, actionExID string, result *db.Result, wfAction bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	var actionEx *db.ActionExecution
	var wfEx *db.WorkflowExecution
	var childWfEx *db.WorkflowExecution // only set when wfAction=true — the nested WorkflowExecution that just completed
	var taskEx *db.TaskExecution
	var err error

	if wfAction {
		// actionExID here IS the child WorkflowExecution's ID (Python: wf_action=True).
		childWfEx, err = e.store.WorkflowExecutions().Get(actionExID)
		if err != nil {
			return err
		}
		if childWfEx.TaskExecutionID == "" {
			return fmt.Errorf("wfAction=True but child wfEx %s has no TaskExecutionID", childWfEx.ID)
		}
		taskEx, err = e.store.TaskExecutions().Get(childWfEx.TaskExecutionID)
		if err != nil {
			return err
		}
		// wfEx must be the PARENT workflow execution — the one that owns taskEx —
		// because the controller and dispatcher run against it, not the child.
		wfEx, err = e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
		if err != nil {
			return err
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
	var newState string
	var stateInfo string
	if wfAction && childWfEx != nil {
		// Sub-workflow completion: use the child WorkflowExecution's terminal state
		// directly, mirroring Python RegularTask.on_action_complete which reads
		// action_ex.state (where action_ex can be a WorkflowExecution).
		newState = childWfEx.State
		if info, ok := childWfEx.StateInfo.(string); ok {
			stateInfo = info
		}
	} else {
		newState = states.SUCCESS
		if result != nil {
			if result.IsCancel() {
				newState = states.CANCELLED
				stateInfo = fmt.Sprintf("%v", result.Error)
			} else if result.Error != nil {
				newState = states.ERROR
				stateInfo = fmt.Sprintf("%v", result.Error)
			}
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

	// Handle with-items: defer task completion until all iterations finish.
	// Mirror Python WithItemsTask._complete_actions (engine/tasks.py ~line 870):
	// if any iter ERROR → task ERROR, else SUCCESS. We count failed iters
	// alongside done_count so the final state is determined by aggregation.
	if _, hasSize := taskEx.RuntimeContext["with_items_total"]; hasSize {
		total := db.CoerceInt(taskEx.RuntimeContext["with_items_total"])
		doneCount := db.CoerceInt(taskEx.RuntimeContext["with_items_done_count"])
		failedCount := db.CoerceInt(taskEx.RuntimeContext["with_items_failed_count"])
		doneCount++
		if states.IsFailed(newState) {
			failedCount++
		}
		taskEx.RuntimeContext["with_items_done_count"] = doneCount
		taskEx.RuntimeContext["with_items_failed_count"] = failedCount
		if doneCount < total {
			log.Printf("with-items task %s: %d/%d iterations done (%d failed)", taskEx.Name, doneCount, total, failedCount)
			return e.store.TaskExecutions().Update(taskEx)
		}
		// Aggregate final state — Python WithItemsTask rule.
		if failedCount > 0 {
			newState = states.ERROR
			stateInfo = fmt.Sprintf("%d of %d with-items iterations failed", failedCount, total)
		}
		log.Printf("with-items task %s: all %d iterations done (%d failed), final state=%s",
			taskEx.Name, total, failedCount, newState)
	}

	// Complete the task.
	taskEx.State = newState
	taskEx.StateInfo = stateInfo
	taskEx.Processed = true

	if wfAction && childWfEx != nil {
		// Sub-workflow completion: the child WorkflowExecution's output IS the
		// "action output" that the parent task should see. Mirror Python where
		// action_ex.output (of the WorkflowExecution) propagates as task result.
		taskEx.Output = childWfEx.Output
		if taskEx.Output == nil {
			taskEx.Output = make(map[string]interface{})
		}
		// Also ensure a "result" key exists for easy dot-path expressions.
		if _, ok := taskEx.Output["result"]; !ok {
			taskEx.Output["result"] = childWfEx.State
		}
	} else if result != nil && result.Data != nil {
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
				// Build context using in-memory current taskEx (Output already
				// set above) instead of the stale store version.
				allTasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
				for i, te := range allTasks {
					if te.ID == taskEx.ID {
						allTasks[i] = taskEx
						break
					}
				}
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
	if err := e.store.WorkflowExecutions().Update(wfEx); err != nil {
		return err
	}

	// If this workflow is a sub-workflow (has a parent task), send the result
	// back so the parent task can complete. Mirror of Python Workflow._send_result_to_parent_workflow.
	if wfEx.TaskExecutionID != "" {
		var result *db.Result
		switch wfEx.State {
		case states.SUCCESS:
			result = nil // Python: result=None
		case states.ERROR:
			errMsg := ""
			if v, ok := wfEx.StateInfo.(string); ok {
				errMsg = v
			}
			if errMsg == "" {
				errMsg = fmt.Sprintf("Failed subworkflow [execution_id=%s]", wfEx.ID)
			}
			result = &db.Result{Error: errMsg}
		case states.CANCELLED:
			errMsg := ""
			if v, ok := wfEx.StateInfo.(string); ok {
				errMsg = v
			}
			if errMsg == "" {
				errMsg = fmt.Sprintf("Cancelled subworkflow [execution_id=%s]", wfEx.ID)
			}
			result = &db.Result{Error: errMsg, Cancel: true}
		}
		if err := e.OnActionComplete(context.Background(), wfEx.ID, result, true); err != nil {
			log.Printf("  sub-workflow result callback to parent failed: %v", err)
		} else {
			log.Printf("  sub-workflow %s (%s) result sent to parent task %s", wfEx.Name, wfEx.State, wfEx.TaskExecutionID)
		}
	}

	return nil
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
