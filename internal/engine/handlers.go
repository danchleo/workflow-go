package engine

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/actions"
	"github.com/mistral/workflow/internal/commands"
	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/executors"
	"github.com/mistral/workflow/internal/expressions"
	"github.com/mistral/workflow/internal/lang"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"github.com/mistral/workflow/internal/scheduler"
	"github.com/mistral/workflow/internal/states"
	"github.com/mistral/workflow/internal/workflow"
)

// DefaultEngine is the main engine implementation.
// Ported from mistral-master/mistral/engine/default_engine.py
type DefaultEngine struct {
	store      db.Store
	dispatcher *Dispatcher
	sched      scheduler.Scheduler // optional delayed-job scheduler (nil if none)
}

// NewDefaultEngine creates a DefaultEngine with the given store.
func NewDefaultEngine(store db.Store) *DefaultEngine {
	eng := &DefaultEngine{
		store: store,
	}
	eng.dispatcher = NewDispatcher(store, eng)
	return eng
}

// WithScheduler wires a generic delayed-job scheduler into the engine.
// Policies like retry-delay, wait-before, wait-after and timeout need it.
// This also registers engine handlers in the scheduler's JobHandler registry
// (idempotent — registers once).
func (e *DefaultEngine) WithScheduler(s scheduler.Scheduler) *DefaultEngine {
	e.sched = s
	registerEngineJobHandlers(e)
	return e
}

var engineHandlersOnce sync.Once

// registerEngineJobHandler registers engine-specific handlers for JobType
// values. Called once by WithScheduler — idempotent via sync.Once.
func registerEngineJobHandlers(e *DefaultEngine) {
	engineHandlersOnce.Do(func() {
		// fail_if_incomplete: scheduler fires → engine forces ActionExecution ERROR.
		scheduler.RegisterJobHandler(scheduler.JobFailIfIncomplete, func(args map[string]interface{}) {
			actionExID, _ := args["action_ex_id"].(string)
			timeoutSec := db.CoerceInt(args["timeout_sec"])
			e.failIfIncomplete(actionExID, timeoutSec)
		})

		// continue_task: generic deferred re-entry to StartTask.
		//   wait-before  → just re-start the task (sets RUNNING_DELAYED → RUNNING).
		//   retry delay  → re-set retry_attempts on taskEx BEFORE re-entering StartTask
		//                  so the new ActionExecution carries the right attempt count.
		scheduler.RegisterJobHandler(scheduler.JobContinueTask, func(args map[string]interface{}) {
			taskExID, _ := args["task_ex_id"].(string)
			if taskExID == "" {
				return
			}

			if stashed, ok := args["stashed_retry_attempts"]; ok {
				attempt := db.CoerceInt(stashed)
				retryLimit := db.CoerceInt(args["retry_limit"])
				te, _ := e.store.TaskExecutions().Get(taskExID)
				if te != nil {
					wfDef, _ := e.store.WorkflowDefinitions().Get(te.WorkflowID)
					if wfDef != nil {
						wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
						taskSpec := wfSpec.GetTask(te.Name)
						if taskSpec != nil {
							input, _ := te.RuntimeContext["retry_action_input"].(map[string]interface{})
							actionName, _ := te.RuntimeContext["retry_action_name"].(string)
							idx := db.CoerceInt(te.RuntimeContext["retry_action_index"])
							sync2 := true
							newActionEx := &db.ActionExecution{
								Name:              actionName,
								State:             states.RUNNING,
								Input:             input,
								RuntimeContext:    map[string]interface{}{"index": idx, "safe_rerun": true, "retry_attempts": attempt},
								WorkflowNamespace: te.WorkflowNamespace,
								TaskExecutionID:   te.ID,
								WorkflowName:      te.WorkflowName,
								WorkflowID:        te.WorkflowID,
								ProjectID:         te.ProjectID,
								IsSync:            &sync2,
							}
							if err := e.store.ActionExecutions().Create(newActionEx); err == nil {
								e.scheduleTaskTimeout(newActionEx, te, taskSpec)
								log.Printf("scheduler: retry [delayed] attempt %d/%d fired → action_ex=%s",
									attempt, retryLimit, newActionEx.ID)
							}
						}
					}
					te.State = states.RUNNING
					te.StateInfo = ""
					_ = e.store.TaskExecutions().Update(te)
				}
				return
			}

			if err := e.StartTask(context.Background(), taskExID, false, false, nil, false, false); err != nil {
				log.Printf("scheduler: JobContinueTask StartTask(%s) failed: %v", taskExID, err)
			}
		})

		// complete_task: wait-after deferred final state change + advanceWorkflow.
		scheduler.RegisterJobHandler("complete_task", func(args map[string]interface{}) {
			wfExID, _ := args["wf_ex_id"].(string)
			taskExID, _ := args["task_ex_id"].(string)
			endState, _ := args["end_state"].(string)
			endInfo, _ := args["end_info"].(string)

			if wfExID == "" || taskExID == "" {
				return
			}
			te, _ := e.store.TaskExecutions().Get(taskExID)
			if te == nil {
				return
			}
			te.State = endState
			te.StateInfo = endInfo
			te.Processed = true
			if err := e.store.TaskExecutions().Update(te); err != nil {
				log.Printf("scheduler: JobCompleteTask finalize failed: %v", err)
				return
			}
			e.advanceWorkflow(wfExID, taskExID)
		})

		// refresh_task_state: join-task wakeup. Scheduled after any task completes,
		// this handler re-evaluates a waiting join task's logical state and either
		// (a) re-enters StartTask if RUNNING (calling continue_task / _refresh_task_state),
		// (b) marks ERROR if dependencies are permanently broken, or
		// (c) no-ops if still WAITING.
		// Mirrors Python task_handler._refresh_task_state + _schedule_refresh_task_state.
		scheduler.RegisterJobHandler(scheduler.JobRefreshTaskState, func(args map[string]interface{}) {
			taskExID, _ := args["task_ex_id"].(string)
			if taskExID == "" {
				return
			}
			e.refreshTaskState(taskExID)
		})

		scheduler.RegisterJobHandler(scheduler.JobIntegrityCheck, func(args map[string]interface{}) {
			wfExID, _ := args["wf_ex_id"].(string)
			if wfExID == "" {
				return
			}
			e.checkIntegrity(context.Background(), wfExID)
		})
	})
}

// scheduleTaskTimeout checks the task's timeout policy and, if set, fires
// a delayed callback via the shared scheduler (or time.AfterFunc as fallback)
// that will force the ActionExecution to ERROR after timeoutSec seconds if
// it's still not completed. Mirrors Python TimeoutPolicy.before_task_start
// (mistral/engine/policies.py ~line 412) which schedules _fail_if_incomplete
// via the system scheduler.
func (e *DefaultEngine) scheduleTaskTimeout(actionEx *db.ActionExecution, taskEx *db.TaskExecution, taskSpec *v2.TaskSpec) {
	if taskSpec == nil {
		return
	}
	policies := taskSpec.GetPolicies()
	if policies == nil {
		return
	}
	timeoutRaw := policies.GetTimeout()
	if timeoutRaw == nil {
		return
	}
	timeoutSec := -1
	switch t := timeoutRaw.(type) {
	case float64:
		timeoutSec = int(t)
	case int:
		timeoutSec = t
	}
	if timeoutSec <= 0 {
		return
	}
	delay := time.Duration(timeoutSec) * time.Second
	if e.sched != nil {
		args := map[string]interface{}{
			"action_ex_id": actionEx.ID,
			"timeout_sec":  timeoutSec,
		}
		if _, err := e.sched.Schedule(scheduler.JobFailIfIncomplete, args, delay); err != nil {
			log.Printf("timeout: scheduler.Schedule failed: %v — falling back to time.AfterFunc", err)
			time.AfterFunc(delay, func() { e.failIfIncomplete(actionEx.ID, timeoutSec) })
		}
	} else {
		time.AfterFunc(delay, func() {
			e.failIfIncomplete(actionEx.ID, timeoutSec)
		})
	}
}

// failIfIncomplete is the Go equivalent of Python's
// policies._fail_task_if_incomplete (mistral/engine/policies.py ~line 545).
// If the action is still not completed after the timeout, call
// OnActionComplete with an error Result so retry/with-items/on-error
// policies run correctly through the normal pipeline.
func (e *DefaultEngine) failIfIncomplete(actionExID string, timeoutSec int) {
	actionEx, err := e.store.ActionExecutions().Get(actionExID)
	if err != nil || actionEx == nil {
		return
	}
	if states.IsCompleted(actionEx.State) {
		return // already finished, nothing to do
	}

	msg := fmt.Sprintf("Task timed out [timeout(s)=%d].", timeoutSec)
	log.Printf("timeout: actionEx %s timed out after %ds, forcing ERROR", actionExID, timeoutSec)

	// Feed into the normal completion pipeline with an error Result.
	// OnActionComplete will derive state=ERROR from result.Error != nil.
	result := &db.Result{Error: msg}
	if err := e.OnActionComplete(context.Background(), actionExID, result, false); err != nil {
		log.Printf("timeout: OnActionComplete for %s failed: %v", actionExID, err)
	}
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

	// Crash-recovery sweep: schedule an integrity check that fires ~120s
	// from now (and re-schedules itself) to catch tasks that may be stuck
	// in RUNNING state after an engine crash. Mirrors Python
	// _schedule_check_and_fix_integrity called from workflow_handler.py
	// whenever a workflow transitions to RUNNING.
	e.scheduleIntegrityCheck(wfEx.ID)

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

	// Re-run all IDLE tasks (set by pause-before policy).
	// Python resume_workflow iterates over IDLE tasks and calls continue_task.
	for _, t := range tasks {
		if t.State == states.IDLE {
			// Trigger a refresh; scheduleRefreshTaskState will re-check logical state
			// and call continueTask → StartTask if ready.
			e.scheduleRefreshTaskState(t.ID)
		}
	}

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

	if err := e.dispatcher.DispatchWorkflowCommands(wfEx, cmds); err != nil {
		return err
	}

	// For SKIP: the task never goes through OnActionComplete, so we need
	// to explicitly drive ContinueWorkflow to pick up on-skip transitions
	// and (eventually) workflow completion.
	if skip {
		// Skip updated the task state → reload for controller's fresh view.
		tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")
		ctrl = workflow.GetController(wfEx, wfSpec, tasks)
		// Find the just-skipped task (it's still in tasks list, now SKIPPED).
		var skipTaskEx *db.TaskExecution
		for _, te := range tasks {
			if te.ID == taskEx.ID {
				skipTaskEx = te
				break
			}
		}
		next := ctrl.ContinueWorkflow(skipTaskEx)
		if len(next) > 0 {
			if err := e.dispatcher.DispatchWorkflowCommands(wfEx, next); err != nil {
				return err
			}
		}

		// Mirror OnActionComplete's tail: check workflow completion + wake joins.
		wfEx, _ = e.store.WorkflowExecutions().Get(wfEx.ID)
		if ctrl.MayCompleteWorkflow(skipTaskEx) {
			if err := e.checkAndCompleteWorkflow(wfEx, ctrl, wfSpec); err != nil {
				return err
			}
		}
		e.checkAffectedTasks(skipTaskEx, wfEx)
	}

	return nil
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

	// ================================================================
	// PauseBeforePolicy — mirror Python policies.py PauseBeforePolicy.
	// If task has pause-before (truthy boolean), set workflow to PAUSED
	// and this task to IDLE. Mirrors task._before_task_start().
	// ================================================================
	if taskEx.RuntimeContext["pause_before_done"] == nil {
		policies := taskSpec.GetPolicies()
		if policies != nil {
			if pbRaw := policies.GetPauseBefore(); pbRaw != nil {
				if isTruthy(pbRaw) {
					taskEx.State = states.IDLE
					taskEx.StateInfo = "Set by 'pause-before' policy"
					taskEx.RuntimeContext["pause_before_done"] = true
					_ = e.store.TaskExecutions().Update(taskEx)

					wfEx.State = states.PAUSED
					wfEx.StateInfo = fmt.Sprintf("Paused by 'pause-before' policy at task '%s'", taskEx.Name)
					_ = e.store.WorkflowExecutions().Update(wfEx)

					log.Printf("pause-before: workflow %s paused before task %s → PAUSED", wfEx.ID, taskEx.Name)
					return nil
				}
				taskEx.RuntimeContext["pause_before_done"] = true
			}
		}
	}

	// ================================================================
	// WaitBeforePolicy — mirror Python policies.py WaitBeforePolicy.
	// If task has wait_before: N, set state→RUNNING_DELAYED and defer
	// StartTask completion via the shared scheduler. Mirrors
	// task._before_task_start() → WaitBeforePolicy.before_task_start.
	// ================================================================
	if e.sched != nil && taskEx.RuntimeContext["wait_before_done"] == nil {
		policies := taskSpec.GetPolicies()
		if policies != nil {
			if wbRaw := policies.GetWaitBefore(); wbRaw != nil {
				wbSec := 0
				switch v := wbRaw.(type) {
				case float64:
					wbSec = int(v)
				case int:
					wbSec = v
				}
				if wbSec > 0 {
					delay := time.Duration(wbSec) * time.Second
					taskEx.State = states.RUNNING_DELAYED
					taskEx.StateInfo = fmt.Sprintf("Delayed by 'wait-before' policy [delay=%ds]", wbSec)
					taskEx.RuntimeContext["wait_before_done"] = true
					if err := e.store.TaskExecutions().Update(taskEx); err != nil {
						log.Printf("wait-before: update taskEx to RUNNING_DELAYED failed: %v", err)
					}
					e.sched.Schedule(scheduler.JobContinueTask, map[string]interface{}{
						"task_ex_id": taskExID,
					}, delay)
					log.Printf("wait-before: task %s delayed %ds → RUNNING_DELAYED", taskEx.Name, wbSec)
					return nil
				}
			}
		}
	}

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

	// Timeout policy — schedule forced failure if action doesn't complete in time.
	e.scheduleTaskTimeout(actionEx, taskEx, taskSpec)

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
	taskEx.RuntimeContext["with_items_failed_count"] = 0
	taskEx.RuntimeContext["with_items_values"] = withValues

	// Concurrency limit (Python WithItemsTask._get_with_items_capacity).
	// 0 = unlimited; otherwise cap how many iterations start at once.
	concurrency := taskSpec.GetConcurrency()
	if concurrency <= 0 || concurrency > minLen {
		concurrency = minLen
	}
	taskEx.RuntimeContext["with_items_concurrency"] = concurrency
	taskEx.RuntimeContext["with_items_started"] = 0

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

	// Phase 1: launch up to `concurrency` initial iterations. The rest will
	// be started lazily in OnActionComplete as completions arrive (Python
	// WithItemsTask _dispatch_actions capacity = concurrency - running_count).
	initialBatch := concurrency
	startedCount := 0
	for i := 0; i < initialBatch; i++ {
		if err := e.startWithItemsIter(ctx, i, minLen, taskEx, taskSpec, wfEx, subWfDef, withValues, baseInput, subWfName); err != nil {
			return err
		}
		startedCount++
	}
	taskEx.RuntimeContext["with_items_started"] = startedCount

	if err := e.store.TaskExecutions().Update(taskEx); err != nil {
		return err
	}
	if concurrency < minLen {
		log.Printf("with-items task %s started (concurrency %d/%d, %d deferred)", taskEx.Name, concurrency, minLen, minLen-concurrency)
	} else {
		log.Printf("with-items task %s started (%d parallel iterations)", taskEx.Name, minLen)
	}
	return nil
}

// startWithItemsIter creates a single with-items iteration (ActionExecution or child WorkflowExecution).
func (e *DefaultEngine) startWithItemsIter(ctx context.Context, i, total int, taskEx *db.TaskExecution, taskSpec *v2.TaskSpec, wfEx *db.WorkflowExecution, subWfDef *db.WorkflowDefinition, withValues map[string][]interface{}, baseInput map[string]interface{}, subWfName string) error {
	// Build per-iteration context — Python WithItemsTask re-evaluates input
	// expressions with the iteration variables bound so that "<% $.vm_info %>"
	// resolves to the current iteration item.
	iterCtx := workflow.BuildFullContext(wfEx, nil, nil)
	for varName, items := range withValues {
		if i < len(items) {
			iterCtx[varName] = items[i]
		}
	}

	// Re-evaluate task input with iteration variables bound.
	inputSpec := taskSpec.GetInput()
	var iterInput map[string]interface{}
	if len(inputSpec) > 0 {
		evaluated := expressions.EvaluateRecursively(inputSpec, iterCtx)
		if m, ok := evaluated.(map[string]interface{}); ok {
			iterInput = m
		}
	}
	if iterInput == nil {
		iterInput = make(map[string]interface{})
		for k, v := range baseInput {
			iterInput[k] = v
		}
	}
	// Always bind iteration variables into the input (for action-type tasks).
	for varName, items := range withValues {
		if i < len(items) {
			iterInput[varName] = items[i]
		}
	}

	if subWfDef != nil {
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
			fmt.Sprintf("with-items iter %d/%d of %s", i+1, total, taskEx.Name), iterParams)
		if err != nil {
			return fmt.Errorf("with-items iter %d sub-workflow: %w", i, err)
		}
		log.Printf("with-items iter %d/%d sub-workflow %s started: childEx=%s", i+1, total, subWfName, childEx.ID)
	} else {
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
		log.Printf("with-items iter %d/%d action created for task %s (action_ex=%s)", i+1, total, taskEx.Name, actionEx.ID)
		// Timeout policy — each with-items iteration gets its own timer.
		e.scheduleTaskTimeout(actionEx, taskEx, taskSpec)
	}
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
		actionEx.StateInfo = stateInfo
		actionEx.Accepted = true
		if err := e.store.ActionExecutions().Update(actionEx); err != nil {
			return err
		}
	}

	// ============================================================
	// Retry Policy — Python engine/policies.py RetryPolicy.after_task_complete.
	// Per-ActionExecution granularity: a failed individual iteration retries
	// independently of other with-items siblings. This runs BEFORE the
	// with-items aggregation gate so retries don't corrupt failed_count.
	//
	// Invoked for both ERROR and SUCCESS — break_on / continue_on inside
	// handleRetry decide whether to actually retry. SUCCESS without
	// continue-on defaults to NO retry (stop_continue_flag).
	// handleRetry itself returns false when no retry policy is configured.
	// ============================================================
	if actionEx != nil {
		retryRetried, retryErr := e.handleRetry(ctx, actionEx, taskEx, wfEx)
		if retryErr != nil {
			return retryErr
		}
		if retryRetried {
			// Retry scheduled — do NOT advance task state or with-items counters.
			return nil
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
		startedCount := db.CoerceInt(taskEx.RuntimeContext["with_items_started"])
		concurrency := db.CoerceInt(taskEx.RuntimeContext["with_items_concurrency"])
		doneCount++
		if states.IsFailed(newState) {
			failedCount++
		}
		taskEx.RuntimeContext["with_items_done_count"] = doneCount
		taskEx.RuntimeContext["with_items_failed_count"] = failedCount

		// Concurrency gate — Python WithItemsTask._dispatch_actions:
		// capacity = concurrency - (started - done). If we have spare capacity
		// and there are undelivered iterations, launch the next batch now.
		if concurrency > 0 && startedCount < total {
			running := startedCount - doneCount
			capacity := concurrency - running
			if capacity > 0 {
				// Load taskSpec + wfSpec to resolve subWfDef and get with_values.
				wfDef2, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
				if wfDef2 != nil {
					wfSpec2, _ := v2.NewWorkflowSpec(wfDef2.Name, wfDef2.Spec)
					if wfSpec2 != nil {
						taskSpec := wfSpec2.GetTask(taskEx.Name)
						if taskSpec != nil {
							subWfName := taskSpec.GetWorkflowName()
							var subWfDef *db.WorkflowDefinition
							if subWfName != "" {
								parentWbName, _ := wfEx.RuntimeContext["wb_name"].(string)
								if parentWbName != "" {
									subWfDef, _ = e.store.WorkflowDefinitions().GetByName(parentWbName+"."+subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
								}
								if subWfDef == nil {
									subWfDef, _ = e.store.WorkflowDefinitions().GetByName(subWfName, wfEx.WorkflowNamespace, wfEx.ProjectID)
								}
							}
							withValues, _ := taskEx.RuntimeContext["with_items_values"].(map[string][]interface{})
							// If values got JSON-round-tripped as []interface{} lists inside an interface{} map,
							// convert them back.
							if withValues == nil {
								if raw, ok := taskEx.RuntimeContext["with_items_values"].(map[string]interface{}); ok {
									withValues = make(map[string][]interface{})
									for k, v := range raw {
										if items, ok := v.([]interface{}); ok {
											withValues[k] = items
										}
									}
								}
							}
							// Load task input from wfSpec.
							inputDict := evaluateTaskInput(taskSpec, workflow.BuildFullContext(wfEx, wfSpec2, nil), wfEx.Input, wfEx.Context)
							for b := 0; b < capacity && startedCount < total; b++ {
								if err := e.startWithItemsIter(ctx, startedCount, total, taskEx, taskSpec, wfEx, subWfDef, withValues, inputDict, subWfName); err != nil {
									return fmt.Errorf("with-items capacity launch iter %d: %w", startedCount, err)
								}
								startedCount++
							}
							taskEx.RuntimeContext["with_items_started"] = startedCount
						}
					}
				}
				log.Printf("with-items task %s capacity refill: launched %d new iters, started=%d/%d", taskEx.Name, min(capacity, total-startedCount), startedCount, total)
			}
		}

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

	// ================================================================
	// WaitAfterPolicy — mirror Python policies.py WaitAfterPolicy.
	// Before completing the task, if wait_after: N is set, defer the
	// final state change + ContinueWorkflow via scheduler. Mirrors
	// task._after_task_complete() → WaitAfterPolicy.after_task_complete.
	// ================================================================
	if e.sched != nil && taskEx.RuntimeContext["wait_after_done"] == nil {
		wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
		if wfDef != nil {
			wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
			wbTaskSpec := wfSpec.GetTask(taskEx.Name)
			if wbTaskSpec != nil {
				policies := wbTaskSpec.GetPolicies()
				if policies != nil {
					if waRaw := policies.GetWaitAfter(); waRaw != nil {
						waSec := 0
						switch v := waRaw.(type) {
						case float64:
							waSec = int(v)
						case int:
							waSec = v
						}
						if waSec > 0 {
							delay := time.Duration(waSec) * time.Second
							taskEx.State = states.RUNNING_DELAYED
							taskEx.StateInfo = fmt.Sprintf("Delayed by 'wait-after' policy [delay=%ds]", waSec)
							taskEx.RuntimeContext["wait_after_done"] = true
							if err := e.store.TaskExecutions().Update(taskEx); err != nil {
								log.Printf("wait-after: update taskEx to RUNNING_DELAYED failed: %v", err)
							}
							e.sched.Schedule("complete_task", map[string]interface{}{
								"wf_ex_id":   wfEx.ID,
								"task_ex_id": taskEx.ID,
								"end_state":  newState,
								"end_info":   stateInfo,
							}, delay)
							log.Printf("wait-after: task %s completion delayed %ds → RUNNING_DELAYED (will finish as %s)",
								taskEx.Name, waSec, newState)
							return nil
						}
					}
				}
			}
		}
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

			// ================================================================
			// FailOnPolicy — mirror Python policies.py FailOnPolicy.
			// If task SUCCESS and fail-on evaluates truthy, override to ERROR.
			// Supports literal boolean AND string expression form
			// (fail-on: <% $.should_fail %>) — Python YAQL handles both.
			// ================================================================
			if taskEx.State == states.SUCCESS {
				if policies := taskSpec.GetPolicies(); policies != nil {
					if foRaw := policies.GetFailOn(); foRaw != nil {
						var foVal interface{}
						if s, ok := foRaw.(string); ok {
							// Expression form — evaluate against full context.
							allTasks2, _ := e.store.TaskExecutions().List(wfEx.ID, "")
							for i, te := range allTasks2 {
								if te.ID == taskEx.ID {
									allTasks2[i] = taskEx
									break
								}
							}
							ctx := workflow.BuildFullContext(wfEx, wfSpec, allTasks2)
							foVal = expressions.EvaluateRecursively(s, ctx)
						} else {
							foVal = foRaw
						}
						if isTruthy(foVal) {
							taskEx.State = states.ERROR
							taskEx.StateInfo = "Failed by 'fail-on' policy"
							log.Printf("fail-on: task %s SUCCESS → ERROR by fail-on policy", taskEx.Name)
						}
					}
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

		// P0: wake up waiting join tasks that were transitively affected.
		// Mirrors Python task_handler._check_affected_tasks called after
		// Task.complete() dispatches workflow commands.
		e.checkAffectedTasks(taskEx, wfEx)
	}

	return nil
}

// handleRetry implements per-ActionExecution retry after a failure.
// Mirrors Python engine/policies.py RetryPolicy.after_task_complete but
// operates on individual ActionExecutions (finer granularity — sibling
// with-items iterations retry independently).
//
// Supports retry.delay via the shared scheduler: if delay > 0 and the
// engine has a scheduler wired in, we set the task to RUNNING_DELAYED
// and defer the new ActionExecution creation + StartTask call by delay.
// Otherwise retry is immediate (delay=0 or no scheduler).
//
// Returns (retried, error): retried=true means a new ActionExecution will
// be created (either now or later via scheduler) — caller must NOT advance
// task state or with-items counters.
func (e *DefaultEngine) handleRetry(ctx context.Context, actionEx *db.ActionExecution, taskEx *db.TaskExecution, wfEx *db.WorkflowExecution) (bool, error) {
	// Load taskSpec to find retry policy.
	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef == nil {
		return false, nil
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	if wfSpec == nil {
		return false, nil
	}
	taskSpec := wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return false, nil
	}
	policies := taskSpec.GetPolicies()
	if policies == nil {
		return false, nil
	}
	retrySpec := policies.GetRetry()
	if retrySpec == nil {
		return false, nil
	}

	retryCount := -1
	switch c := retrySpec.GetCount().(type) {
	case float64:
		retryCount = int(c)
	case int:
		retryCount = c
	}
	if retryCount <= 0 {
		return false, nil
	}

	// Resolve delay.
	retryDelaySeconds := 0
	switch d := retrySpec.GetDelay().(type) {
	case float64:
		retryDelaySeconds = int(d)
	case int:
		retryDelaySeconds = d
	}

	attempt := db.CoerceInt(actionEx.RuntimeContext["retry_attempts"])
	if attempt >= retryCount {
		log.Printf("retry: ActionExecution %s already attempted %d times (limit %d) — giving up",
			actionEx.ID, attempt, retryCount)
		return false, nil
	}

	// ================================================================
	// break_on / continue_on condition evaluation.
	// Mirrors Python RetryPolicy.after_task_complete expression logic.
	//   break_triggered = (state == ERROR) AND break_on evaluates truthy
	//   stop_continue   = (state == SUCCESS) AND (continue_on NOT set OR evaluates falsy)
	// Python evaluates expressions against task outbound context (data_flow).
	// Go evaluates against full workflow context (richer, includes parent scope).
	// ================================================================
	allTasks2, _ := e.store.TaskExecutions().List(wfEx.ID, "")
	for i, te := range allTasks2 {
		if te.ID == taskEx.ID {
			allTasks2[i] = taskEx // in-memory current state
			break
		}
	}
	exprCtx := workflow.BuildFullContext(wfEx, wfSpec, allTasks2)
	// Also expose the current action execution result so expressions like
	//   break-on: <% $.last_error.contains('timeout') %>  work.
	if states.IsError(actionEx.State) {
		if si, ok := actionEx.StateInfo.(string); ok {
			exprCtx["last_error"] = si
		} else {
			exprCtx["last_error"] = fmt.Sprintf("%v", actionEx.StateInfo)
		}
		exprCtx["last_output"] = actionEx.Output
	}

	breakOnRaw := retrySpec.GetBreakOn()
	continueOnRaw := retrySpec.GetContinueOn()

	breakTriggered := false
	if states.IsError(actionEx.State) && breakOnRaw != nil {
		var boVal interface{}
		if s, ok := breakOnRaw.(string); ok {
			boVal = expressions.EvaluateRecursively(s, exprCtx)
		} else {
			boVal = breakOnRaw
		}
		breakTriggered = isTruthy(boVal)
		if breakTriggered {
			log.Printf("retry: break-on triggered for actionEx %s (state_info=%v) — aborting retry",
				actionEx.ID, actionEx.StateInfo)
		}
	}

	stopContinue := false
	if actionEx.State == states.SUCCESS {
		// Python: stop_continue_flag = (state == SUCCESS) AND (continue_on NOT SET OR evaluates falsy)
		// Must NOT trigger on ERROR — we want ERROR → retry (unless break_on fires).
		if continueOnRaw == nil {
			stopContinue = true // SUCCESS + no continue-on → default: don't retry
		} else {
			var coVal interface{}
			if s, ok := continueOnRaw.(string); ok {
				coVal = expressions.EvaluateRecursively(s, exprCtx)
			} else {
				coVal = continueOnRaw
			}
			stopContinue = !isTruthy(coVal)
			if stopContinue {
				log.Printf("retry: continue-on evaluated falsy for actionEx %s — aborting retry",
					actionEx.ID)
			}
		}
	}

	if breakTriggered || stopContinue {
		return false, nil
	}

	attempt++
	sync := true
	buildRetryActionEx := func() *db.ActionExecution {
		return &db.ActionExecution{
			Name:  actionEx.Name,
			State: states.RUNNING,
			Input: actionEx.Input,
			RuntimeContext: map[string]interface{}{
				"index":          db.CoerceInt(actionEx.RuntimeContext["index"]),
				"safe_rerun":     true,
				"retry_attempts": attempt,
			},
			WorkflowNamespace: actionEx.WorkflowNamespace,
			TaskExecutionID:   actionEx.TaskExecutionID,
			WorkflowName:      actionEx.WorkflowName,
			WorkflowID:        actionEx.WorkflowID,
			ProjectID:         actionEx.ProjectID,
			IsSync:            &sync,
		}
	}

	// Delayed retry path: set task → RUNNING_DELAYED, defer via scheduler.
	if retryDelaySeconds > 0 && e.sched != nil {
		delay := time.Duration(retryDelaySeconds) * time.Second

		taskEx.State = states.RUNNING_DELAYED
		taskEx.StateInfo = fmt.Sprintf("Delayed by 'retry' policy [delay=%ds]", retryDelaySeconds)
		// Stash the retry ActionEx creation parameters in taskEx.RuntimeContext
		// so the JobContinueTask handler (fired by scheduler) can re-build them.
		taskEx.RuntimeContext["retry_attempts"] = attempt
		taskEx.RuntimeContext["retry_action_input"] = actionEx.Input
		taskEx.RuntimeContext["retry_action_name"] = actionEx.Name
		taskEx.RuntimeContext["retry_action_index"] = db.CoerceInt(actionEx.RuntimeContext["index"])
		if err := e.store.TaskExecutions().Update(taskEx); err != nil {
			log.Printf("retry: update taskEx to RUNNING_DELAYED failed: %v", err)
		}

		e.sched.Schedule(scheduler.JobContinueTask, map[string]interface{}{
			"task_ex_id":             taskEx.ID,
			"stashed_retry_attempts": attempt,
			"retry_limit":            retryCount,
		}, delay)

		log.Printf("retry: ActionExecution %s scheduled DELAYED retry attempt %d/%d (delay=%ds)",
			actionEx.ID, attempt, retryCount, retryDelaySeconds)
		return true, nil
	}

	// Immediate retry path (delay=0 or no scheduler).
	newActionEx := buildRetryActionEx()
	if err := e.store.ActionExecutions().Create(newActionEx); err != nil {
		return false, fmt.Errorf("retry creating ActionExecution: %w", err)
	}

	e.scheduleTaskTimeout(newActionEx, taskEx, taskSpec)

	log.Printf("retry: ActionExecution %s scheduled retry attempt %d/%d → new action_ex=%s (state=RUNNING)",
		actionEx.ID, attempt, retryCount, newActionEx.ID)

	return true, nil
}

// forceFailWorkflow forcibly transitions a workflow execution to ERROR state
// when the engine encounters an internal failure it cannot recover from.
// Mirrors Python engine's force_fail_task — engine-level errors must propagate
// to workflow level so operators see them, not just a log line.
func (e *DefaultEngine) forceFailWorkflow(wfEx *db.WorkflowExecution, reason string) {
	if wfEx == nil {
		return
	}
	if states.IsCompleted(wfEx.State) || states.IsPaused(wfEx.State) {
		return
	}
	wfEx.State = states.ERROR
	wfEx.StateInfo = reason
	wfEx.Accepted = true
	if err := e.store.WorkflowExecutions().Update(wfEx); err != nil {
		log.Printf("forceFailWorkflow: failed to persist wfEx %s ERROR: %v", wfEx.ID, err)
	} else {
		log.Printf("forceFailWorkflow: wfEx %s → ERROR (%s)", wfEx.ID, reason)
	}
}

// advanceWorkflow is the deferred-completion helper used by wait-after policy
// (and any future policy that defers task → workflow advancement). It mirrors
// the tail of OnActionComplete: load wfSpec, get controller, ContinueWorkflow,
// DispatchWorkflowCommands, and checkAndCompleteWorkflow.
//
// wfExID is the workflow execution; doneTaskExID (optional, "") is the
// task that just completed — passed through so workflow policies like
// direct-join can observe it.
func (e *DefaultEngine) advanceWorkflow(wfExID string, doneTaskExID string) {
	wfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil || wfEx == nil {
		log.Printf("advanceWorkflow: wfEx %s not found: %v", wfExID, err)
		return
	}
	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef == nil {
		return
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	var doneTaskEx *db.TaskExecution
	if doneTaskExID != "" {
		for _, te := range tasks {
			if te.ID == doneTaskExID {
				doneTaskEx = te
				break
			}
		}
	}

	ctrl := workflow.GetController(wfEx, wfSpec, tasks)
	cmds := ctrl.ContinueWorkflow(doneTaskEx)

	if err := e.dispatcher.DispatchWorkflowCommands(wfEx, cmds); err != nil {
		log.Printf("advanceWorkflow: DispatchWorkflowCommands failed: %v", err)
		return
	}

	if err := e.checkAndCompleteWorkflow(wfEx, ctrl, wfSpec); err != nil {
		log.Printf("advanceWorkflow: checkAndCompleteWorkflow failed: %v", err)
	}

	// P0: same as OnActionComplete — wake up join tasks after deferred advance
	// (wait-after policy path). doneTaskEx carries the freshly-completed task.
	if doneTaskEx != nil {
		e.checkAffectedTasks(doneTaskEx, wfEx)
	}
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
	// Python ContextView flattens wf_ex.input keys to top level.
	for k, v := range wfInput {
		if _, exists := ctx[k]; !exists {
			ctx[k] = v
		}
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

// isTruthy returns true for Go truthy values — mirrors Python's bool() semantics.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case []interface{}:
		return len(x) > 0
	case map[string]interface{}:
		return len(x) > 0
	}
	return true
}

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

// --- P0: join-task refresh logic -------------------------------------------
// Ported from mistral/engine/task_handler.py:
//   _check_affected_tasks → scheduleRefreshTaskState → refreshTaskState

// checkAffectedTasks examines the completed task and wakes any waiting join
// tasks that are transitively downstream. Called by OnActionComplete and
// advanceWorkflow after the main workflow advance is done.
func (e *DefaultEngine) checkAffectedTasks(taskEx *db.TaskExecution, wfEx *db.WorkflowExecution) {
	if taskEx == nil || wfEx == nil {
		return
	}
	// Workflow already terminal — nothing to refresh.
	if states.IsCompleted(wfEx.State) || states.IsPaused(wfEx.State) {
		return
	}
	// Task not terminal — shouldn't happen here, but guard anyway.
	if !states.IsCompleted(taskEx.State) {
		return
	}
	// Must have a scheduler wired in to defer the refresh.
	if e.sched == nil {
		return
	}

	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef == nil {
		return
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	if wfSpec == nil {
		return
	}
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	ctrl := workflow.GetController(wfEx, wfSpec, tasks)
	affectedIDs := ctrl.FindIndirectlyAffectedTasks(taskEx.Name)
	if len(affectedIDs) == 0 {
		return
	}

	for _, id := range affectedIDs {
		e.scheduleRefreshTaskState(id)
	}
	log.Printf("checkAffectedTasks: task %s completed → scheduled refresh for %d join task(s)",
		taskEx.Name, len(affectedIDs))
}

// scheduleRefreshTaskState schedules a refresh for a single join taskEx via
// the shared scheduler with key-based dedup. Multiple upstream completions
// that target the same join task collapse to one scheduled job.
func (e *DefaultEngine) scheduleRefreshTaskState(taskExID string) {
	if taskExID == "" || e.sched == nil {
		return
	}
	dedupKey := "th_r_t_s-" + taskExID
	if _, err := e.sched.ScheduleWithKey(
		scheduler.JobRefreshTaskState,
		map[string]interface{}{"task_ex_id": taskExID},
		0, // fire as soon as scheduler picks it up
		dedupKey,
	); err != nil {
		log.Printf("scheduleRefreshTaskState: failed to schedule for %s: %v", taskExID, err)
	}
}

// refreshTaskState is the scheduler callback. It re-evaluates the join task's
// logical state and either continues it, completes it with ERROR, or no-ops
// if still waiting. Mirrors Python task_handler._refresh_task_state.
func (e *DefaultEngine) refreshTaskState(taskExID string) {
	taskEx, err := e.store.TaskExecutions().Get(taskExID)
	if err != nil || taskEx == nil {
		log.Printf("refreshTaskState: taskEx %s not found: %v", taskExID, err)
		return
	}

	wfEx, err := e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
	if err != nil || wfEx == nil {
		return
	}
	if states.IsCompleted(wfEx.State) || states.IsPaused(wfEx.State) {
		return
	}

	wfDef, _ := e.store.WorkflowDefinitions().Get(wfEx.WorkflowID)
	if wfDef == nil {
		return
	}
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	if wfSpec == nil {
		return
	}
	tasks, _ := e.store.TaskExecutions().List(wfEx.ID, "")

	ctrl := workflow.GetController(wfEx, wfSpec, tasks)
	logical, err := ctrl.GetLogicalTaskState(taskEx)
	if err != nil {
		log.Printf("refreshTaskState: GetLogicalTaskState failed for %s: %v", taskEx.Name, err)
		return
	}

	switch logical.State {
	case states.RUNNING:
		log.Printf("refreshTaskState: join task %s → RUNNING (dependencies met) → continueTask", taskEx.Name)
		e.continueTask(taskEx, wfEx)
	case states.ERROR:
		log.Printf("refreshTaskState: join task %s → ERROR (dependencies unresolvable)", taskEx.Name)
		taskEx.State = states.ERROR
		taskEx.StateInfo = logical.StateInfo
		taskEx.Processed = true
		_ = e.store.TaskExecutions().Update(taskEx)
		e.advanceWorkflow(wfEx.ID, taskEx.ID)
	default:
		// Still WAITING — nothing to do.
		log.Printf("refreshTaskState: join task %s still WAITING (cardinality=%d) — no-op",
			taskEx.Name, logical.Cardinality)
	}
}

// continueTask resets a task to RUNNING state and re-enters StartTask so a
// new ActionExecution is created and dispatched. Mirrors Python
// task_handler.continue_task called from _refresh_task_state.
func (e *DefaultEngine) continueTask(taskEx *db.TaskExecution, wfEx *db.WorkflowExecution) {
	taskEx.State = states.RUNNING
	taskEx.StateInfo = ""
	taskEx.Processed = false
	_ = e.store.TaskExecutions().Update(taskEx)

	if err := e.StartTask(context.Background(), taskEx.ID, false, false, nil, false, false); err != nil {
		log.Printf("continueTask: StartTask(%s) failed: %v", taskEx.ID, err)
	}
}

// ================================================================
// P2.1: on_action_update — subworkflow PAUSE/RUNNING cascade to parent workflow.
// Ported from Python mistral/engine/task_handler.py:_on_action_update.
//
// Called externally whenever an ActionExecution transitions to a non-terminal
// state (e.g. PAUSE, RUNNING). The logic is:
//   - action_ex.state = PAUSED → pause the parent workflow
//   - action_ex.state = RUNNING → resume parent workflow BUT only if no other
//     sibling task in the parent is still PAUSED (keep parent paused)
//
// In Go this is invoked by the API layer when clients explicitly pause/resume
// a subworkflow (child WorkflowExecution). The PAUSE/RESUME API calls mutate
// childWfEx.state; callers should then invoke OnActionUpdate(childWfEx) which
// reads childWfEx.TaskExecutionID (the parent's taskEx row) and walks to the
// parent wfEx to apply cascade rules.
// ================================================================
func (e *DefaultEngine) OnActionUpdate(wfExID string) error {
	childWfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil || childWfEx == nil {
		return fmt.Errorf("OnActionUpdate: child wfEx %s not found: %v", wfExID, err)
	}

	taskEx, err := e.store.TaskExecutions().Get(childWfEx.TaskExecutionID)
	if err != nil || taskEx == nil {
		// Not a subworkflow (childWfEx has no parent taskEx) → nothing to cascade.
		return nil
	}

	parentWfEx, err := e.store.WorkflowExecutions().Get(taskEx.WorkflowExecutionID)
	if err != nil || parentWfEx == nil {
		return nil
	}

	log.Printf("OnActionUpdate: child wfEx %s state=%s → parent taskEx %s state=%s",
		childWfEx.ID, childWfEx.State, taskEx.ID, parentWfEx.State)

	if states.IsPaused(childWfEx.State) {
		// Cascade: parent workflow pauses.
		if !states.IsPaused(parentWfEx.State) {
			parentWfEx.State = states.PAUSED
			parentWfEx.StateInfo = fmt.Sprintf("Paused by subworkflow %s (task %s)", childWfEx.ID, taskEx.Name)
			if err := e.store.WorkflowExecutions().Update(parentWfEx); err != nil {
				return err
			}
			log.Printf("OnActionUpdate: parent wfEx %s → PAUSED (child wfEx %s)", parentWfEx.ID, childWfEx.ID)
		}
		// Parent task stays RUNNING_DELAYED-ish — mark as PAUSED so dispatcher skips.
		if !states.IsPausedOrCompleted(taskEx.State) {
			taskEx.State = states.PAUSED
			_ = e.store.TaskExecutions().Update(taskEx)
		}
		return nil
	}

	if states.IsRunning(childWfEx.State) {
		// Cascade: check whether parent has ANY other PAUSED taskEx.
		// If yes → parent stays PAUSED (multi-subworkflow mutual-cascade rule).
		// If no  → resume parent workflow.
		tasks, _ := e.store.TaskExecutions().List(parentWfEx.ID, "")
		hasOtherPaused := false
		for _, t := range tasks {
			if t.ID != taskEx.ID && states.IsPaused(t.State) {
				hasOtherPaused = true
				break
			}
		}
		if !hasOtherPaused && states.IsPaused(parentWfEx.State) {
			// Resume taskEx first if it's PAUSED, then resume parent wfEx.
			if states.IsPaused(taskEx.State) {
				taskEx.State = states.RUNNING_DELAYED
				_ = e.store.TaskExecutions().Update(taskEx)
			}
			return e.ResumeWorkflow(context.Background(), parentWfEx.ID, nil)
		}
		// Other siblings still paused → just resume this specific taskEx.
		if states.IsPaused(taskEx.State) {
			taskEx.State = states.RUNNING_DELAYED
			_ = e.store.TaskExecutions().Update(taskEx)
		}
	}
	return nil
}

// ================================================================
// D3: DispatchAction — executor integration.
//
// Ported from Python mistral/engine/default_engine.py:_run_action.
// The Go engine originally left action execution entirely to external test
// drivers (tests run the action and invoke OnActionComplete). This method
// closes the loop: given an ActionExecution ID, resolve the matching action
// from the registry, run it through the default executor, and feed the
// result back into OnActionComplete. This allows callers (tests, API
// clients) to fire an action without manual orchestration.
// ================================================================
func (e *DefaultEngine) DispatchAction(ctx context.Context, actionExID string) (*db.ActionExecution, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	actionEx, err := e.store.ActionExecutions().Get(actionExID)
	if err != nil || actionEx == nil {
		return nil, fmt.Errorf("DispatchAction: actionEx %s not found", actionExID)
	}
	if !states.IsRunning(actionEx.State) {
		return actionEx, nil // already terminal or waiting
	}

	action := actions.NewActionRegistry()
	d, ok := action.Get(actionEx.Name)
	if !ok {
		// No such action — fail gracefully rather than panic.
		if err := e.OnActionComplete(ctx, actionExID, &db.Result{Error: fmt.Errorf("unknown action %q", actionEx.Name)}, false); err != nil {
			return nil, err
		}
		return e.store.ActionExecutions().Get(actionExID)
	}
	inst := d.Factory()

	// Pull timeout from parent task's policies, if configured.
	var timeoutDur time.Duration
	taskEx, _ := e.store.TaskExecutions().Get(actionEx.TaskExecutionID)
	if taskEx != nil {
		wfDef, _ := e.store.WorkflowDefinitions().Get(taskEx.WorkflowID)
		if wfDef != nil {
			wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
			if wfSpec != nil {
				ts := wfSpec.GetTask(taskEx.Name)
				if ts != nil {
					if p := ts.GetPolicies(); p != nil {
						if td := p.GetTimeout(); td != nil {
							switch v := td.(type) {
							case float64:
								timeoutDur = time.Duration(int(v)) * time.Second
							case int:
								timeoutDur = time.Duration(v) * time.Second
							}
						}
					}
				}
			}
		}
	}

	exec := executors.GetDefaultExecutor()
	input := actionEx.Input
	if input == nil {
		input = map[string]interface{}{}
	}
	result, runErr := exec.RunAction(inst, actionEx.ID, false, input, "", false, timeoutDur)
	if runErr != nil {
		// Executor returned Go error — wrap in db.Result{Error: ...}.
		result = &db.Result{Error: runErr}
	}

	if err := e.OnActionComplete(ctx, actionExID, result, false); err != nil {
		return nil, err
	}
	return e.store.ActionExecutions().Get(actionExID)
}

// ================================================================
// _check_and_fix_integrity — crash-recovery sweep for stuck RUNNING tasks.
// Ported from Python mistral/engine/workflow_handler.py ~line 110.
//
// The pattern: a workflow may be left with taskEx.State=RUNNING but NO
// actionEx=RUNNING (engine crashed in between). The integrity check fires
// periodically, finds these stuck rows, and triggers OnActionComplete on
// the most recently finished child so the task can resume.
//
// This handler also reschedules itself with a 120-second delay — mirroring
// Python's `_schedule_check_and_fix_integrity(wf_ex, delay=120)`. The check
// stops rescheduling once the workflow reaches a terminal state.
// ================================================================

const integrityCheckDelay = 120 * time.Second

// scheduleIntegrityCheck queues the first (or next) integrity sweep for wfEx.
func (e *DefaultEngine) scheduleIntegrityCheck(wfExID string) {
	if wfExID == "" {
		return
	}
	if e.sched == nil {
		return // no scheduler configured (test mode)
	}
	args := map[string]interface{}{"wf_ex_id": wfExID}
	if _, err := e.sched.ScheduleWithKey(
		scheduler.JobIntegrityCheck,
		args,
		integrityCheckDelay,
		"",
	); err != nil {
		log.Printf("integrity_check: scheduler.Schedule failed: %v", err)
	}
}

func (e *DefaultEngine) checkIntegrity(ctx context.Context, wfExID string) {
	wfEx, err := e.store.WorkflowExecutions().Get(wfExID)
	if err != nil || wfEx == nil {
		return
	}

	// Stop rescheduling once workflow is terminal.
	if states.IsCompleted(wfEx.State) {
		return
	}

	// Reschedule ourselves FIRST so that even if we crash mid-sweep, the
	// next tick will pick up where we left off.
	e.scheduleIntegrityCheck(wfExID)

	// Find RUNNING task executions.
	taskExes, _ := e.store.TaskExecutions().List(wfExID, states.RUNNING)
	for _, taskEx := range taskExes {
		actionExes, _ := e.store.ActionExecutions().List(taskEx.ID, "")
		if len(actionExes) == 0 {
			continue
		}

		// Is every actionEx terminal? If yes, the task is stuck.
		allFinished := true
		var lastFinished *db.ActionExecution
		var lastFinishedAt time.Time
		for _, ae := range actionExes {
			if !states.IsCompleted(ae.State) {
				allFinished = false
				break
			}
			if ae.UpdatedAt.After(lastFinishedAt) {
				lastFinishedAt = ae.UpdatedAt
				lastFinished = ae
			}
		}
		if !allFinished {
			continue
		}

		// All children terminal → trigger OnActionComplete on the most
		// recently finished one to advance task state.
		if lastFinished == nil {
			continue
		}
		log.Printf("integrity_check: recovering stuck taskEx %s (wfEx %s) via child actionEx %s",
			taskEx.ID, wfEx.ID, lastFinished.ID)

		if err := e.OnActionComplete(ctx, lastFinished.ID, nil, false); err != nil {
			log.Printf("integrity_check: OnActionComplete failed: %v", err)
		}
	}
}
