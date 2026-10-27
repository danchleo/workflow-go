// Package workflow provides workflow control logic — the algorithms that
// decide which tasks to run next based on the current state of task executions.
//
// Ported from mistral-master/mistral/workflow/
package workflow

import (
	"fmt"

	"github.com/danchleo/workflow-go/internal/commands"
	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/expressions"
	v2 "github.com/danchleo/workflow-go/internal/lang/v2"
	"github.com/danchleo/workflow-go/internal/states"
)

// WorkflowController determines the next tasks to run given the current state.
// Different workflow types (direct, reverse) have different algorithms.
// Ported from mistral-master/mistral/workflow/base.py
type WorkflowController interface {
	// ContinueWorkflow calculates a list of commands to continue the workflow.
	// If taskEx is specified, it was the task that just completed.
	ContinueWorkflow(taskEx *db.TaskExecution) []commands.WorkflowCommand

	// RerunTasks returns commands to rerun the specified task executions.
	RerunTasks(taskExecs []*db.TaskExecution, reset bool) []commands.WorkflowCommand

	// SkipTasks returns commands to skip the specified task executions.
	SkipTasks(taskExecs []*db.TaskExecution) []commands.WorkflowCommand

	// GetLogicalTaskState determines what state a task SHOULD have.
	GetLogicalTaskState(taskEx *db.TaskExecution) (*TaskLogicalState, error)

	// IsErrorHandledFor reports whether errors for a task are handled.
	IsErrorHandledFor(taskEx *db.TaskExecution) bool

	// AllErrorsHandled reports whether all errors in the workflow are handled.
	AllErrorsHandled() bool

	// AnyCancels reports whether any task is cancelled.
	AnyCancels() bool

	// EvaluateWorkflowFinalContext builds the final output context.
	EvaluateWorkflowFinalContext() (map[string]interface{}, error)

	// MayCompleteWorkflow reports whether completing this task may complete the workflow.
	MayCompleteWorkflow(taskEx *db.TaskExecution) bool

	// FindIndirectlyAffectedTasks walks outbound transitions from the given task
	// and returns TaskExecution IDs of join tasks that are transitively affected.
	// Ported from mistral/workflow/direct_workflow.py find_indirectly_affected_task_executions.
	// Reverse workflow returns nil — there are no join tasks.
	FindIndirectlyAffectedTasks(taskName string) []string
}

// TaskLogicalState describes what state a task should have based on workflow logic.
type TaskLogicalState struct {
	State       string
	StateInfo   string
	Cardinality int // Number of unmet preconditions for WAITING state.
	TriggeredBy []map[string]interface{}
}

// --- DataFlow helpers ---

// BuildTaskResultMap builds a nested map like {"task1": {"result": ..., "published": ...}}
// so that expressions like <% $.task1.result %> resolve correctly.
// This mirrors data_flow.ContextView from mistral-master/mistral/workflow/data_flow.py
func BuildTaskResultMap(taskExecs []*db.TaskExecution) map[string]interface{} {
	result := make(map[string]interface{})
	for _, te := range taskExecs {
		if !states.IsCompleted(te.State) {
			continue
		}
		taskData := make(map[string]interface{})

		// Task's primary result (from action output).
		if len(te.Output) > 0 {
			if r, ok := te.Output["result"]; ok {
				taskData["result"] = r
			} else {
				taskData["result"] = te.Output
			}
		}

		// Task's published variables.
		if len(te.Published) > 0 {
			taskData["published"] = te.Published
			for k, v := range te.Published {
				if _, exists := taskData[k]; !exists {
					taskData[k] = v
				}
			}
		}

		taskData["state"] = te.State
		taskData["id"] = te.ID

		result[te.Name] = taskData
	}
	return result
}

// BuildFullContext builds a complete evaluation context for a workflow.
// Returns a map that supports expressions like:
//   - <% $.task1.result %>        → task1's action result (nested under task name)
//   - <% $.task1.my_published %>  → task1's published variable (nested under task name)
//   - <% $.my_published %>        → published variable (flattened to top level for convenience)
//   - <% $.input.var_name %>      → workflow input
//   - <% $.env.xxx %>             → environment variables
//   - <% $.vars.xxx %>            → workflow variables
func BuildFullContext(
	wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, taskExecs []*db.TaskExecution,
) map[string]interface{} {
	ctx := make(map[string]interface{})

	// Priority order (mirrors Python ContextView priority chain — lower priority
	// dicts are written FIRST so higher priority later writes naturally overwrite):
	//   1. wfEx.Input    (lowest — workflow input params, never change)
	//   2. wfEx.Context  (global publish cumulative + vars — grows as tasks complete)
	//   3. task layers   (ctx[taskName] = {...} per-task scoped view — highest)
	//   4. env + __execution + __task_results

	// Layer 1: Input (flatten + nested ctx["input"]).
	for k, v := range wfEx.Input {
		ctx[k] = v
	}
	ctx["input"] = wfEx.Input

	// Layer 2: wfEx.Context (global publish cumulative + workflow vars).
	for k, v := range wfEx.Context {
		ctx[k] = v
	}

	// Layer 3: Per-task views (task output + branch publish + state).
	taskResults := make(map[string]interface{})
	for _, te := range taskExecs {
		if !states.IsCompleted(te.State) {
			continue
		}
		taskData := make(map[string]interface{})

		if len(te.Output) > 0 {
			if r, ok := te.Output["result"]; ok {
				taskData["result"] = r
			} else {
				taskData["result"] = te.Output
			}
		}

		if len(te.Published) > 0 {
			taskData["published"] = te.Published
		}

		taskData["state"] = te.State
		taskData["id"] = te.ID
		ctx[te.Name] = taskData
		taskResults[te.Name] = taskData
	}
	ctx["__task_results"] = taskResults

	// Env — Python mistral calls this __env; we expose both.
	var envMap map[string]interface{}
	if e, ok := wfEx.Params["env"].(map[string]interface{}); ok {
		envMap = e
	} else {
		envMap = make(map[string]interface{})
	}
	ctx["env"] = envMap
	ctx["__env"] = envMap

	// __execution — Python's execution() builtin.
	ctx["__execution"] = map[string]interface{}{
		"id":     wfEx.ID,
		"name":   wfEx.Name,
		"state":  wfEx.State,
		"params": wfEx.Params,
		"input":  wfEx.Input,
	}

	// Vars.
	if wfSpec != nil {
		varsMap := make(map[string]interface{})
		for k, v := range wfSpec.GetVars() {
			varsMap[k] = v
		}
		ctx["vars"] = varsMap
	} else {
		ctx["vars"] = make(map[string]interface{})
	}

	return ctx
}

// BuildTaskExpressionContext adds per-task execution details to ctx.
// In Python mistral this is the __task_execution special key.
// Also makes task() (no-arg) work by pointing __current_task.
func BuildTaskExpressionContext(ctx map[string]interface{}, taskEx *db.TaskExecution) map[string]interface{} {
	if ctx == nil {
		ctx = make(map[string]interface{})
	}
	if taskEx == nil {
		return ctx
	}
	tex := map[string]interface{}{
		"id":              taskEx.ID,
		"name":            taskEx.Name,
		"state":           taskEx.State,
		"in_context":      taskEx.InContext,
		"runtime_context": taskEx.RuntimeContext,
	}
	ctx["__task_execution"] = tex
	ctx["__current_task"] = tex // for task() no-arg builtin
	return ctx
}

// --- DirectWorkflowController ---

// DirectWorkflowController implements the direct workflow algorithm.
// Ported from mistral-master/mistral/workflow/direct_workflow.py
type DirectWorkflowController struct {
	wfEx   *db.WorkflowExecution
	wfSpec *v2.WorkflowSpec
	tasks  []*db.TaskExecution // All task executions for this workflow
}

// NewDirectWorkflowController creates a new direct workflow controller.
func NewDirectWorkflowController(
	wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, tasks []*db.TaskExecution,
) *DirectWorkflowController {
	return &DirectWorkflowController{
		wfEx:   wfEx,
		wfSpec: wfSpec,
		tasks:  tasks,
	}
}

// GetController returns the appropriate controller based on workflow type.
func GetController(wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, tasks []*db.TaskExecution) WorkflowController {
	wfType := wfSpec.GetType()
	switch wfType {
	case "direct":
		return NewDirectWorkflowController(wfEx, wfSpec, tasks)
	case "reverse":
		return NewReverseWorkflowController(wfEx, wfSpec, tasks)
	default:
		return NewDirectWorkflowController(wfEx, wfSpec, tasks) // Fallback.
	}
}

func (c *DirectWorkflowController) ContinueWorkflow(taskEx *db.TaskExecution) []commands.WorkflowCommand {
	if states.IsCompleted(c.wfEx.State) || states.IsPaused(c.wfEx.State) {
		return nil
	}
	return c.findNextCommands(taskEx)
}

func (c *DirectWorkflowController) findNextCommands(taskEx *db.TaskExecution) []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand

	if taskEx == nil {
		// Initial workflow start: run all start tasks.
		for _, startTask := range c.wfSpec.StartTasks() {
			ctx := c.calculateInboundContext(startTask, nil)
			cmds = append(cmds, commands.NewRunTask(startTask.GetName(), ctx, nil, false))
		}
		return cmds
	}

	taskSpec := c.wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return nil
	}

	// Build the ContextView that Python uses for condition/param evaluation.
	ctxView := BuildFullContext(c.wfEx, c.wfSpec, c.tasks)
	// Inject the current task's outbound context (task dict) — Python
	// ContextView prepends get_current_task_dict(task_ex).
	ctxView["__current_task"] = taskEx
	ctxView["current_task"] = map[string]interface{}{
		"id":     taskEx.ID,
		"name":   taskEx.Name,
		"state":  taskEx.State,
		"result": taskEx.Output,
	}

	// Python _find_next_tasks clause ordering:
	//   1. ERROR     → on-error
	//   2. SKIPPED   → on-skip (empty fallback to on-success)
	//   3. SUCCESS   → on-success
	//   4. completed & not CANCELLED/SKIPPED → on-complete (APPENDED last)
	var candidateClauses []*v2.OnClauseSpec
	skipFallbackSuccess := false

	switch taskEx.State {
	case states.ERROR:
		if cl := taskSpec.GetOnError(); cl != nil {
			candidateClauses = append(candidateClauses, cl)
		}
	case states.SKIPPED:
		if cl := taskSpec.GetOnSkip(); cl != nil {
			candidateClauses = append(candidateClauses, cl)
			// Check if on-skip has ANY matched entries; if not, fall back to on-success.
			if !cl.HasAnyWhen() {
				skipFallbackSuccess = true
			}
		} else {
			skipFallbackSuccess = true
		}
	}

	// SUCCESS or skip-empty fallback → on-success.
	if taskEx.State == states.SUCCESS || skipFallbackSuccess {
		if cl := taskSpec.GetOnSuccess(); cl != nil {
			candidateClauses = append(candidateClauses, cl)
		}
	}

	// on-complete: only if task is in a terminal state that's NOT cancelled/skipped.
	if states.IsCompleted(taskEx.State) && !states.IsCancelledOrSkipped(taskEx.State) {
		if cl := taskSpec.GetOnComplete(); cl != nil {
			candidateClauses = append(candidateClauses, cl)
		}
	}

	if len(candidateClauses) == 0 {
		return nil
	}

	for _, clause := range candidateClauses {
		for _, next := range clause.GetNext() {
			// Evaluate "when" condition (empty or missing = always run).
			if next.When != "" {
				truthy, err := expressions.EvaluateBool(next.When, ctxView)
				if err != nil {
					// Fail-open? Python expr.evaluate returns truthy on error.
					// We log but default to true to match mistral's resilient behavior.
					truthy = true
				}
				if !truthy {
					continue
				}
			}

			if v2.IsEngineCommand(next.Task) {
				switch next.Task {
				case v2.FailCommand:
					cmds = append(cmds, commands.NewFailWorkflow(taskEx.Name, nil, "", nil, true))
				case v2.SucceedCommand:
					cmds = append(cmds, commands.NewSucceedWorkflow(taskEx.Name, nil, "", nil, true))
				case v2.PauseCommand:
					cmds = append(cmds, commands.NewPauseWorkflow(taskEx.Name, nil, "", nil, true))
				case v2.NoopCommand:
					// Skip noop.
				}
				continue
			}

			triggeredBy := []map[string]interface{}{
				{
					"task_id":   taskEx.ID,
					"task_name": taskEx.Name,
					"event":     taskEx.State,
				},
			}

			nextTaskSpec := c.wfSpec.GetTask(next.Task)
			if nextTaskSpec == nil {
				continue
			}

			// Join logic (same as before).
			joinVal := nextTaskSpec.GetJoin()
			inboundSpecs := c.wfSpec.InboundTaskSpecs(nextTaskSpec)
			inboundExecs := c.filterCompletedTasksForSpecs(inboundSpecs)

			shouldRun := true
			if joinVal != nil {
				switch jv := joinVal.(type) {
				case string:
					if jv == "all" {
						shouldRun = len(inboundExecs) >= len(inboundSpecs)
					} else if jv == "one" {
						shouldRun = len(inboundExecs) >= 1
					}
				case float64:
					shouldRun = len(inboundExecs) >= int(jv)
				case int:
					shouldRun = len(inboundExecs) >= jv
				}
			}

			if !shouldRun {
				continue
			}

			ctx := c.calculateInboundContext(nextTaskSpec, taskEx)

			// Recursively evaluate params (Python expr.evaluate_recursively).
			params := next.Params
			if len(params) > 0 {
				evaluated := expressions.EvaluateRecursively(params, ctxView)
				if m, ok := evaluated.(map[string]interface{}); ok {
					ctx = mergeMaps(ctx, m)
				}
			}

			handlesError := taskEx.State == states.ERROR
			cmds = append(cmds, commands.NewRunTask(next.Task, ctx, triggeredBy, handlesError))
		}
	}

	return cmds
}

// mergeMaps merges b into a (shallow copy — values may themselves be maps).
func mergeMaps(a, b map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func (c *DirectWorkflowController) filterCompletedTasksForSpecs(specs []*v2.TaskSpec) []*db.TaskExecution {
	var result []*db.TaskExecution
	for _, te := range c.tasks {
		if !states.IsCompleted(te.State) {
			continue
		}
		for _, spec := range specs {
			if te.Name == spec.GetName() {
				result = append(result, te)
				break
			}
		}
	}
	return result
}

// calculateInboundContext builds the inbound context for a task, including
// upstream task results under their task names (so $.upstream.result works).
func (c *DirectWorkflowController) calculateInboundContext(
	taskSpec *v2.TaskSpec, triggerTask *db.TaskExecution,
) map[string]interface{} {
	ctx := make(map[string]interface{})

	// Add upstream task results nested under task names.
	inboundSpecs := c.wfSpec.InboundTaskSpecs(taskSpec)
	for _, spec := range inboundSpecs {
		for _, te := range c.tasks {
			if te.Name == spec.GetName() && states.IsCompleted(te.State) {
				taskData := make(map[string]interface{})
				if r, ok := te.Output["result"]; ok {
					taskData["result"] = r
				}
				for k, v := range te.Published {
					taskData[k] = v
					if _, exists := taskData[k]; !exists {
						taskData[k] = v
					}
				}
				taskData["state"] = te.State
				ctx[te.Name] = taskData
				break
			}
		}
	}

	// Also include the trigger task's data.
	if triggerTask != nil {
		taskData, ok := ctx[triggerTask.Name].(map[string]interface{})
		if !ok || len(taskData) == 0 {
			taskData = make(map[string]interface{})
			if r, ok := triggerTask.Output["result"]; ok {
				taskData["result"] = r
			}
			for k, v := range triggerTask.Published {
				taskData[k] = v
			}
			taskData["state"] = triggerTask.State
			ctx[triggerTask.Name] = taskData
		}
	}

	// Add input and env.
	ctx["input"] = c.wfEx.Input
	for k, v := range c.wfEx.Input {
		if _, exists := ctx[k]; !exists {
			ctx[k] = v
		}
	}
	if env, ok := c.wfEx.Params["env"].(map[string]interface{}); ok {
		ctx["env"] = env
	}

	return ctx
}

func (c *DirectWorkflowController) RerunTasks(taskExecs []*db.TaskExecution, reset bool) []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand
	for _, te := range taskExecs {
		cmds = append(cmds, commands.NewRunExistingTask(te.ID, te.Name, te.InContext, reset, true, nil, false))
	}
	return cmds
}

func (c *DirectWorkflowController) SkipTasks(taskExecs []*db.TaskExecution) []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand
	for _, te := range taskExecs {
		cmds = append(cmds, commands.NewSkipTask(te.ID, te.Name, te.InContext, nil, false))
	}
	return cmds
}

func (c *DirectWorkflowController) GetLogicalTaskState(taskEx *db.TaskExecution) (*TaskLogicalState, error) {
	if states.IsCompleted(taskEx.State) {
		return &TaskLogicalState{State: taskEx.State}, nil
	}

	taskSpec := c.wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return nil, fmt.Errorf("unknown task: %s", taskEx.Name)
	}

	inboundSpecs := c.wfSpec.InboundTaskSpecs(taskSpec)

	// Build fast lookup from task name → TaskExecution.
	tExecByName := map[string]*db.TaskExecution{}
	for _, te := range c.tasks {
		tExecByName[te.Name] = te
	}

	// Classify inbound executions by state.
	// Plus: for inbound tasks that have NO TaskExecution yet, recursively check
	// whether they could possibly be created (mirrors Python
	// _get_induced_join_state + _possible_route). If they can never be created,
	// count them as ERROR-like blockers.
	var successCnt, errorCnt int
	var waitingNames, impossibleNames []string
	for _, spec := range inboundSpecs {
		name := spec.GetName()
		matched := tExecByName[name]
		if matched == nil {
			// No TaskExecution yet — check if it can possibly be triggered.
			if possible, _ := c.possibleRoute(spec, tExecByName, map[string]bool{}, 0); possible {
				waitingNames = append(waitingNames, name)
			} else {
				impossibleNames = append(impossibleNames, name)
				errorCnt++ // permanently blocked — treat like ERROR for join math
			}
			continue
		}
		switch matched.State {
		case states.SUCCESS, states.SKIPPED:
			successCnt++
		case states.ERROR, states.CANCELLED:
			errorCnt++
		default:
			waitingNames = append(waitingNames, name)
		}
	}

	joinVal := taskSpec.GetJoin()
	totalInbound := len(inboundSpecs)
	if totalInbound == 0 {
		return &TaskLogicalState{State: states.RUNNING}, nil
	}

	// Resolve join requirement.
	joinRequirement := 1
	joinIsAll := false
	if joinVal != nil {
		switch jv := joinVal.(type) {
		case string:
			if jv == "all" {
				joinRequirement = totalInbound
				joinIsAll = true
			} else if jv == "one" {
				joinRequirement = 1
			}
		case float64:
			joinRequirement = int(jv)
		case int:
			joinRequirement = jv
		}
	}

	// SUCCESS count meets requirement → RUNNING.
	if successCnt >= joinRequirement {
		return &TaskLogicalState{State: states.RUNNING}, nil
	}

	// Check if permanently blocked:
	//   - join:all with any ERROR (incl. impossible route) → impossible to satisfy
	//   - join:N / one  → errors leave fewer remaining than requirement
	if joinIsAll && errorCnt > 0 {
		msg := fmt.Sprintf("Blocked by failed inbound tasks: %v", waitingNames)
		if len(impossibleNames) > 0 {
			msg = fmt.Sprintf("Blocked — upstream tasks have no possible route: %v", impossibleNames)
		}
		return &TaskLogicalState{State: states.ERROR, StateInfo: msg}, nil
	}
	if !joinIsAll && errorCnt > (totalInbound-joinRequirement) {
		return &TaskLogicalState{
			State: states.ERROR,
			StateInfo: fmt.Sprintf(
				"Cannot satisfy join requirement: %d succeeded, %d failed (need %d)", successCnt, errorCnt,
				joinRequirement,
			),
		}, nil
	}

	cardinality := joinRequirement - successCnt
	return &TaskLogicalState{State: states.WAITING, Cardinality: cardinality}, nil
}

// possibleRoute recursively checks whether the given taskSpec can possibly be
// triggered. Mirrors Python DirectWorkflowController._possible_route.
//
// A task can be triggered if:
//   - It has no inbound specs (start task → always possible), OR
//   - At least one inbound spec has a TaskExecution that is NOT terminal
//     (still running → could complete and trigger us), OR
//   - At least one inbound spec has a COMPLETED TaskExecution whose outbound
//     clauses (on-success/on-error/...) include us (we were actually triggered),
//     OR
//   - At least one ancestor branch itself has a possible route (no dead end).
//
// Key insight (matching Python): if inbound's TaskExecution is ERROR but
// inbound has NO on-error clause pointing at us → we CANNOT be triggered
// from that branch. We must check spec-level outbound definitions.
//
// Cycle guard: visited map prevents infinite loops in DAG traversal.
// Depth guard: 30 matches Python MAX_SEARCH_DEPTH.
func (c *DirectWorkflowController) possibleRoute(
	spec *v2.TaskSpec, tExecByName map[string]*db.TaskExecution, visited map[string]bool, depth int,
) (bool, int) {
	const maxDepth = 30
	if depth > maxDepth {
		return true, depth // depth cap — assume possible to avoid false negatives
	}
	name := spec.GetName()
	if visited[name] {
		return true, depth // already checked upstream → don't re-enter
	}
	visited[name] = true

	// Has TaskExecution?
	if te, ok := tExecByName[name]; ok {
		if !states.IsCompleted(te.State) {
			return true, depth // running/waiting — route exists, hasn't finished
		}
		// Terminal state: was this specific task triggered by some inbound?
		// Python checks t_ex.next_tasks which is populated dynamically; we
		// approximate by checking spec-level outbound clauses.
		// If ANY inbound spec's outbound leads to us → possible.
		return true, depth // terminal task had exec → route existed (done)
	}

	// No TaskExecution — recurse into inbound.
	inSpecs := c.wfSpec.InboundTaskSpecs(spec)
	if len(inSpecs) == 0 {
		return true, depth // start task — guaranteed to be created eventually
	}

	for _, in := range inSpecs {
		inTe, hasExec := tExecByName[in.GetName()]

		if hasExec {
			// Inbound has execution.
			if !states.IsCompleted(inTe.State) {
				// Inbound still running → could trigger us later.
				return true, depth
			}
			// Inbound is terminal. Does it have an outbound clause that
			// could trigger US given its actual state?
			if inboundCouldTrigger(in, inTe.State, name) {
				return true, depth
			}
			// Inbound terminal but can't trigger us → dead end, check next inbound.
			continue
		}

		// Inbound has NO execution — recurse into ITS inbound.
		if sub, _ := c.possibleRoute(in, tExecByName, visited, depth+1); sub {
			return true, depth
		}
	}
	return false, depth // all upstream branches dead-ended
}

// inboundCouldTrigger checks whether an inbound spec could trigger us given
// that its own execution ended in endState. Mirrors Python's t_name in next_tasks
// check — static, using spec-level outbound clause definitions.
func inboundCouldTrigger(inbound *v2.TaskSpec, endState, ourName string) bool {
	if inbound == nil {
		return false
	}
	// On-SUCCESS always fires for SUCCESS/SKIPPED.
	if endState == states.SUCCESS || endState == states.SKIPPED {
		return clauseContainsTask(inbound.GetOnSuccess(), ourName)
	}
	// On-ERROR fires for ERROR/CANCELLED.
	if endState == states.ERROR || endState == states.CANCELLED {
		if clauseContainsTask(inbound.GetOnError(), ourName) {
			return true
		}
		if clauseContainsTask(inbound.GetOnComplete(), ourName) {
			return true
		}
		if clauseContainsTask(inbound.GetOnSkip(), ourName) {
			return true
		}
		return false
	}
	return clauseContainsTask(inbound.GetOnComplete(), ourName)
}

// clauseContainsTask returns true if any NextTaskNames from the clause contain target.
func clauseContainsTask(clause *v2.OnClauseSpec, target string) bool {
	if clause == nil {
		return false
	}
	for _, n := range clause.NextTaskNames() {
		if n == target {
			return true
		}
	}
	return false
}

func (c *DirectWorkflowController) IsErrorHandledFor(taskEx *db.TaskExecution) bool {
	if taskEx.State != states.ERROR {
		return true
	}

	taskSpec := c.wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return false
	}

	if taskSpec.GetOnError() != nil {
		return true
	}

	taskDefaults := c.wfSpec.GetTaskDefaults()
	if taskDefaults != nil && taskDefaults.GetOnError() != nil {
		return true
	}

	return false
}

func (c *DirectWorkflowController) AllErrorsHandled() bool {
	for _, te := range c.tasks {
		if te.State == states.ERROR && !c.IsErrorHandledFor(te) {
			return false
		}
	}
	return true
}

func (c *DirectWorkflowController) AnyCancels() bool {
	for _, te := range c.tasks {
		if te.State == states.CANCELLED {
			return true
		}
	}
	return false
}

func (c *DirectWorkflowController) EvaluateWorkflowFinalContext() (map[string]interface{}, error) {
	return BuildFullContext(c.wfEx, c.wfSpec, c.tasks), nil
}

func (c *DirectWorkflowController) MayCompleteWorkflow(taskEx *db.TaskExecution) bool {
	// Python: super().may_complete_workflow(task_ex) and not task_ex.has_next_tasks
	// Base = states.is_completed(task_ex.state); Direct adds the has_next_tasks check.
	if !states.IsCompleted(taskEx.State) {
		return false
	}
	// If this task already has known next tasks, we can't complete yet —
	// downstream commands may still be in flight.
	if taskEx.HasNextTasks {
		return false
	}
	// Also ensure no sibling tasks are still pending — another pending
	// task could still produce downstream transitions that affect completion.
	for _, te := range c.tasks {
		if te.ID != taskEx.ID && !states.IsCompleted(te.State) {
			return false
		}
	}
	return true
}

// FindIndirectlyAffectedTasks walks outbound transitions from the given
// task and collects TaskExecution IDs of join tasks that are transitively
// affected. A join task is "affected" if there is a path via on-success /
// on-error / on-complete / on-skip from the source task to a join task,
// AND that join task has a TaskExecution row in DB.
//
// This is what wakes waiting join tasks when a dependency completes.
// Ported from mistral/workflow/direct_workflow.py: find_indirectly_affected_task_executions
func (c *DirectWorkflowController) FindIndirectlyAffectedTasks(taskName string) []string {
	// Collect all join task spec names.
	allJoins := map[string]bool{}
	for name, spec := range c.wfSpec.GetTasks() {
		_ = name
		if spec.GetJoin() != nil {
			allJoins[spec.GetName()] = true
		}
	}
	if len(allJoins) == 0 {
		return nil
	}

	// Build TaskExecution lookup by name (only for join tasks — we don't need all).
	tExecByName := map[string]*db.TaskExecution{}
	for _, te := range c.tasks {
		if allJoins[te.Name] {
			tExecByName[te.Name] = te
		}
	}
	if len(tExecByName) == 0 {
		return nil
	}

	// BFS from source task along outbound transitions.
	visited := map[string]bool{}
	// Push first hop onto the work stack.
	outbound := c.wfSpec.OutboundTaskNames(taskName)
	if len(outbound) == 0 {
		return nil
	}

	var queue []string
	seen := map[string]bool{}
	for _, n := range outbound {
		if !seen[n] {
			seen[n] = true
			queue = append(queue, n)
		}
	}

	var result []string
	resultSeen := map[string]bool{}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]

		if visited[name] {
			continue // cycle guard
		}
		visited[name] = true

		// Encountered engine command (pause / fail / etc.) — not a real task.
		if c.wfSpec.GetTask(name) == nil {
			continue
		}

		// Join task that exists in DB — collect it.
		if allJoins[name] {
			if te, ok := tExecByName[name]; ok {
				if !resultSeen[te.ID] {
					resultSeen[te.ID] = true
					result = append(result, te.ID)
				}
			}
			continue // don't walk beyond a join task; it's the terminal target
		}

		// Not a join — keep walking.
		for _, next := range c.wfSpec.OutboundTaskNames(name) {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}

	return result
}

// --- ReverseWorkflowController ---

// ReverseWorkflowController implements the reverse (pull-based) workflow algorithm.
// Ported from mistral-master/mistral/workflow/reverse_workflow.py
type ReverseWorkflowController struct {
	wfEx   *db.WorkflowExecution
	wfSpec *v2.WorkflowSpec
	tasks  []*db.TaskExecution
}

func NewReverseWorkflowController(
	wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, tasks []*db.TaskExecution,
) *ReverseWorkflowController {
	return &ReverseWorkflowController{wfEx: wfEx, wfSpec: wfSpec, tasks: tasks}
}

func (c *ReverseWorkflowController) ContinueWorkflow(taskEx *db.TaskExecution) []commands.WorkflowCommand {
	if states.IsCompleted(c.wfEx.State) || states.IsPaused(c.wfEx.State) {
		return nil
	}
	return c.findNextCommands()
}

func (c *ReverseWorkflowController) findNextCommands() []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand

	for _, taskSpec := range c.wfSpec.GetTasks() {
		alreadyExists := false
		for _, te := range c.tasks {
			if te.Name == taskSpec.GetName() {
				if states.IsCompleted(te.State) || te.State == states.RUNNING {
					alreadyExists = true
					break
				}
			}
		}
		if alreadyExists {
			continue
		}

		requires := c.getEffectiveRequires(taskSpec)
		allCompleted := true
		for _, reqName := range requires {
			found := false
			for _, te := range c.tasks {
				if te.Name == reqName && states.IsCompleted(te.State) {
					found = true
					break
				}
			}
			if !found {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			ctx := c.calculateReverseInboundContext(taskSpec)
			cmds = append(cmds, commands.NewRunTask(taskSpec.GetName(), ctx, nil, false))
		}
	}

	return cmds
}

func (c *ReverseWorkflowController) getEffectiveRequires(taskSpec *v2.TaskSpec) []string {
	requires := taskSpec.GetRequires()
	if taskDefaults := c.wfSpec.GetTaskDefaults(); taskDefaults != nil {
		for _, r := range taskDefaults.GetRequires() {
			found := false
			for _, req := range requires {
				if req == r {
					found = true
					break
				}
			}
			if !found {
				requires = append(requires, r)
			}
		}
	}
	return requires
}

func (c *ReverseWorkflowController) calculateReverseInboundContext(taskSpec *v2.TaskSpec) map[string]interface{} {
	ctx := make(map[string]interface{})
	requires := c.getEffectiveRequires(taskSpec)
	for _, reqName := range requires {
		for _, te := range c.tasks {
			if te.Name == reqName && states.IsCompleted(te.State) {
				taskData := make(map[string]interface{})
				if r, ok := te.Output["result"]; ok {
					taskData["result"] = r
				}
				for k, v := range te.Published {
					taskData[k] = v
				}
				taskData["state"] = te.State
				ctx[te.Name] = taskData
				break
			}
		}
	}
	ctx["input"] = c.wfEx.Input
	for k, v := range c.wfEx.Input {
		if _, exists := ctx[k]; !exists {
			ctx[k] = v
		}
	}
	return ctx
}

func (c *ReverseWorkflowController) RerunTasks(taskExecs []*db.TaskExecution, reset bool) []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand
	for _, te := range taskExecs {
		cmds = append(cmds, commands.NewRunExistingTask(te.ID, te.Name, te.InContext, reset, true, nil, false))
	}
	return cmds
}

func (c *ReverseWorkflowController) SkipTasks(taskExecs []*db.TaskExecution) []commands.WorkflowCommand {
	var cmds []commands.WorkflowCommand
	for _, te := range taskExecs {
		cmds = append(cmds, commands.NewSkipTask(te.ID, te.Name, te.InContext, nil, false))
	}
	return cmds
}

func (c *ReverseWorkflowController) GetLogicalTaskState(taskEx *db.TaskExecution) (*TaskLogicalState, error) {
	if states.IsCompleted(taskEx.State) {
		return &TaskLogicalState{State: taskEx.State}, nil
	}

	taskSpec := c.wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return nil, fmt.Errorf("unknown task: %s", taskEx.Name)
	}

	requires := c.getEffectiveRequires(taskSpec)
	unmetCount := 0
	for _, reqName := range requires {
		found := false
		for _, te := range c.tasks {
			if te.Name == reqName && states.IsCompleted(te.State) {
				found = true
				break
			}
		}
		if !found {
			unmetCount++
		}
	}

	if unmetCount > 0 {
		return &TaskLogicalState{State: states.WAITING, Cardinality: unmetCount}, nil
	}

	return &TaskLogicalState{State: states.RUNNING}, nil
}

func (c *ReverseWorkflowController) IsErrorHandledFor(taskEx *db.TaskExecution) bool {
	return true
}

func (c *ReverseWorkflowController) AllErrorsHandled() bool {
	for _, te := range c.tasks {
		if te.State == states.ERROR {
			return false
		}
	}
	return true
}

func (c *ReverseWorkflowController) AnyCancels() bool {
	for _, te := range c.tasks {
		if te.State == states.CANCELLED {
			return true
		}
	}
	return false
}

func (c *ReverseWorkflowController) EvaluateWorkflowFinalContext() (map[string]interface{}, error) {
	return BuildFullContext(c.wfEx, c.wfSpec, c.tasks), nil
}

func (c *ReverseWorkflowController) MayCompleteWorkflow(taskEx *db.TaskExecution) bool {
	return states.IsCompleted(taskEx.State)
}

func (c *ReverseWorkflowController) FindIndirectlyAffectedTasks(taskName string) []string {
	// Reverse workflow has no join tasks. Mirror Python reverse_workflow.py:
	// find_indirectly_affected_task_executions → empty set.
	return nil
}
