// Package workflow provides workflow control logic — the algorithms that
// decide which tasks to run next based on the current state of task executions.
//
// Ported from mistral-master/mistral/workflow/
package workflow

import (
	"fmt"

	"github.com/mistral/workflow/internal/commands"
	"github.com/mistral/workflow/internal/db"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"github.com/mistral/workflow/internal/states"
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
func BuildFullContext(wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, taskExecs []*db.TaskExecution) map[string]interface{} {
	ctx := make(map[string]interface{})

	// Task results nested under task names. Also flatten published vars to top level.
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
			for k, v := range te.Published {
				if _, exists := taskData[k]; !exists {
					taskData[k] = v
				}
				// Also flatten to top level. Later tasks can overwrite earlier ones.
				ctx[k] = v
			}
		}

		taskData["state"] = te.State
		taskData["id"] = te.ID
		ctx[te.Name] = taskData
	}

	// Workflow context vars.
	for k, v := range wfEx.Context {
		ctx[k] = v
	}

	// Input.
	ctx["input"] = wfEx.Input

	// Env.
	if env, ok := wfEx.Params["env"].(map[string]interface{}); ok {
		ctx["env"] = env
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

// --- DirectWorkflowController ---

// DirectWorkflowController implements the direct workflow algorithm.
// Ported from mistral-master/mistral/workflow/direct_workflow.py
type DirectWorkflowController struct {
	wfEx   *db.WorkflowExecution
	wfSpec *v2.WorkflowSpec
	tasks  []*db.TaskExecution // All task executions for this workflow
}

// NewDirectWorkflowController creates a new direct workflow controller.
func NewDirectWorkflowController(wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, tasks []*db.TaskExecution) *DirectWorkflowController {
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

	// Task completion: find outbound transitions.
	taskSpec := c.wfSpec.GetTask(taskEx.Name)
	if taskSpec == nil {
		return nil
	}

	// Determine which on-clause to use based on the completed task state.
	var outboundClause *v2.OnClauseSpec
	switch taskEx.State {
	case states.SUCCESS:
		outboundClause = taskSpec.GetOnSuccess()
	case states.ERROR:
		outboundClause = taskSpec.GetOnError()
	case states.SKIPPED:
		outboundClause = taskSpec.GetOnSkip()
	default:
		outboundClause = taskSpec.GetOnComplete()
	}

	if outboundClause == nil {
		outboundClause = taskSpec.GetOnComplete()
	}

	if outboundClause == nil {
		return nil
	}

	// Generate commands for each outbound transition.
	for _, next := range outboundClause.GetNext() {
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

		triggeredBy := []map[string]interface{}{{
			"task_id":   taskEx.ID,
			"task_name": taskEx.Name,
			"event":     taskEx.State,
		}}

		nextTaskSpec := c.wfSpec.GetTask(next.Task)
		if nextTaskSpec == nil {
			continue
		}

		// Check join logic.
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
		handlesError := taskEx.State == states.ERROR
		cmds = append(cmds, commands.NewRunTask(next.Task, ctx, triggeredBy, handlesError))
	}

	return cmds
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
func (c *DirectWorkflowController) calculateInboundContext(taskSpec *v2.TaskSpec, triggerTask *db.TaskExecution) map[string]interface{} {
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
	inboundExecs := c.filterCompletedTasksForSpecs(inboundSpecs)

	joinVal := taskSpec.GetJoin()
	cardinality := len(inboundSpecs) - len(inboundExecs)

	joinRequirement := 1
	if joinVal != nil {
		switch jv := joinVal.(type) {
		case string:
			if jv == "all" {
				joinRequirement = len(inboundSpecs)
			} else if jv == "one" {
				joinRequirement = 1
			}
		case float64:
			joinRequirement = int(jv)
		case int:
			joinRequirement = jv
		}
	}

	if len(inboundExecs) < joinRequirement {
		return &TaskLogicalState{State: states.WAITING, Cardinality: cardinality}, nil
	}

	return &TaskLogicalState{State: states.RUNNING}, nil
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
	return states.IsCompleted(taskEx.State)
}

// --- ReverseWorkflowController ---

// ReverseWorkflowController implements the reverse (pull-based) workflow algorithm.
// Ported from mistral-master/mistral/workflow/reverse_workflow.py
type ReverseWorkflowController struct {
	wfEx   *db.WorkflowExecution
	wfSpec *v2.WorkflowSpec
	tasks  []*db.TaskExecution
}

func NewReverseWorkflowController(wfEx *db.WorkflowExecution, wfSpec *v2.WorkflowSpec, tasks []*db.TaskExecution) *ReverseWorkflowController {
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
