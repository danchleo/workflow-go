// Package commands defines workflow command types that form the protocol
// between workflow controllers and their clients (engine, dispatcher, etc).
// Ported from mistral-master/mistral/workflow/commands.py
package commands

import (
	"github.com/mistral/workflow/internal/states"
)

// WorkflowCommand is the base interface for all workflow commands.
// A set of workflow commands form a communication protocol between workflow
// controller and its clients.
type WorkflowCommand interface {
	// TaskName returns the target task name.
	TaskName() string
	// Ctx returns the command context.
	Ctx() map[string]interface{}
	// TriggeredBy returns trigger information.
	TriggeredBy() []map[string]interface{}
	// HandlesError reports whether this command handles an error.
	HandlesError() bool
	// ToDict serializes the command for backlog persistence.
	ToDict() map[string]interface{}
}

// BaseCommand contains common fields for all commands.
type BaseCommand struct {
	taskSpecName string
	ctx          map[string]interface{}
	triggeredBy  []map[string]interface{}
	handlesError bool
}

func (b *BaseCommand) TaskName() string                    { return b.taskSpecName }
func (b *BaseCommand) Ctx() map[string]interface{}         { return b.ctx }
func (b *BaseCommand) TriggeredBy() []map[string]interface{} { return b.triggeredBy }
func (b *BaseCommand) HandlesError() bool                  { return b.handlesError }

// Noop is a no-operation command.
type Noop struct {
	BaseCommand
}

func NewNoop(taskName string, ctx map[string]interface{}) *Noop {
	return &Noop{BaseCommand: BaseCommand{taskSpecName: taskName, ctx: ctx}}
}

func (c *Noop) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"cmd_name":  "noop",
		"task_name": c.taskSpecName,
		"ctx":       c.ctx,
	}
}

// RunTask instructs the engine to run a new workflow task.
type RunTask struct {
	BaseCommand
	wait      bool
	uniqueKey string
}

func NewRunTask(taskName string, ctx map[string]interface{}, triggeredBy []map[string]interface{}, handlesError bool) *RunTask {
	return &RunTask{
		BaseCommand: BaseCommand{
			taskSpecName: taskName,
			ctx:          ctx,
			triggeredBy:  triggeredBy,
			handlesError: handlesError,
		},
	}
}

func (c *RunTask) IsWaiting() bool   { return c.wait }
func (c *RunTask) UniqueKey() string { return c.uniqueKey }
func (c *RunTask) SetWait(v bool)    { c.wait = v }
func (c *RunTask) SetUniqueKey(k string) { c.uniqueKey = k }

func (c *RunTask) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"cmd_name":   c.taskSpecName,
		"task_name":  c.taskSpecName,
		"ctx":        c.ctx,
		"triggered_by": c.triggeredBy,
		"wait":       c.wait,
		"unique_key": c.uniqueKey,
	}
}

// RunExistingTask runs an existing (previously created) task execution.
type RunExistingTask struct {
	BaseCommand
	taskExID  string
	reset     bool
	uniqueKey string
	rerun     bool
}

func NewRunExistingTask(taskExID, taskName string, ctx map[string]interface{}, reset bool, rerun bool, triggeredBy []map[string]interface{}, handlesError bool) *RunExistingTask {
	return &RunExistingTask{
		BaseCommand: BaseCommand{
			taskSpecName: taskName,
			ctx:          ctx,
			triggeredBy:  triggeredBy,
			handlesError: handlesError,
		},
		taskExID:  taskExID,
		reset:     reset,
		rerun:     rerun,
	}
}

func (c *RunExistingTask) TaskExID() string { return c.taskExID }
func (c *RunExistingTask) Reset() bool      { return c.reset }
func (c *RunExistingTask) Rerun() bool      { return c.rerun }

func (c *RunExistingTask) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"cmd_name":   "run_existing_task",
		"task_name":  c.taskSpecName,
		"task_ex_id": c.taskExID,
		"ctx":        c.ctx,
		"triggered_by": c.triggeredBy,
		"reset":      c.reset,
	}
}

// SkipTask skips an existing task execution.
type SkipTask struct {
	BaseCommand
	taskExID  string
	uniqueKey string
}

func NewSkipTask(taskExID, taskName string, ctx map[string]interface{}, triggeredBy []map[string]interface{}, handlesError bool) *SkipTask {
	return &SkipTask{
		BaseCommand: BaseCommand{
			taskSpecName: taskName,
			ctx:          ctx,
			triggeredBy:  triggeredBy,
			handlesError: handlesError,
		},
		taskExID: taskExID,
	}
}

func (c *SkipTask) TaskExID() string { return c.taskExID }

func (c *SkipTask) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"cmd_name":  "skip_task",
		"task_name": c.taskSpecName,
		"task_ex_id": c.taskExID,
		"ctx":       c.ctx,
		"triggered_by": c.triggeredBy,
	}
}

// SetWorkflowState is the base for commands that change workflow state.
type SetWorkflowState struct {
	BaseCommand
	newState string
	msg      string
}

func (c *SetWorkflowState) NewState() string { return c.newState }
func (c *SetWorkflowState) Msg() string       { return c.msg }

func (c *SetWorkflowState) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"task_name":    c.taskSpecName,
		"ctx":          c.ctx,
		"triggered_by": c.triggeredBy,
		"new_state":    c.newState,
		"msg":          c.msg,
	}
}

// FailWorkflow fails a workflow.
type FailWorkflow struct{ SetWorkflowState }

func NewFailWorkflow(taskName string, ctx map[string]interface{}, msg string, triggeredBy []map[string]interface{}, handlesError bool) *FailWorkflow {
	return &FailWorkflow{SetWorkflowState{
		BaseCommand: BaseCommand{taskSpecName: taskName, ctx: ctx, triggeredBy: triggeredBy, handlesError: handlesError},
		newState:    states.ERROR,
		msg:         msg,
	}}
}

func (c *FailWorkflow) ToDict() map[string]interface{} {
	d := c.SetWorkflowState.ToDict()
	d["cmd_name"] = "fail"
	return d
}

// SucceedWorkflow succeeds a workflow.
type SucceedWorkflow struct{ SetWorkflowState }

func NewSucceedWorkflow(taskName string, ctx map[string]interface{}, msg string, triggeredBy []map[string]interface{}, handlesError bool) *SucceedWorkflow {
	return &SucceedWorkflow{SetWorkflowState{
		BaseCommand: BaseCommand{taskSpecName: taskName, ctx: ctx, triggeredBy: triggeredBy, handlesError: handlesError},
		newState:    states.SUCCESS,
		msg:         msg,
	}}
}

func (c *SucceedWorkflow) ToDict() map[string]interface{} {
	d := c.SetWorkflowState.ToDict()
	d["cmd_name"] = "succeed"
	return d
}

// PauseWorkflow pauses a workflow.
type PauseWorkflow struct{ SetWorkflowState }

func NewPauseWorkflow(taskName string, ctx map[string]interface{}, msg string, triggeredBy []map[string]interface{}, handlesError bool) *PauseWorkflow {
	return &PauseWorkflow{SetWorkflowState{
		BaseCommand: BaseCommand{taskSpecName: taskName, ctx: ctx, triggeredBy: triggeredBy, handlesError: handlesError},
		newState:    states.PAUSED,
		msg:         msg,
	}}
}

func (c *PauseWorkflow) ToDict() map[string]interface{} {
	d := c.SetWorkflowState.ToDict()
	d["cmd_name"] = "pause"
	return d
}

// IsEngineCommand reports whether cmd is an engine-level command (SetWorkflowState or Noop).
func IsEngineCommand(cmd WorkflowCommand) bool {
	switch cmd.(type) {
	case *SetWorkflowState, *FailWorkflow, *SucceedWorkflow, *PauseWorkflow, *Noop:
		return true
	default:
		return false
	}
}

// IsPauseCommand reports whether cmd is a PauseWorkflow.
func IsPauseCommand(cmd WorkflowCommand) bool {
	_, ok := cmd.(*PauseWorkflow)
	return ok
}

// IsRunTaskCommand reports whether cmd is a RunTask (new task).
func IsRunTaskCommand(cmd WorkflowCommand) bool {
	_, ok := cmd.(*RunTask)
	return ok
}

// IsRunExistingTaskCommand reports whether cmd is a RunExistingTask.
func IsRunExistingTaskCommand(cmd WorkflowCommand) bool {
	_, ok := cmd.(*RunExistingTask)
	return ok
}
