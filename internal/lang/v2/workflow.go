package v2

import (
	"fmt"

	"github.com/mistral/workflow/internal/lang"
)

// Standard engine command names used in workflow DSL.
const (
	NoopCommand   = "noop"
	FailCommand   = "fail"
	SucceedCommand = "succeed"
	PauseCommand  = "pause"
)

// WorkflowSpec represents a workflow definition (either direct or reverse).
// Ported from mistral-master/mistral/lang/v2/workflows.py
type WorkflowSpec struct {
	name            string
	description     string
	tags            []string
	workflowType    string // "direct" or "reverse"
	input           map[string]interface{}
	output          map[string]interface{}
	outputOnError   map[string]interface{}
	vars            map[string]interface{}
	taskDefaults    *TaskDefaultsSpec
	tasks           map[string]*TaskSpec
	raw             map[string]interface{}
}

// NewWorkflowSpec creates a WorkflowSpec from raw YAML data.
func NewWorkflowSpec(name string, data map[string]interface{}) (*WorkflowSpec, error) {
	if data == nil {
		return nil, fmt.Errorf("workflow data is nil")
	}
	data["name"] = name

	s := &WorkflowSpec{
		name: name,
		raw:  data,
	}

	// Workflow type (default: direct).
	if v, ok := data["type"].(string); ok {
		s.workflowType = v
	} else {
		s.workflowType = "direct"
	}

	// Description.
	if v, ok := data["description"].(string); ok {
		s.description = v
	}

	// Tags.
	if v, ok := data["tags"].([]interface{}); ok {
		for _, t := range v {
			if tag, ok := t.(string); ok {
				s.tags = append(s.tags, tag)
			}
		}
	}

	// Input - can be a list of strings or a list of single-key dicts.
	s.input = parseInput(data["input"])

	// Output.
	if v, ok := data["output"].(map[string]interface{}); ok {
		s.output = v
	} else {
		s.output = make(map[string]interface{})
	}

	// Output on error.
	if v, ok := data["output-on-error"].(map[string]interface{}); ok {
		s.outputOnError = v
	} else {
		s.outputOnError = make(map[string]interface{})
	}

	// Vars.
	if v, ok := data["vars"].(map[string]interface{}); ok {
		s.vars = v
	} else {
		s.vars = make(map[string]interface{})
	}

	// Task defaults.
	s.taskDefaults = NewTaskDefaultsSpec(data["task-defaults"])

	// Tasks - required.
	tasksRaw, ok := data["tasks"].(map[string]interface{})
	if !ok || len(tasksRaw) == 0 {
		return nil, fmt.Errorf("workflow '%s' must define at least one task", name)
	}

	s.tasks = make(map[string]*TaskSpec, len(tasksRaw))
	for tName, tData := range tasksRaw {
		tMap, ok := tData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("task '%s' must be a mapping", tName)
		}
		s.tasks[tName] = NewTaskSpec(tName, tMap, s.workflowType)
	}

	// Validate semantics based on workflow type.
	if s.workflowType == "direct" {
		if !s.hasStartTasks() {
			return nil, fmt.Errorf(
				"direct workflow '%s' must have at least one start task (no inbound transition)", name,
			)
		}
	}

	return s, nil
}

// parseInput handles both ["var1", "var2"] and [{"var1": default}, "var2"] forms.
func parseInput(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	if data == nil {
		return result
	}

	switch v := data.(type) {
	case []interface{}:
		for _, item := range v {
			switch val := item.(type) {
			case string:
				result[val] = lang.NotDefined
			case map[string]interface{}:
				for k, v := range val {
					result[k] = v
				}
			}
		}
	case []string:
		for _, item := range v {
			result[item] = lang.NotDefined
		}
	case map[string]interface{}:
		result = v
	}

	return result
}

// Accessor methods.

func (s *WorkflowSpec) GetName() string                 { return s.name }
func (s *WorkflowSpec) GetDescription() string          { return s.description }
func (s *WorkflowSpec) GetTags() []string               { return s.tags }
func (s *WorkflowSpec) GetType() string                 { return s.workflowType }
func (s *WorkflowSpec) GetInput() map[string]interface{} { return s.input }
func (s *WorkflowSpec) GetOutput() map[string]interface{} { return s.output }
func (s *WorkflowSpec) GetOutputOnError() map[string]interface{} { return s.outputOnError }
func (s *WorkflowSpec) GetVars() map[string]interface{}  { return s.vars }
func (s *WorkflowSpec) GetTaskDefaults() *TaskDefaultsSpec { return s.taskDefaults }
func (s *WorkflowSpec) GetTasks() map[string]*TaskSpec { return s.tasks }
func (s *WorkflowSpec) GetTask(name string) *TaskSpec  { return s.tasks[name] }

// ToMap returns the raw spec as a map.
func (s *WorkflowSpec) ToMap() map[string]interface{} { return s.raw }

// StartTasks returns tasks with no inbound transitions (direct workflow).
func (s *WorkflowSpec) StartTasks() []*TaskSpec {
	var result []*TaskSpec
	for _, t := range s.tasks {
		if !s.hasInboundTransitions(t) {
			result = append(result, t)
		}
	}
	return result
}

func (s *WorkflowSpec) hasStartTasks() bool {
	for _, t := range s.tasks {
		if !s.hasInboundTransitions(t) {
			return true
		}
	}
	return false
}

func (s *WorkflowSpec) hasInboundTransitions(task *TaskSpec) bool {
	taskName := task.GetName()
	for _, t := range s.tasks {
		// Check all on-xxx clauses for transitions to this task.
		if clauses := s.getAllOutboundClauses(t); clauses != nil {
			for _, clause := range clauses {
				if clause != nil {
					for _, n := range clause.GetNext() {
						if n.Task == taskName {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func (s *WorkflowSpec) getAllOutboundClauses(t *TaskSpec) []*OnClauseSpec {
	if s.workflowType == "direct" {
		return []*OnClauseSpec{
			t.GetOnComplete(),
			t.GetOnSuccess(),
			t.GetOnError(),
			t.GetOnSkip(),
		}
	}
	return nil
}

// OutboundTaskNames returns all task names reachable from the given task name.
func (s *WorkflowSpec) OutboundTaskNames(taskName string) []string {
	var result []string
	t := s.GetTask(taskName)
	if t == nil {
		return result
	}
	for _, clause := range s.getAllOutboundClauses(t) {
		if clause != nil {
			result = append(result, clause.NextTaskNames()...)
		}
	}
	// Also check engine commands.
	for _, name := range result {
		if name == NoopCommand || name == FailCommand || name == SucceedCommand || name == PauseCommand {
			// These are not real tasks.
		}
	}
	return result
}

// InboundTaskSpecs returns all tasks that have outbound transitions to the given task.
func (s *WorkflowSpec) InboundTaskSpecs(task *TaskSpec) []*TaskSpec {
	var result []*TaskSpec
	taskName := task.GetName()

	for _, t := range s.tasks {
		if t.GetName() == taskName {
			continue
		}
		for _, clause := range s.getAllOutboundClauses(t) {
			if clause != nil {
				for _, n := range clause.GetNext() {
					if n.Task == taskName {
						result = append(result, t)
						break
					}
				}
			}
		}
	}

	return result
}

// IsEngineCommand reports whether taskName is an engine command, not a real task.
func IsEngineCommand(taskName string) bool {
	switch taskName {
	case NoopCommand, FailCommand, SucceedCommand, PauseCommand:
		return true
	}
	return false
}

// compile-time check
var _ lang.Specification = (*WorkflowSpec)(nil)
