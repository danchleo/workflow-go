package v2

import (
	"github.com/mistral/workflow/internal/lang"
)

// Task type constants.
const (
	ActionTaskType   = "ACTION"
	WorkflowTaskType = "WORKFLOW"
)

// TaskSpec represents a single task in a workflow.
// Ported from mistral-master/mistral/lang/v2/tasks.py
type TaskSpec struct {
	name        string
	description string
	action      string
	workflow    string
	tags        []string
	input       map[string]interface{}
	withItems   map[string]interface{}
	publish     map[string]interface{}
	publishOnError map[string]interface{}
	publishOnSkip  map[string]interface{}
	policies    *PoliciesSpec
	target      interface{}
	keepResult  interface{}
	safeRerun   interface{}

	// Direct workflow specific.
	join       interface{}
	onComplete *OnClauseSpec
	onSuccess  *OnClauseSpec
	onError    *OnClauseSpec
	onSkip     *OnClauseSpec

	// Reverse workflow specific.
	requires []string

	// Raw data for serialization.
	raw map[string]interface{}
}

// NewTaskSpec creates a TaskSpec from raw YAML data.
// taskType is "direct" or "reverse", injected by WorkflowSpec.
func NewTaskSpec(name string, data map[string]interface{}, taskType string) *TaskSpec {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["name"] = name

	s := &TaskSpec{
		name: name,
		raw:  data,
	}

	// Description.
	if v, ok := data["description"].(string); ok {
		s.description = v
	}

	// Action or workflow.
	if v, ok := data["action"].(string); ok {
		s.action = v
	}
	if v, ok := data["workflow"].(string); ok {
		s.workflow = v
	}

	// Default to std.noop if neither action nor workflow specified.
	if s.action == "" && s.workflow == "" {
		s.action = "std.noop"
	}

	// Tags.
	if v, ok := data["tags"].([]interface{}); ok {
		for _, t := range v {
			if tag, ok := t.(string); ok {
				s.tags = append(s.tags, tag)
			}
		}
	}

	// Input.
	if v, ok := data["input"].(map[string]interface{}); ok {
		s.input = v
	} else {
		s.input = make(map[string]interface{})
	}

	// With-items.
	if v, ok := data["with-items"]; ok {
		s.withItems = parseWithItems(v)
	}

	// Publish.
	if v, ok := data["publish"].(map[string]interface{}); ok {
		s.publish = v
	}
	if v, ok := data["publish-on-error"].(map[string]interface{}); ok {
		s.publishOnError = v
	}
	if v, ok := data["publish-on-skip"].(map[string]interface{}); ok {
		s.publishOnSkip = v
	}

	// Policies - group policy fields.
	policyFields := []string{"retry", "wait-before", "wait-after", "timeout", "pause-before", "concurrency", "fail-on"}
	policyMap := make(map[string]interface{})
	for _, f := range policyFields {
		if v, ok := data[f]; ok {
			policyMap[f] = v
		}
	}
	if len(policyMap) > 0 {
		s.policies = NewPoliciesSpec(policyMap)
	} else {
		s.policies = NewPoliciesSpec(make(map[string]interface{}))
	}

	// Target.
	if v, ok := data["target"]; ok {
		s.target = v
	}

	// Keep-result (defaults to true).
	if v, ok := data["keep-result"]; ok {
		s.keepResult = v
	} else {
		s.keepResult = true
	}

	// Safe-rerun.
	if v, ok := data["safe-rerun"]; ok {
		s.safeRerun = v
	}

	// Type-specific fields.
	switch taskType {
	case "direct":
		// Join.
		if v, ok := data["join"]; ok {
			s.join = v
		}
		// On clauses.
		s.onComplete, _ = NewOnClauseSpec(data["on-complete"])
		s.onSuccess, _ = NewOnClauseSpec(data["on-success"])
		s.onError, _ = NewOnClauseSpec(data["on-error"])
		s.onSkip, _ = NewOnClauseSpec(data["on-skip"])

	case "reverse":
		// Requires.
		if v, ok := data["requires"]; ok {
			switch val := v.(type) {
			case string:
				s.requires = []string{val}
			case []interface{}:
				for _, item := range val {
					if itemStr, ok := item.(string); ok {
						s.requires = append(s.requires, itemStr)
					}
				}
			}
		}
	}

	return s
}

// parseWithItems parses the 'with-items' expression.
// Format: var1 in <% $.array1 %>, var2 in [item1, item2, ...]
func parseWithItems(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	var rawItems []interface{}
	switch v := data.(type) {
	case string:
		rawItems = []interface{}{v}
	case []interface{}:
		rawItems = v
	default:
		return result
	}

	for _, item := range rawItems {
		itemStr, ok := item.(string)
		if !ok {
			continue
		}
		// Parse "var in expression" format.
		// Find " in " separator.
		sepIdx := -1
		for i := 0; i < len(itemStr)-3; i++ {
			if itemStr[i:i+4] == " in " {
				sepIdx = i
				break
			}
		}
		if sepIdx < 0 {
			continue
		}
		varName := itemStr[:sepIdx]
		expr := itemStr[sepIdx+4:]
		result[varName] = expr
	}

	return result
}

// Accessor methods.

func (s *TaskSpec) GetName() string         { return s.name }
func (s *TaskSpec) GetDescription() string  { return s.description }
func (s *TaskSpec) GetActionName() string   { return s.action }
func (s *TaskSpec) GetWorkflowName() string { return s.workflow }
func (s *TaskSpec) GetTags() []string       { return s.tags }
func (s *TaskSpec) GetInput() map[string]interface{} { return s.input }
func (s *TaskSpec) GetWithItems() map[string]interface{} { return s.withItems }
func (s *TaskSpec) GetPublish() map[string]interface{}  { return s.publish }
func (s *TaskSpec) GetPublishOnError() map[string]interface{} { return s.publishOnError }
func (s *TaskSpec) GetPublishOnSkip() map[string]interface{} { return s.publishOnSkip }
func (s *TaskSpec) GetPolicies() *PoliciesSpec { return s.policies }
func (s *TaskSpec) GetTarget() interface{}    { return s.target }
func (s *TaskSpec) GetKeepResult() interface{} { return s.keepResult }
func (s *TaskSpec) GetSafeRerun() interface{}  { return s.safeRerun }
func (s *TaskSpec) GetType() string {
	if s.workflow != "" {
		return WorkflowTaskType
	}
	return ActionTaskType
}

// Direct workflow specific.
func (s *TaskSpec) GetJoin() interface{}      { return s.join }
func (s *TaskSpec) GetOnComplete() *OnClauseSpec { return s.onComplete }
func (s *TaskSpec) GetOnSuccess() *OnClauseSpec  { return s.onSuccess }
func (s *TaskSpec) GetOnError() *OnClauseSpec    { return s.onError }
func (s *TaskSpec) GetOnSkip() *OnClauseSpec     { return s.onSkip }

// Reverse workflow specific.
func (s *TaskSpec) GetRequires() []string { return s.requires }

// ToMap returns the raw spec as a map (can be used for serialization).
func (s *TaskSpec) ToMap() map[string]interface{} {
	return s.raw
}

// GetTimeout returns the timeout policy value.
func (s *TaskSpec) GetTimeout() interface{} {
	if s.policies != nil {
		return s.policies.GetTimeout()
	}
	return nil
}

// compile-time check
var _ lang.Specification = (*TaskSpec)(nil)
