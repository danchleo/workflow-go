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
	name           string
	description    string
	action         string
	workflow       string
	tags           []string
	input          map[string]interface{}
	withItems      map[string]interface{}
	publish        map[string]interface{}
	publishOnError map[string]interface{}
	publishOnSkip  map[string]interface{}
	policies       *PoliciesSpec
	target         interface{}
	keepResult     interface{}
	safeRerun      interface{}
	concurrency    int

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

	// Concurrency limit — positive integer; 0 means unlimited (Python default).
	// Mirrors mistral/lang/v2/tasks.py EXPRESSION_OR_POSITIVE_INTEGER schema.
	if v, ok := data["concurrency"]; ok {
		switch n := v.(type) {
		case float64:
			if n > 0 {
				s.concurrency = int(n)
			}
		case int:
			if n > 0 {
				s.concurrency = n
			}
		}
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

	// Policies - group policy fields. Two sources supported (mirrors Mistral):
	//   1. Direct:  task.retry: {count: 3}              ← top-level key
	//   2. Block:   task.policies: {retry: {count: 3}}  ← grouped under policies:
	policyFields := []string{"retry", "wait-before", "wait-after", "timeout", "pause-before", "concurrency", "fail-on"}
	policyMap := make(map[string]interface{})
	// Source 1: top-level keys.
	for _, f := range policyFields {
		if v, ok := data[f]; ok {
			policyMap[f] = v
		}
	}
	// Source 2: policies: block (overrides top-level for the same field).
	if v, ok := data["policies"].(map[string]interface{}); ok {
		for _, f := range policyFields {
			if fv, fok := v[f]; fok {
				policyMap[f] = fv
			}
		}
	}
	if len(policyMap) > 0 {
		s.policies = NewPoliciesSpec(policyMap)
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
// Three formats supported (mirror Python mistral/lang/v2/tasks.py _parse_with_items):
//  1. String:      "var1 in <% $.array1 %>, var2 in <% $.array2 %>"   (multi-var)
//  2. List of strs: ["var1 in <% $.array1 %>", "var2 in <% $.array2 %>"]
//  3. Map:         {item: [a, b, c]}  — var → literal item list
func parseWithItems(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	switch v := data.(type) {
	case map[string]interface{}:
		// Format 3: with-items: {var: [item1, item2]} or {var: "<% $.expr %>"}
		for varName, items := range v {
			result[varName] = items
		}
		return result
	case string:
		// Wrap single string form into the iteration below.
		parts := splitTopLevel(v, ',')
		for _, p := range parts {
			sepIdx := -1
			for i := 0; i < len(p)-3; i++ {
				if p[i:i+4] == " in " {
					sepIdx = i
					break
				}
			}
			if sepIdx < 0 {
				continue
			}
			varName := p[:sepIdx]
			expr := p[sepIdx+4:]
			result[varName] = expr
		}
	case []interface{}:
		for _, item := range v {
			if itemStr, ok := item.(string); ok {
				parts := splitTopLevel(itemStr, ',')
				for _, p := range parts {
					sepIdx := -1
					for i := 0; i < len(p)-3; i++ {
						if p[i:i+4] == " in " {
							sepIdx = i
							break
						}
					}
					if sepIdx < 0 {
						continue
					}
					varName := p[:sepIdx]
					expr := p[sepIdx+4:]
					result[varName] = expr
				}
			}
		}
	default:
		return result
	}

	return result
}

// splitTopLevel splits s by sep but respects nested expression
// delimiters (<% %>, {{ }}, [ ], { }).
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	depthAngular, depthBrace, depthBracket := 0, 0, 0
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '<':
			if i+1 < len(s) && s[i+1] == '%' {
				depthAngular++
				i++
			} else if i+1 < len(s) && s[i+1] == '{' {
				depthBrace++
				i++
			}
		case '%':
			if i+1 < len(s) && s[i+1] == '>' {
				depthAngular--
				i++
			}
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			if depthBrace > 0 || depthAngular > 0 {
				depthBrace++
			}
		case sep:
			if depthAngular == 0 && depthBrace == 0 && depthBracket == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}

// Accessor methods.

func (s *TaskSpec) GetName() string                           { return s.name }
func (s *TaskSpec) GetDescription() string                    { return s.description }
func (s *TaskSpec) GetActionName() string                     { return s.action }
func (s *TaskSpec) GetWorkflowName() string                   { return s.workflow }
func (s *TaskSpec) GetTags() []string                         { return s.tags }
func (s *TaskSpec) GetInput() map[string]interface{}          { return s.input }
func (s *TaskSpec) GetWithItems() map[string]interface{}      { return s.withItems }
func (s *TaskSpec) GetPublish() map[string]interface{}        { return s.publish }
func (s *TaskSpec) GetPublishOnError() map[string]interface{} { return s.publishOnError }
func (s *TaskSpec) GetPublishOnSkip() map[string]interface{}  { return s.publishOnSkip }
func (s *TaskSpec) GetPolicies() *PoliciesSpec                { return s.policies }
func (s *TaskSpec) GetTarget() interface{}                    { return s.target }
func (s *TaskSpec) GetKeepResult() interface{}                { return s.keepResult }
func (s *TaskSpec) GetSafeRerun() interface{}                 { return s.safeRerun }
func (s *TaskSpec) GetConcurrency() int                       { return s.concurrency }
func (s *TaskSpec) GetType() string {
	if s.workflow != "" {
		return WorkflowTaskType
	}
	return ActionTaskType
}

// Direct workflow specific.
func (s *TaskSpec) GetJoin() interface{}         { return s.join }
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
