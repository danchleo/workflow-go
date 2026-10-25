package v2

import (
	"fmt"
	"strings"
)

// OnClauseSpec represents an "on-xxx" clause in a task definition.
// Ported from mistral-master/mistral/lang/v2/on_clause.py prepare_next_clause.
//
// Python's prepare_next_clause produces triples (task_name, condition, params).
// condition (a.k.a. "when:") is optional — empty string = always run.
type OnClauseSpec struct {
	raw     map[string]interface{}
	next    []NextItem
	publish map[string]interface{}
}

// NextItem represents a single entry in an on-clause transition list.
// Mirrors Python's (task_name, condition, params) triple.
//
// Python YAML forms supported:
//
//	on-success: task1                          → Task: "task1"
//	on-success: [task1, task2]                → list
//	on-success:                                → dict with condition
//	  task1: "<% $.x == 1 %>"                 → when = "<% $.x == 1 %>"
//	on-success:                                → advanced form
//	  publish: {k: v}
//	  next:
//	    - task: task1
//	      when: "<% $.x == 1 %>"
//	      params: {foo: bar}
type NextItem struct {
	Task       string                 // next task name (may be engine command like "fail")
	When       string                 // optional YAQL expression evaluated against inbound context
	Params     map[string]interface{} // optional params passed to the next task
	WhenIsYAQL bool                   // true if When looks like a YAQL/Jinja expression
}

// NewOnClauseSpec creates an OnClauseSpec from raw YAML data.
func NewOnClauseSpec(data interface{}) (*OnClauseSpec, error) {
	if data == nil {
		return nil, nil
	}

	spec := &OnClauseSpec{}

	switch d := data.(type) {
	case map[string]interface{}:
		spec.raw = d

		// Advanced form with 'publish' and/or 'next' sub-keys.
		if v, ok := d["next"]; ok {
			spec.next = parseNextField(v)
		} else {
			// The whole map is task-name → condition mapping.
			spec.next = parseNextField(d)
		}

		if p, ok := d["publish"].(map[string]interface{}); ok {
			spec.publish = p
		} else if p, ok := d["publish"].([]interface{}); ok {
			spec.publish = map[string]interface{}{"branch": p}
		}

	case string:
		// Shorthand form: "on-success: next_task_name"
		spec.next = []NextItem{{Task: d}}

	case []interface{}:
		// List form: "on-success: [task1, task2, {task: t, when: expr}]"
		spec.next = parseNextField(d)

	default:
		return nil, fmt.Errorf("unexpected type for on-clause: %T", data)
	}

	return spec, nil
}

func parseNextField(data interface{}) []NextItem {
	if data == nil {
		return nil
	}

	switch d := data.(type) {
	case string:
		return []NextItem{{Task: d}}

	case map[string]interface{}:
		// Each key is a task name; value is either "" or a when expression.
		var result []NextItem
		for taskName, cond := range d {
			item := NextItem{Task: taskName}
			if condStr, ok := cond.(string); ok {
				item.When = condStr
				item.WhenIsYAQL = strings.Contains(condStr, "<%") || strings.Contains(condStr, "{{")
			}
			result = append(result, item)
		}
		return result

	case []interface{}:
		var result []NextItem
		for _, item := range d {
			result = append(result, parseNextItem(item)...)
		}
		return result

	default:
		return nil
	}
}

func parseNextItem(data interface{}) []NextItem {
	switch d := data.(type) {
	case string:
		return []NextItem{{Task: d}}

	case map[string]interface{}:
		// Shorthand form: {task_name: "<% when expr %>"} — Python style where
		// the ONLY key is the NEXT TASK NAME (not a known advanced field).
		if len(d) == 1 {
			for k, val := range d {
				if k == "task" || k == "when" || k == "params" || k == "name" {
					break // it's an advanced-form field, fall through
				}
				if condStr, ok := val.(string); ok {
					item := NextItem{Task: k, When: condStr}
					item.WhenIsYAQL = strings.Contains(condStr, "<%") || strings.Contains(condStr, "{{")
					return []NextItem{item}
				}
			}
		}

		// Advanced form: {task: t, when: expr, params: {...}}
		item := NextItem{}
		if v, ok := d["task"].(string); ok {
			item.Task = v
		} else if v, ok := d["name"].(string); ok { // tolerate old Go key
			item.Task = v
		}
		if v, ok := d["when"].(string); ok {
			item.When = v
			item.WhenIsYAQL = strings.Contains(v, "<%") || strings.Contains(v, "{{")
		}
		if v, ok := d["params"].(map[string]interface{}); ok {
			item.Params = v
		}
		return []NextItem{item}

	default:
		return nil
	}
}

// GetNext returns the list of next task items.
func (s *OnClauseSpec) GetNext() []NextItem {
	if s == nil {
		return nil
	}
	return s.next
}

// GetPublish returns the publish section.
func (s *OnClauseSpec) GetPublish() map[string]interface{} {
	if s == nil {
		return nil
	}
	return s.publish
}

// HasAnyWhen returns true if any next-item in this clause has a non-empty
// "when:" expression. Used to detect "empty" on-skip clauses so we can fall
// back to on-success (Python DirectWorkflowController._find_next_tasks).
func (s *OnClauseSpec) HasAnyWhen() bool {
	if s == nil {
		return false
	}
	for _, n := range s.next {
		if n.When != "" {
			return true
		}
	}
	return false
}

// NextTaskNames returns just the task names from next clauses.
func (s *OnClauseSpec) NextTaskNames() []string {
	if s == nil {
		return nil
	}
	names := make([]string, 0, len(s.next))
	for _, n := range s.next {
		names = append(names, n.Task)
	}
	return names
}
