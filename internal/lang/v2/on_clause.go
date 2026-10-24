package v2

import (
	"fmt"
)

// OnClauseSpec represents an "on-xxx" clause in a task definition.
// Ported from mistral-master/mistral/lang/v2/on_clause.py
type OnClauseSpec struct {
	raw     map[string]interface{}
	next    []NextItem
	publish map[string]interface{}
}

// NextItem represents a single entry in an on-clause transition list.
type NextItem struct {
	Task   string
	Params map[string]interface{}
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

		// Parse 'next' field.
		spec.next = parseNextField(d["next"])

		// Parse 'publish' field.
		if p, ok := d["publish"].(map[string]interface{}); ok {
			spec.publish = p
		} else if p, ok := d["publish"].([]interface{}); ok {
			spec.publish = map[string]interface{}{"branch": p}
		}

	case string:
		// Shorthand form: "on-success: next_task_name"
		spec.next = []NextItem{{Task: d}}

	case []interface{}:
		// List form: "on-success: [task1, task2]"
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
		// "next: task_name" or "next: <% $.expr %>"
		return []NextItem{{Task: d}}

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
		taskName, _ := d["name"].(string)
		params, _ := d["params"].(map[string]interface{})
		return []NextItem{{Task: taskName, Params: params}}

	default:
		return nil
	}
}

// GetNext returns the list of next task names (without params).
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
