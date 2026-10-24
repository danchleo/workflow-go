package v2

import (
	"fmt"

	"github.com/mistral/workflow/internal/lang"
)

// WorkbookSpec represents a Mistral workbook (container for actions and workflows).
// Ported from mistral-master/mistral/lang/v2/workbook.py
type WorkbookSpec struct {
	name        string
	description string
	tags        []string
	actions     map[string]map[string]interface{}
	workflows   map[string]*WorkflowSpec
	raw         map[string]interface{}
}

// NewWorkbookSpec creates a WorkbookSpec from raw YAML data.
func NewWorkbookSpec(data map[string]interface{}) (*WorkbookSpec, error) {
	if data == nil {
		return nil, fmt.Errorf("workbook data is nil")
	}

	s := &WorkbookSpec{raw: data}

	if v, ok := data["name"].(string); ok {
		s.name = v
	}
	if v, ok := data["description"].(string); ok {
		s.description = v
	}
	if v, ok := data["tags"].([]interface{}); ok {
		for _, t := range v {
			if tag, ok := t.(string); ok {
				s.tags = append(s.tags, tag)
			}
		}
	}

	// Actions section.
	if actionsRaw, ok := data["actions"].(map[string]interface{}); ok {
		s.actions = make(map[string]map[string]interface{})
		for aName, aData := range actionsRaw {
			if aMap, ok := aData.(map[string]interface{}); ok {
				s.actions[aName] = aMap
			}
		}
	}

	// Workflows section.
	if wfRaw, ok := data["workflows"].(map[string]interface{}); ok {
		s.workflows = make(map[string]*WorkflowSpec, len(wfRaw))
		for wfName, wfData := range wfRaw {
			if wfMap, ok := wfData.(map[string]interface{}); ok {
				wfSpec, err := NewWorkflowSpec(wfName, wfMap)
				if err != nil {
					return nil, fmt.Errorf("workflow '%s': %w", wfName, err)
				}
				s.workflows[wfName] = wfSpec
			}
		}
	}

	return s, nil
}

func (s *WorkbookSpec) GetName() string                       { return s.name }
func (s *WorkbookSpec) GetDescription() string                { return s.description }
func (s *WorkbookSpec) GetTags() []string                     { return s.tags }
func (s *WorkbookSpec) GetActions() map[string]map[string]interface{} { return s.actions }
func (s *WorkbookSpec) GetWorkflows() map[string]*WorkflowSpec { return s.workflows }

func (s *WorkbookSpec) ToMap() map[string]interface{} { return s.raw }

// compile-time check
var _ lang.Specification = (*WorkbookSpec)(nil)

// ActionSpec represents a custom action definition.
type ActionSpec struct {
	name        string
	description string
	actionClass string
	input       map[string]interface{}
	attributes  map[string]interface{}
	raw         map[string]interface{}
}

func NewActionSpec(name string, data map[string]interface{}) *ActionSpec {
	s := &ActionSpec{name: name, raw: data}
	if v, ok := data["description"].(string); ok {
		s.description = v
	}
	if v, ok := data["class"].(string); ok {
		s.actionClass = v
	}
	if v, ok := data["input"].(map[string]interface{}); ok {
		s.input = v
	}
	if v, ok := data["attributes"].(map[string]interface{}); ok {
		s.attributes = v
	}
	return s
}

func (s *ActionSpec) GetName() string         { return s.name }
func (s *ActionSpec) GetDescription() string  { return s.description }
func (s *ActionSpec) GetActionClass() string  { return s.actionClass }
func (s *ActionSpec) GetInput() map[string]interface{} { return s.input }
func (s *ActionSpec) GetAttributes() map[string]interface{} { return s.attributes }
func (s *ActionSpec) ToMap() map[string]interface{} { return s.raw }
