// Package services contains high-level service functions used by the engine and API.
// Ported from mistral-master/mistral/services/
package services

import (
	"fmt"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/lang/parser"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"gopkg.in/yaml.v3"
)

// WorkflowService handles workflow definition management.
type WorkflowService struct {
	store db.Store
}

// NewWorkflowService creates a WorkflowService.
func NewWorkflowService(store db.Store) *WorkflowService {
	return &WorkflowService{store: store}
}

// CreateWorkflow parses and saves a workflow definition.
func (s *WorkflowService) CreateWorkflow(name string, definition string, projectID, namespace string) (*db.WorkflowDefinition, error) {
	data, err := parser.ParseYAML(definition)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}

	// Get workflow spec.
	wfSpec, err := v2.NewWorkflowSpec(name, data)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow spec: %w", err)
	}
	_ = wfSpec // Used to validate, not stored directly.

	// Check for duplicate.
	existing, err := s.store.WorkflowDefinitions().GetByName(name, namespace, projectID)
	if err == nil && existing != nil {
		return nil, &db.ConflictError{Entity: "WorkflowDefinition", Field: "name", Value: name}
	}

	wfDef := &db.WorkflowDefinition{
		Name:       name,
		Namespace:  namespace,
		Definition: definition,
		Spec:       data,
		ProjectID:  projectID,
	}

	if err := s.store.WorkflowDefinitions().Create(wfDef); err != nil {
		return nil, fmt.Errorf("creating workflow: %w", err)
	}

	return wfDef, nil
}

// UpdateWorkflow updates an existing workflow definition.
func (s *WorkflowService) UpdateWorkflow(wfID string, definition string) (*db.WorkflowDefinition, error) {
	wfDef, err := s.store.WorkflowDefinitions().Get(wfID)
	if err != nil {
		return nil, err
	}

	data, err := parser.ParseYAML(definition)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}

	wfDef.Definition = definition
	wfDef.Spec = data

	if err := s.store.WorkflowDefinitions().Update(wfDef); err != nil {
		return nil, err
	}

	return wfDef, nil
}

// DeleteWorkflow deletes a workflow definition.
func (s *WorkflowService) DeleteWorkflow(wfID string) error {
	return s.store.WorkflowDefinitions().Delete(wfID)
}

// GetWorkflow returns a workflow definition by ID.
func (s *WorkflowService) GetWorkflow(wfID string) (*db.WorkflowDefinition, error) {
	return s.store.WorkflowDefinitions().Get(wfID)
}

// ListWorkflows lists workflow definitions.
func (s *WorkflowService) ListWorkflows(namespace, projectID string) ([]*db.WorkflowDefinition, error) {
	return s.store.WorkflowDefinitions().List(namespace, projectID)
}

// WorkbookService handles workbook management.
type WorkbookService struct {
	store db.Store
}

// NewWorkbookService creates a WorkbookService.
func NewWorkbookService(store db.Store) *WorkbookService {
	return &WorkbookService{store: store}
}

// CreateWorkbook parses and saves a workbook, creating all contained
// workflow definitions as separate DB records. Mirrors Python
// mistral.services.workbooks._create_or_update_workflows: each workflow
// is registered as "<wb_name>.<wf_name>" with WorkbookName stamped.
func (s *WorkbookService) CreateWorkbook(name string, definition string, projectID, namespace string) (*db.Workbook, error) {
	data, err := parser.ParseYAML(definition)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}

	wbSpec, err := v2.NewWorkbookSpec(data)
	if err != nil {
		return nil, fmt.Errorf("invalid workbook spec: %w", err)
	}

	wb := &db.Workbook{
		Name:       name,
		Namespace:  namespace,
		Definition: definition,
		ProjectID:  projectID,
	}
	if err := s.store.Workbooks().Create(wb); err != nil {
		return nil, fmt.Errorf("creating workbook: %w", err)
	}

	wbFlows := wbSpec.GetWorkflows()
	for wfName, wfSpec := range wbFlows {
		// Python版: wf_name = '%s.%s' % (wb_db.name, wf_spec.get_name())
		fullName := name + "." + wfName

		wfMap := wfSpec.ToMap()

		wfDef := &db.WorkflowDefinition{
			Name:         fullName,
			Namespace:    namespace,
			Definition:   serializeWf(wfMap),
			Spec:         wfMap,
			WorkbookName: name,
			ProjectID:    projectID,
		}

		existing, _ := s.store.WorkflowDefinitions().GetByName(fullName, namespace, projectID)
		if existing != nil {
			continue
		}
		if err := s.store.WorkflowDefinitions().Create(wfDef); err != nil {
			return nil, fmt.Errorf("creating workflow '%s' in workbook: %w", fullName, err)
		}
	}

	return wb, nil
}

// serializeWf re-serializes a workflow spec map to YAML so that each stored
// workflow definition is self-contained (Python stores extractable slices).
func serializeWf(m map[string]interface{}) string {
	if m == nil {
		return ""
	}
	b, _ := yaml.Marshal(m)
	return string(b)
}
