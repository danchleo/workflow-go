package services

import (
	"strings"
	"testing"

	"github.com/mistral/workflow/internal/db"
)

// ------------------------------------------------------------
// WorkflowService tests
// ------------------------------------------------------------

const validDirectWf = `
version: "2.0"
name: test_wf

input:
  - greeting: "Hi"

tasks:
  step1:
    action: std.echo
    input:
      output: "<% $.greeting %> there"
    on-success:
      - step2

  step2:
    action: std.echo
    input:
      output: "done"
`

const reverseWf = `
version: "2.0"
name: reverse_wf

tasks:
  end:
    action: std.echo
    requires: [step1, step2]

  step1:
    action: std.echo
    input:
      output: "s1"

  step2:
    action: std.echo
    input:
      output: "s2"
`

const invalidYAML = `
not: valid: yaml: {
  broken
`

func TestWorkflowService_Create_OK(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("test_wf", validDirectWf, "proj1", "ns1")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	if wfDef.Name != "test_wf" {
		t.Errorf("name = %s, want test_wf", wfDef.Name)
	}
	if wfDef.ProjectID != "proj1" {
		t.Errorf("projectID = %s, want proj1", wfDef.ProjectID)
	}
	if wfDef.Namespace != "ns1" {
		t.Errorf("namespace = %s, want ns1", wfDef.Namespace)
	}
	if wfDef.ID == "" {
		t.Error("ID should be populated")
	}
}

func TestWorkflowService_Create_InvalidYAML(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	_, err := svc.CreateWorkflow("bad", invalidYAML, "", "")
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
	if !strings.Contains(err.Error(), "invalid YAML") {
		t.Errorf("error should mention 'invalid YAML': %v", err)
	}
}

func TestWorkflowService_Create_DuplicateAcrossNamespaces(t *testing.T) {
	// Same workflow name in different namespaces should NOT conflict.
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	_, err := svc.CreateWorkflow("shared", validDirectWf, "", "nsA")
	if err != nil {
		t.Fatalf("nsA create: %v", err)
	}
	_, err = svc.CreateWorkflow("shared", validDirectWf, "", "nsB")
	if err != nil {
		t.Fatalf("nsB create should succeed (different namespace): %v", err)
	}

	// Same namespace still conflicts.
	_, err = svc.CreateWorkflow("shared", validDirectWf, "", "nsA")
	if err == nil {
		t.Error("expected ConflictError for duplicate in same namespace")
	}
}

func TestWorkflowService_Create_Duplicate(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	_, err := svc.CreateWorkflow("dup", validDirectWf, "", "")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = svc.CreateWorkflow("dup", validDirectWf, "", "")
	if err == nil {
		t.Fatal("expected ConflictError on duplicate")
	}
	ce, ok := err.(*db.ConflictError)
	if !ok {
		t.Errorf("expected *db.ConflictError, got %T", err)
	} else if ce.Entity != "WorkflowDefinition" {
		t.Errorf("ConflictError.Entity = %s", ce.Entity)
	}
}

func TestWorkflowService_Update_OK(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("upd", validDirectWf, "", "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	updated, err := svc.UpdateWorkflow(wfDef.ID, reverseWf)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Definition != reverseWf {
		t.Error("definition not updated")
	}
}

func TestWorkflowService_Update_NotFound(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	_, err := svc.UpdateWorkflow("nonexistent-id", validDirectWf)
	if err == nil {
		t.Error("expected error for missing workflow")
	}
}

func TestWorkflowService_Update_InvalidYAML(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	wfDef, _ := svc.CreateWorkflow("bad_upd", validDirectWf, "", "")
	_, err := svc.UpdateWorkflow(wfDef.ID, invalidYAML)
	if err == nil {
		t.Error("expected error for invalid YAML update")
	}
}

func TestWorkflowService_Delete_OK(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	wfDef, _ := svc.CreateWorkflow("delme", validDirectWf, "", "")
	if err := svc.DeleteWorkflow(wfDef.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := svc.GetWorkflow(wfDef.ID)
	if err == nil {
		t.Error("workflow should no longer exist")
	}
}

func TestWorkflowService_GetWorkflow(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	wfDef, _ := svc.CreateWorkflow("getme", validDirectWf, "", "")
	got, err := svc.GetWorkflow(wfDef.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "getme" {
		t.Errorf("got name = %s", got.Name)
	}
}

func TestWorkflowService_ListWorkflows(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkflowService(store)

	svc.CreateWorkflow("wf1", validDirectWf, "projA", "")
	svc.CreateWorkflow("wf2", reverseWf, "projA", "")
	svc.CreateWorkflow("wf3", validDirectWf, "projB", "")

	all, err := svc.ListWorkflows("", "")
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("total = %d, want 3", len(all))
	}

	proja, _ := svc.ListWorkflows("", "projA")
	if len(proja) != 2 {
		t.Errorf("projA count = %d, want 2", len(proja))
	}
}

// ------------------------------------------------------------
// WorkbookService tests
// ------------------------------------------------------------

const workbookYAML = `
version: "2.0"

workflows:
  my_first_wf:
    tasks:
      a1:
        action: std.echo
        input:
          output: "from-first"

  my_second_wf:
    input:
      - name
    tasks:
      a2:
        action: std.echo
        input:
          output: "<% $.name %> from-second"
`

const workbookNestedYAML = `
version: "2.0"

workflows:
  parent_wf:
    tasks:
      call_child:
        workflow: child_wf
        input:
          greeting: "hi"

  child_wf:
    input:
      - greeting
    tasks:
      echo_child:
        action: std.echo
        input:
          output: "<% $.greeting %> child"
`

const invalidWorkbook = `
version: "2.0"
  bad: indentation
yaml: error: here
`

func TestWorkbookService_Create_OK(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkbookService(store)

	wb, err := svc.CreateWorkbook("my_wb", workbookYAML, "p1", "")
	if err != nil {
		t.Fatalf("CreateWorkbook: %v", err)
	}
	if wb.Name != "my_wb" {
		t.Errorf("wb name = %s", wb.Name)
	}
	if wb.ID == "" {
		t.Error("wb ID should be populated")
	}

	// Two workflows should be auto-created with "<wb_name>.<wf_name>" naming.
	wf1, err := store.WorkflowDefinitions().GetByName("my_wb.my_first_wf", "", "p1")
	if err != nil || wf1 == nil {
		t.Fatalf("my_wb.my_first_wf not found: %v", err)
	}
	if wf1.WorkbookName != "my_wb" {
		t.Errorf("wf1.WorkbookName = %s, want my_wb", wf1.WorkbookName)
	}

	wf2, err := store.WorkflowDefinitions().GetByName("my_wb.my_second_wf", "", "p1")
	if err != nil || wf2 == nil {
		t.Fatalf("my_wb.my_second_wf not found: %v", err)
	}
}

func TestWorkbookService_Create_NestedWorkflow(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkbookService(store)

	_, err := svc.CreateWorkbook("nested", workbookNestedYAML, "", "")
	if err != nil {
		t.Fatalf("CreateWorkbook nested: %v", err)
	}

	// Both parent_wf and child_wf are registered under "nested." prefix.
	for _, name := range []string{"parent_wf", "child_wf"} {
		full := "nested." + name
		wf, err := store.WorkflowDefinitions().GetByName(full, "", "")
		if err != nil || wf == nil {
			t.Errorf("%s not registered: err=%v", full, err)
		}
	}
}

func TestWorkbookService_Create_InvalidYAML(t *testing.T) {
	store := db.NewInMemoryStore()
	svc := NewWorkbookService(store)

	_, err := svc.CreateWorkbook("bad_wb", invalidWorkbook, "", "")
	if err == nil {
		t.Error("expected error for invalid workbook YAML")
	}
}

func TestWorkbookService_Create_DedupOnRetry(t *testing.T) {
	// CreateWorkbook is idempotent for workflows: re-registering the same
	// workbook should silently skip already-existing workflows.
	store := db.NewInMemoryStore()
	svc := NewWorkbookService(store)

	if _, err := svc.CreateWorkbook("dup_wb", workbookYAML, "", ""); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := svc.CreateWorkbook("dup_wb", workbookYAML, "", ""); err != nil {
		t.Fatalf("second (idempotent): %v", err)
	}

	wfs, _ := store.WorkflowDefinitions().List("", "")
	got := 0
	for _, w := range wfs {
		if strings.HasPrefix(w.Name, "dup_wb.") {
			got++
		}
	}
	if got != 2 {
		t.Errorf("dup_wb workflows count = %d, want 2 (no duplicates)", got)
	}
}
