package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/mistral/workflow/internal/db"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"github.com/mistral/workflow/internal/services"
	"github.com/mistral/workflow/internal/states"
)

const directWFYAML = `
version: "2.0"
name: e2e_test

input:
  - greeting: "Hello"
  - name: "World"

tasks:
  a:
    action: std.echo
    input:
      output: "result-a"
    on-success:
      - b

  b:
    action: std.echo
    input:
      output: "result-b"
`

const reverseWFYAML = `
version: "2.0"
name: e2e_reverse

tasks:
  x:
    action: std.echo
  y:
    action: std.echo
    requires: [x]
  z:
    action: std.echo
    requires: [y]
`

const withItemsWFYAML = `
version: "2.0"
name: with_items_test

input:
  - items: ["alpha", "beta", "gamma"]

tasks:
  fanout:
    action: std.echo
    with-items: item in <% $.input.items %>
    input:
      output: "<% $.item %>"
    publish:
      result: "<% $.fanout.result %>"
    on-success:
      - done

  done:
    action: std.echo
    input:
      output: "all-done"
`

// runToCompletion drives all pending tasks to completion. Uses the real
// ActionExecution ID from Task.RuntimeContext["action_ex_id"] when available,
// falling back to direct TaskExecution ID for backwards compatibility.
func runToCompletion(t *testing.T, eng *DefaultEngine, store db.Store, wfExID string) {
	t.Helper()
	const maxIter = 30
	for i := 0; i < maxIter; i++ {
		tasks, err := store.TaskExecutions().List(wfExID, "")
		if err != nil {
			t.Fatalf("list tasks: %v", err)
		}

		allDone := true
		var pending *db.TaskExecution
		for _, te := range tasks {
			if !states.IsCompleted(te.State) {
				allDone = false
				if pending == nil {
					pending = te
				}
			}
		}
		if allDone {
			return
		}

		// Handle with-items: find ALL ActionExecutions for this task and complete each.
		if _, hasSize := pending.RuntimeContext["with_items_size"]; hasSize {
			actionExs, _ := store.ActionExecutions().List(pending.ID, "")
			for _, ae := range actionExs {
				result := &db.Result{Data: pending.Name + "-iter-" + fmt.Sprintf("%v", ae.RuntimeContext["index"])}
				if err := eng.OnActionComplete(context.Background(), ae.ID, result, false); err != nil {
					t.Fatalf("OnActionComplete(%s): %v", ae.ID, err)
				}
			}
			continue
		}

		result := &db.Result{Data: pending.Name + "-done"}
		// Prefer real ActionExecution ID for the proper chain.
		actionExID, _ := pending.RuntimeContext["action_ex_id"].(string)
		invokeID := actionExID
		if invokeID == "" {
			invokeID = pending.ID // fallback
		}
		if err := eng.OnActionComplete(context.Background(), invokeID, result, false); err != nil {
			t.Fatalf("OnActionComplete(%s): %v", invokeID, err)
		}
	}
	t.Fatalf("runToCompletion hit max iterations")
}

func newEngine(t *testing.T) (*DefaultEngine, db.Store) {
	t.Helper()
	store := db.NewInMemoryStore()
	eng := NewDefaultEngine(store)
	return eng, store
}

func TestEngine_DirectWorkflow_EndToEnd(t *testing.T) {
	eng, store := newEngine(t)
	svc := services.NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("e2e_test", directWFYAML, "p", "")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	params := map[string]interface{}{"env": map[string]interface{}{}, "namespace": ""}
	input := map[string]interface{}{"greeting": "Hi", "name": "Mistral"}

	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", input, "test", params)
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	if wfEx.State != states.RUNNING {
		t.Errorf("initial state = %s, want RUNNING", wfEx.State)
	}

	runToCompletion(t, eng, store, wfEx.ID)

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Fatalf("final state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	if len(tasks) != 2 {
		t.Fatalf("tasks count = %d, want 2", len(tasks))
	}
	for _, te := range tasks {
		if !states.IsCompleted(te.State) {
			t.Errorf("task %s state = %s, want completed", te.Name, te.State)
		}
	}
}

func TestEngine_ReverseWorkflow_EndToEnd(t *testing.T) {
	eng, store := newEngine(t)
	svc := services.NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("e2e_reverse", reverseWFYAML, "p", "")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "rev test", map[string]interface{}{
		"env":       map[string]interface{}{},
		"namespace": "",
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	runToCompletion(t, eng, store, wfEx.ID)

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Fatalf("final state = %s (info=%v)", final.State, final.StateInfo)
	}

	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	if len(tasks) != 3 {
		t.Fatalf("tasks count = %d, want 3", len(tasks))
	}
}

func TestEngine_StopWorkflow_Error(t *testing.T) {
	eng, store := newEngine(t)
	svc := services.NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("e2e_test", directWFYAML, "p", "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", map[string]interface{}{
		"env":       map[string]interface{}{},
		"namespace": "",
	})

	if err := eng.StopWorkflow(context.Background(), wfEx.ID, states.ERROR, "manual stop"); err != nil {
		t.Fatalf("stop: %v", err)
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.ERROR {
		t.Errorf("state = %s, want ERROR", final.State)
	}
}

func TestEngine_PauseAndResume(t *testing.T) {
	eng, store := newEngine(t)
	svc := services.NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("e2e_test", directWFYAML, "p", "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", map[string]interface{}{
		"env":       map[string]interface{}{},
		"namespace": "",
	})

	if err := eng.PauseWorkflow(context.Background(), wfEx.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	paused, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if paused.State != states.PAUSED {
		t.Errorf("after pause state = %s, want PAUSED", paused.State)
	}

	if err := eng.ResumeWorkflow(context.Background(), wfEx.ID, nil); err != nil {
		t.Fatalf("resume: %v", err)
	}
	resumed, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if resumed.State != states.RUNNING {
		t.Errorf("after resume state = %s, want RUNNING", resumed.State)
	}
}

func TestEngine_WithItems_EndToEnd(t *testing.T) {
	eng, store := newEngine(t)
	svc := services.NewWorkflowService(store)

	wfDef, err := svc.CreateWorkflow("with_items_test", withItemsWFYAML, "p", "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Check with-items parsing.
	wfSpec, _ := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	fanoutSpec := wfSpec.GetTask("fanout")
	t.Logf("fanoutSpec withItems: %v (len=%d)", fanoutSpec.GetWithItems(), len(fanoutSpec.GetWithItems()))

	input := map[string]interface{}{"items": []interface{}{"alpha", "beta", "gamma"}}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", input, "with-items test", map[string]interface{}{
		"env":       map[string]interface{}{},
		"namespace": "",
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	runToCompletion(t, eng, store, wfEx.ID)

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Fatalf("final state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	// Verify 3 ActionExecutions were created with distinct indices under the fanout task.
	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	var fanoutTaskExID string
	for _, te := range tasks {
		if _, ok := te.RuntimeContext["with_items_size"]; ok {
			fanoutTaskExID = te.ID
		}
	}
	fanoutActionExs, _ := store.ActionExecutions().List(fanoutTaskExID, "")
	indices := make(map[int]bool)
	for _, ae := range fanoutActionExs {
		if idx, ok := ae.RuntimeContext["index"].(int); ok {
			indices[idx] = true
		}
	}
	t.Logf("fanout action executions: %d, distinct indices: %v", len(fanoutActionExs), indices)
	if len(indices) != 3 {
		t.Errorf("fanout distinct indices = %d, want 3", len(indices))
	}
}
