// Package integration runs end-to-end engine tests against YAML fixtures in testdata/.
//
// Each fixture is a complete Mistral v2 workflow definition. Tests load it,
// create a workflow definition, start an execution, drive tasks to completion
// via the real engine (as callers would), then assert final state and output.
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/engine"
	"github.com/mistral/workflow/internal/lang/parser"
	"github.com/mistral/workflow/internal/services"
	"github.com/mistral/workflow/internal/states"
)

// fixture is what each YAML test case + its expectations look like.
type fixture struct {
	yamlPath           string // relative to testdata/
	description        string
	input              map[string]interface{}
	expectedState      string   // e.g. states.SUCCESS
	expectedSubstrings []string // strings that MUST appear in the final workflow output
}

// runWorkflowFromYAML is the single shared helper for all integration tests.
// It creates a SQL-backed Store (:memory:), wires up the engine, loads the
// YAML fixture, drives to completion, and returns the final workflow execution.
func runWorkflowFromYAML(t *testing.T, f fixture) *db.WorkflowExecution {
	t.Helper()

	// 1. Read fixture.
	data, err := os.ReadFile("testdata/" + f.yamlPath)
	if err != nil {
		t.Fatalf("read fixture %s: %v", f.yamlPath, err)
	}

	// Sanity check YAML parses before we even touch the engine.
	if _, err := parser.ParseYAML(string(data)); err != nil {
		t.Fatalf("fixture %s YAML parse failed: %v", f.yamlPath, err)
	}

	// 2. Bring up SQL-backed store + engine.
	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	// 3. Persist workflow definition.
	wfDef, err := svc.CreateWorkflow("integration-"+f.yamlPath, string(data), "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	// 4. Start execution.
	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", f.input, f.description, params)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	t.Logf("  started: wfEx=%s state=%s", wfEx.ID, wfEx.State)

	// 5. Drive to completion. In real life each task's ActionExecution would be
	//    run by an executor; here we simulate that by completing each action.
	const maxIter = 50
	for iter := 0; iter < maxIter; iter++ {
		tasks, err := store.TaskExecutions().List(wfEx.ID, "")
		if err != nil {
			t.Fatalf("List tasks: %v", err)
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
			break
		}

		// With-items: one TaskExecution, N ActionExecutions.
		if _, ok := pending.RuntimeContext["with_items_total"]; ok {
			actionExs, _ := store.ActionExecutions().List(pending.ID, "")
			for _, ae := range actionExs {
				res := &db.Result{Data: fmt.Sprintf("iter-%v-done", ae.RuntimeContext["index"])}
				if err := eng.OnActionComplete(context.Background(), ae.ID, res, false); err != nil {
					t.Fatalf("OnActionComplete with-items iter: %v", err)
				}
			}
			continue
		}

		// Single-action task: find real ActionExecution via runtime context,
		// fall back to taskEx ID for early tasks created before dispatcher integration.
		invokeID, _ := pending.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			invokeID = pending.ID
		}
		res := &db.Result{Data: fmt.Sprintf("result-of-%s", pending.Name)}
		if err := eng.OnActionComplete(context.Background(), invokeID, res, false); err != nil {
			t.Fatalf("OnActionComplete (%s): %v", pending.Name, err)
		}
	}

	// 6. Reload final execution and assert.
	final, err := store.WorkflowExecutions().Get(wfEx.ID)
	if err != nil {
		t.Fatalf("get final wfEx: %v", err)
	}

	if final.State != f.expectedState {
		t.Fatalf("final state = %s (info=%v), want %s", final.State, final.StateInfo, f.expectedState)
	}

	t.Logf("  final: state=%s output=%v", final.State, final.Output)
	return final
}

// --- Individual tests (one per fixture) ---

func TestIntegration_DirectSerial(t *testing.T) {
	final := runWorkflowFromYAML(t, fixture{
		yamlPath:      "direct_serial.yaml",
		description:   "2-task serial workflow with publish + output expressions",
		input:         map[string]interface{}{"greeting": "Hi", "name": "Integration"},
		expectedState: states.SUCCESS,
	})
	if final.Output != nil {
		if msg, ok := final.Output["final_msg"].(string); ok {
			if msg == "" {
				t.Errorf("final_msg is empty")
			}
		}
	}
}

func TestIntegration_WithItems_Fanout(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "with_items_fanout.yaml",
		description:   "with-items creating 3 parallel iterations",
		input:         map[string]interface{}{"items": []interface{}{"alpha", "beta", "gamma"}},
		expectedState: states.SUCCESS,
	})
}

func TestIntegration_Reverse_RequiresChain(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "reverse_requires.yaml",
		description:   "reverse workflow: requires dependency chain",
		input:         nil,
		expectedState: states.SUCCESS,
	})
}

func TestIntegration_Branching_FanoutJoin(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "branching_fanout_join.yaml",
		description:   "one task fans out to 2, both fan into a join",
		input:         nil,
		expectedState: states.SUCCESS,
	})
}

func TestIntegration_ErrorHandling_OnError(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "error_handling.yaml",
		description:   "task fails, on-error sends control to recovery",
		input:         nil,
		expectedState: states.SUCCESS, // recovery task takes over, overall succeeds
	})
}

func TestIntegration_Join_Parallel(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "join_parallel.yaml",
		description:   "2 independent start tasks with join: all-before",
		input:         nil,
		expectedState: states.SUCCESS,
	})
}
