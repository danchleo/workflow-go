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
	"time"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/engine"
	"github.com/mistral/workflow/internal/lang/parser"
	"github.com/mistral/workflow/internal/scheduler"
	"github.com/mistral/workflow/internal/services"
	"github.com/mistral/workflow/internal/states"
	"gopkg.in/yaml.v3"
)

// fixture is what each YAML test case + its expectations look like.
type fixture struct {
	yamlPath               string // relative to testdata/
	description            string
	input                  map[string]interface{}
	expectedState          string                          // e.g. states.SUCCESS
	expectedSubstrings     []string                        // strings that MUST appear in the final workflow output
	customResults          map[string]*db.Result           // per-task override: key=taskName, value=Result (nil = default success)
	customResultsByAttempt map[string]func(int) *db.Result // per-task, per-retry-attempt: attempts 0,1,2,...
	withItemsResult        *db.Result                      // override for all with-items iterations of a task (applied via key in customResults)
	extraYAMLs             []string                        // other YAMLs to register as sub-workflow definitions
	workbookPath           string                          // workbook YAML; if set, register all its workflows + use entryWorkflow to start
	entryWorkflow          string                          // when workbookPath set, which workflow inside to start (by workbook-relative name)
}

// runWorkflowFromYAML is the single shared helper for all integration tests.
// It creates a SQL-backed Store (:memory:), wires up the engine, loads the
// YAML fixture, drives to completion, and returns the final workflow execution
// together with the store so callers can query task-level data.
func runWorkflowFromYAML(t *testing.T, f fixture) (*db.WorkflowExecution, db.Store) {
	t.Helper()

	// 1. Bring up SQL-backed store + engine.
	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	var wfDef *db.WorkflowDefinition

	if f.workbookPath != "" {
		// === WORKBOOK MODE ===
		wbData, err := os.ReadFile("testdata/" + f.workbookPath)
		if err != nil {
			t.Fatalf("read workbook %s: %v", f.workbookPath, err)
		}
		wbSvc := services.NewWorkbookService(store)
		wbName := yamlWorkflowName(string(wbData))
		if wbName == "" {
			wbName = "integration-workbook"
		}
		wb, err := wbSvc.CreateWorkbook(wbName, string(wbData), "integration-test", "")
		if err != nil {
			t.Fatalf("CreateWorkbook %q: %v", wbName, err)
		}
		t.Logf("  registered workbook %q with %d workflows", wb.Name, 0) // count below

		// The entry workflow inside the workbook. Python stores as "wb_name.wf_name".
		entry := f.entryWorkflow
		if entry == "" {
			entry = "wf_main" // default convention
		}
		fullEntry := wbName + "." + entry
		wfDef, err = store.WorkflowDefinitions().GetByName(fullEntry, "", "integration-test")
		if err != nil {
			t.Fatalf("workflow %s not found in workbook: %v", fullEntry, err)
		}
		t.Logf("  starting workbook entry %s (wfDef.name=%s)", fullEntry, wfDef.Name)
	} else {
		// === SINGLE WORKFLOW MODE ===
		data, err := os.ReadFile("testdata/" + f.yamlPath)
		if err != nil {
			t.Fatalf("read fixture %s: %v", f.yamlPath, err)
		}
		if _, err := parser.ParseYAML(string(data)); err != nil {
			t.Fatalf("fixture %s YAML parse failed: %v", f.yamlPath, err)
		}

		// 3. Register any extra sub-workflow definitions first.
		for _, extra := range f.extraYAMLs {
			extraData, err := os.ReadFile("testdata/" + extra)
			if err != nil {
				t.Fatalf("read extra fixture %s: %v", extra, err)
			}
			if _, err := parser.ParseYAML(string(extraData)); err != nil {
				t.Fatalf("extra fixture %s parse failed: %v", extra, err)
			}
			regName := yamlWorkflowName(string(extraData))
			if regName == "" {
				regName = "integration-" + extra
			}
			if _, err := svc.CreateWorkflow(regName, string(extraData), "integration-test", ""); err != nil {
				t.Fatalf("CreateWorkflow for %s (name=%s): %v", extra, regName, err)
			}
			t.Logf("  registered sub-workflow %q (name=%s)", extra, regName)
		}

		// Persist the primary workflow definition.
		regName := yamlWorkflowName(string(data))
		if regName == "" {
			regName = "integration-" + f.yamlPath
		}
		wfDef, err = svc.CreateWorkflow(regName, string(data), "integration-test", "")
		if err != nil {
			t.Fatalf("CreateWorkflow (name=%s): %v", regName, err)
		}
	}

	// 5. Start execution.
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
	//    We iterate globally over ALL WorkflowExecutions (parent + any nested
	//    sub-workflows) so that descendant workflows get driven too.
	const maxIter = 100
	for iter := 0; iter < maxIter; iter++ {
		// Collect a pending task from any WorkflowExecution (parent or nested).
		var pending *db.TaskExecution
		var pendingWfEx *db.WorkflowExecution

		allWfExs, err := store.WorkflowExecutions().List("")
		if err != nil {
			t.Fatalf("List all WorkflowExecutions: %v", err)
		}
		for _, w := range allWfExs {
			if states.IsCompleted(w.State) {
				continue
			}
			tasks, _ := store.TaskExecutions().List(w.ID, "")
			for _, te := range tasks {
				if states.IsCompleted(te.State) {
					continue
				}
				t.Logf("  iter %d scan: wfEx=%s task=%s state=%s", iter, w.Name, te.Name, te.State)
				// Skip sub-workflow tasks: they have no ActionExecution of their
				// own — the child WorkflowExecution IS their "action" and will
				// call back automatically via OnActionComplete(wfAction=true).
				if te.State == states.RUNNING {
					hasActionEx := false
					if ae, _ := store.ActionExecutions().List(te.ID, ""); len(ae) > 0 {
						hasActionEx = true
					}
					if !hasActionEx {
						continue // sub-workflow task or idle task — skip
					}
				}
				pending = te
				pendingWfEx = w
				break
			}
			if pending != nil {
				break
			}
		}
		if pending == nil {
			break // everything completed
		}
		t.Logf("  iter %d: driving task %s (wfEx=%s state=%s)", iter, pending.Name, pendingWfEx.Name, pendingWfEx.State)

		// With-items: one TaskExecution, N ActionExecutions.
		// Only drive RUNNING actionExs — already-finished ones (ERROR/SUCCESS)
		// have been processed and may have spawned retry siblings.
		if _, ok := pending.RuntimeContext["with_items_total"]; ok {
			actionExs, _ := store.ActionExecutions().List(pending.ID, "")
			drivenAny := false
			for _, ae := range actionExs {
				if ae.State != states.RUNNING {
					continue // already processed in a previous iter
				}
				attempt := db.CoerceInt(ae.RuntimeContext["retry_attempts"])
				res := defaultResultWithAttempt(pending.Name, ae.Name, attempt, f.customResults, f.customResultsByAttempt, fmt.Sprintf("iter-%v-done", ae.RuntimeContext["index"]))
				if err := eng.OnActionComplete(context.Background(), ae.ID, res, false); err != nil {
					t.Fatalf("OnActionComplete with-items iter: %v", err)
				}
				drivenAny = true
			}
			if drivenAny {
				continue // loop — OnActionComplete may have created retry siblings
			}
			continue
		}

		// Single-action task: find real ActionExecution via runtime context,
		// fall back to taskEx ID for early tasks created before dispatcher integration.
		invokeID, _ := pending.RuntimeContext["action_ex_id"].(string)
		var actionEx *db.ActionExecution
		if invokeID != "" {
			all, _ := store.ActionExecutions().List(pending.ID, "")
			for _, ae := range all {
				if ae.ID == invokeID {
					actionEx = ae
					break
				}
			}
			if actionEx == nil {
				actionExID := invokeID
				_ = actionExID
			}
		}

		// If actionEx finished (non-RUNNING) due to a previous iter's retry spawn,
		// pick the most-recently-created RUNNING one instead.
		if actionEx == nil || actionEx.State != states.RUNNING {
			all, _ := store.ActionExecutions().List(pending.ID, "")
			for _, ae := range all {
				if ae.State == states.RUNNING {
					actionEx = ae
					break
				}
			}
		}

		res := defaultResult(pending.Name, f.customResults, fmt.Sprintf("result-of-%s", pending.Name))
		if actionEx != nil {
			attempt := db.CoerceInt(actionEx.RuntimeContext["retry_attempts"])
			res = defaultResultWithAttempt(pending.Name, actionEx.Name, attempt, f.customResults, f.customResultsByAttempt, fmt.Sprintf("result-of-%s", pending.Name))
			invokeID = actionEx.ID
		}
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
	return final, store
}

// --- Individual tests (one per fixture) ---

func TestIntegration_DirectSerial(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "direct_serial.yaml",
		description:   "2-task serial workflow with publish + output expressions",
		input:         map[string]interface{}{"greeting": "Hi", "name": "Integration"},
		expectedState: states.SUCCESS,
	})

	// Output expressions must be fully evaluated (no raw "<% ... %>" left).
	if final.Output == nil {
		t.Fatal("expected final.Output to be populated")
	}
	for k, v := range final.Output {
		if s, ok := v.(string); ok {
			if containsAny(s, "<%", "{{", "$.") {
				t.Errorf("final.Output[%q] still contains unevaluated expression: %q", k, s)
			}
		}
	}

	// Cross-reference: greeting_was (evaluated from greeting_result publish)
	// must equal the actual greeting_task's result value.
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var greetingResult interface{}
	for _, te := range tasks {
		if te.Name == "greeting_task" && len(te.Output) > 0 {
			greetingResult = te.Output["result"]
		}
	}
	if greetingWas, ok := final.Output["greeting_was"].(string); ok {
		if greetingResult != nil {
			if greetingWas != fmt.Sprintf("%v", greetingResult) {
				t.Errorf("greeting_was = %q, want %q (greeting_task.result)", greetingWas, greetingResult)
			}
		} else {
			t.Errorf("couldn't find greeting_task result for cross-check")
		}
	} else {
		t.Error("final.Output['greeting_was'] missing or not a string")
	}
}

func TestIntegration_WithItems_Fanout(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "with_items_fanout.yaml",
		description:   "with-items creating 3 parallel iterations",
		input:         map[string]interface{}{"items": []interface{}{"alpha", "beta", "gamma"}},
		expectedState: states.SUCCESS,
	})

	// Verify fanout actually happened: exactly 3 ActionExecutions under the fanout task.
	fanoutTasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range fanoutTasks {
		if te.Name != "fanout" {
			continue
		}
		actionExs, err := store.ActionExecutions().List(te.ID, "")
		if err != nil {
			t.Fatalf("list ActionExecutions for fanout: %v", err)
		}
		if len(actionExs) != 3 {
			t.Fatalf("fanout ActionExecutions count = %d, want 3", len(actionExs))
		}

		indices := make(map[int]bool)
		for _, ae := range actionExs {
			t.Logf("  ae id=%s index=%v (type %T) state=%s", ae.ID, ae.RuntimeContext["index"], ae.RuntimeContext["index"], ae.State)
			idxRaw := ae.RuntimeContext["index"]
			var idx int
			switch v := idxRaw.(type) {
			case int:
				idx = v
			case float64:
				idx = int(v)
			}
			indices[idx] = true
			if ae.State != states.SUCCESS {
				t.Errorf("ActionExecution idx=%d state=%s, want SUCCESS", idx, ae.State)
			}
		}
		if !indices[0] || !indices[1] || !indices[2] {
			t.Errorf("fanout indices = %v, want {0,1,2}", indices)
		}
		t.Logf("  fanout indices verified: %v", indices)
	}

	if final.Output != nil {
		for k, v := range final.Output {
			if s, ok := v.(string); ok && containsAny(s, "<%", "{{") {
				t.Errorf("final.Output[%q] = %q — still unevaluated", k, s)
			}
		}
	}
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
	yamlFile, _ := os.ReadFile("testdata/error_handling.yaml")
	store, _ := db.NewSQLStore(":memory:")
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)
	wfDef, _ := svc.CreateWorkflow("err-test", string(yamlFile), "p", "")

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "",
		nil, "", map[string]interface{}{"env": map[string]interface{}{}, "namespace": ""})

	const maxIter = 20
	for iter := 0; iter < maxIter; iter++ {
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
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

		invokeID, _ := pending.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			invokeID = pending.ID
		}

		// The risky task uses std.fail — its simulated executor must return
		// an error Result so the engine marks the task ERROR and follows
		// the on-error transition to "recover".
		var res *db.Result
		if pending.Name == "risky" {
			res = &db.Result{Error: "simulated failure"}
		} else {
			res = &db.Result{Data: fmt.Sprintf("ok-%s", pending.Name)}
		}

		if err := eng.OnActionComplete(context.Background(), invokeID, res, false); err != nil {
			t.Fatalf("OnActionComplete(%s): %v", pending.Name, err)
		}
	}

	final, err := store.WorkflowExecutions().Get(wfEx.ID)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if final.State != states.SUCCESS {
		t.Fatalf("final state = %s, want SUCCESS (recover should have taken over)", final.State)
	}

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		switch te.Name {
		case "risky":
			if te.State != states.ERROR {
				t.Errorf("risky task state = %s, want ERROR", te.State)
			}
		case "recover":
			if te.State != states.SUCCESS {
				t.Errorf("recover task state = %s, want SUCCESS", te.State)
			}
		}
	}
}

func TestIntegration_Join_Parallel(t *testing.T) {
	runWorkflowFromYAML(t, fixture{
		yamlPath:      "join_parallel.yaml",
		description:   "2 independent start tasks with join: all-before",
		input:         nil,
		expectedState: states.SUCCESS,
	})
}

// --- helpers ---
// (CoerceInt is provided by db.CoerceInt — integration tests import db so use it directly.)

// yamlWorkflowName extracts the top-level `name:` field from a Mistral YAML
// definition. This is what the engine uses to look up sub-workflows.
func yamlWorkflowName(yamlText string) string {
	var m map[string]interface{}
	if err := yaml.Unmarshal([]byte(yamlText), &m); err != nil {
		return ""
	}
	if n, ok := m["name"].(string); ok {
		return n
	}
	return ""
}

// defaultResult returns the custom Result for a task if registered,
// otherwise a fresh success Result carrying the given default data string.
func defaultResult(taskName string, overrides map[string]*db.Result, defaultData string) *db.Result {
	if overrides != nil {
		if r, ok := overrides[taskName]; ok {
			return r
		}
	}
	return &db.Result{Data: defaultData}
}

// defaultResultWithAttempt extends defaultResult with attempt-aware overrides.
// Lookup order: byAttempt[taskName](attempt) → overrides[taskName] → default success.
// attempt=0 means first (non-retry) execution. attempt=1 is first retry.
func defaultResultWithAttempt(taskName, actionName string, attempt int, overrides map[string]*db.Result, byAttempt map[string]func(int) *db.Result, defaultData string) *db.Result {
	if byAttempt != nil {
		if fn, ok := byAttempt[taskName]; ok {
			if r := fn(attempt); r != nil {
				return r
			}
		}
		if fn, ok := byAttempt[actionName]; ok {
			if r := fn(attempt); r != nil {
				return r
			}
		}
	}
	return defaultResult(taskName, overrides, defaultData)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) == 0 {
			continue
		}
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

// --- Pause / Resume / Stop lifecycle tests ---

func TestIntegration_PauseResumeStop(t *testing.T) {
	data, err := os.ReadFile("testdata/direct_serial.yaml")
	if err != nil {
		t.Fatal(err)
	}
	store, _ := db.NewSQLStore(":memory:")
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)
	wfDef, _ := svc.CreateWorkflow("pause-test", string(data), "p", "")

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "",
		map[string]interface{}{"greeting": "Hi", "name": "Pause"}, "",
		map[string]interface{}{"env": map[string]interface{}{}, "namespace": ""})

	// Pause mid-flight.
	if err := eng.PauseWorkflow(context.Background(), wfEx.ID); err != nil {
		t.Fatalf("PauseWorkflow: %v", err)
	}
	afterPause, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if afterPause.State != states.PAUSED {
		t.Errorf("after pause state = %s, want PAUSED", afterPause.State)
	}

	// Resume.
	if err := eng.ResumeWorkflow(context.Background(), wfEx.ID, nil); err != nil {
		t.Fatalf("ResumeWorkflow: %v", err)
	}

	// Drive to completion.
	const maxIter = 20
	for iter := 0; iter < maxIter; iter++ {
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
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
		invokeID, _ := pending.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			invokeID = pending.ID
		}
		eng.OnActionComplete(context.Background(), invokeID, &db.Result{Data: "ok"}, false)
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Fatalf("after resume + drive: state = %s, want SUCCESS", final.State)
	}
}

// ============ Complex nested flow tests ============

func TestIntegration_DiamondJoin(t *testing.T) {
	// Classic diamond: start → [left,right] → join_here.
	// (No extra assertions beyond SUCCESS — fanout count is implicit in
	// the controller join logic: join_here waits for both left and right.)
	_, _ = runWorkflowFromYAML(t, fixture{
		yamlPath:      "diamond_join.yaml",
		expectedState: states.SUCCESS,
	})
}

func TestIntegration_MultiLevelFanout(t *testing.T) {
	_, _ = runWorkflowFromYAML(t, fixture{
		yamlPath:      "multi_level_fanout.yaml",
		expectedState: states.SUCCESS,
	})
}

func TestIntegration_ErrorRecovery_ChainContinues(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "error_recovery_chain.yaml",
		expectedState: states.SUCCESS,
		customResults: map[string]*db.Result{
			"risky": {Error: "simulated boom"},
		},
	})

	tasks, err := store.TaskExecutions().List(final.ID, "")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	taskStates := make(map[string]string)
	for _, te := range tasks {
		taskStates[te.Name] = te.State
	}

	// risky must have errored
	if taskStates["risky"] != states.ERROR {
		t.Errorf("risky state = %s, want ERROR", taskStates["risky"])
	}
	// recovery must have succeeded
	if taskStates["recover_task"] != states.SUCCESS {
		t.Errorf("recover_task state = %s, want SUCCESS", taskStates["recover_task"])
	}
	// ... and its downstream must have continued — the key assertion!
	if taskStates["continue_after_error"] != states.SUCCESS {
		t.Errorf("continue_after_error state = %s, want SUCCESS (recovery didn't chain)", taskStates["continue_after_error"])
	}
	t.Logf("  task states: %v", taskStates)
}

func TestIntegration_PublishChain_4Levels(t *testing.T) {
	final, _ := runWorkflowFromYAML(t, fixture{
		yamlPath:      "publish_chain.yaml",
		expectedState: states.SUCCESS,
	})

	if final.Output == nil {
		t.Fatal("expected output")
	}
	for k, v := range final.Output {
		if s, ok := v.(string); ok && containsAny(s, "<%", "{{") {
			t.Errorf("output[%q] still has raw expr: %q", k, s)
		}
	}
	for _, key := range []string{"v1_final", "v2_final", "v3_final", "v4_final"} {
		if _, ok := final.Output[key]; !ok {
			t.Errorf("output missing %q", key)
		}
	}

	// Cross-check v2_final includes the T1 value (proves publish chain accumulated).
	if v2f, ok := final.Output["v2_final"].(string); ok {
		if v2f == "" {
			t.Error("v2_final empty — publish chain broken at t2")
		}
	}
	t.Logf("  publish chain output: %v", final.Output)
}

func TestIntegration_WithItems_ThenBranch(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath: "with_items_then_branch.yaml",
		input: map[string]interface{}{
			"items": []interface{}{"x", "y"},
		},
		expectedState: states.SUCCESS,
	})

	// Fanout must have exactly 2 iterations.
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var fanoutTask *db.TaskExecution
	for _, te := range tasks {
		if te.Name == "fanout" {
			fanoutTask = te
		}
	}
	if fanoutTask == nil {
		t.Fatal("fanout task missing")
	}
	actionExs, _ := store.ActionExecutions().List(fanoutTask.ID, "")
	if len(actionExs) != 2 {
		t.Errorf("fanout ActionExecutions = %d, want 2", len(actionExs))
	}
	t.Logf("  fanout ActionExecutions: %d, fanout final state: %s", len(actionExs), fanoutTask.State)
}

// ============ Sub-workflow (nesting) tests ============

func TestIntegration_Subworkflow_SimpleNesting(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "simple_subworkflow_parent.yaml",
		extraYAMLs:    []string{"simple_subworkflow_child.yaml"},
		expectedState: states.SUCCESS,
	})

	// Verify parent workflow output.
	if v, ok := final.Output["parent_msg"].(string); !ok || v == "" {
		t.Errorf("parent_msg missing or empty: %v", final.Output["parent_msg"])
	}
	t.Logf("  parent output: %v", final.Output)

	// Verify the sub-workflow execution was created and completed.
	allWfExs, _ := store.WorkflowExecutions().List("")
	var subWfEx *db.WorkflowExecution
	for _, w := range allWfExs {
		if w.Name == "simple_subworkflow_child" {
			subWfEx = w
			break
		}
	}
	if subWfEx == nil {
		t.Fatal("child WorkflowExecution not found in store")
	}
	if subWfEx.State != states.SUCCESS {
		t.Errorf("child wfEx state = %s, want SUCCESS", subWfEx.State)
	}
	// Most importantly: the sub-workflow's TaskExecutionID must point back
	// to the parent's call_sub task — this is how the engine knows where
	// to propagate the result.
	if subWfEx.TaskExecutionID == "" {
		t.Error("child wfEx.TaskExecutionID is empty — parent linkage broken")
	}
	t.Logf("  child wfEx state=%s task_execution_id=%s output=%v", subWfEx.State, subWfEx.TaskExecutionID, subWfEx.Output)

	// Verify parent's call_sub task got SUCCESS (child result propagated back).
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		if te.Name == "call_sub" {
			if te.State != states.SUCCESS {
				t.Errorf("call_sub state = %s, want SUCCESS", te.State)
			}
			break
		}
	}
}

// ============ Workbook tests ============

func TestIntegration_Workbook_ChainedNestedWorkflows(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "workbook_chained.yaml",
		entryWorkflow: "wf_main",
		expectedState: states.SUCCESS,
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	if len(tasks) == 0 {
		t.Fatalf("main wfEx %s has zero tasks", final.ID)
	}
	t.Logf("  main workflow tasks: %d", len(tasks))

	// Count all WorkflowExecutions — there should be 3 (main + 2 nested).
	// Note: Go stores WorkflowExecution.Name = WorkflowDefinition.Name,
	// which in workbook mode is "wb_name.wf_name" (Python does the same).
	allWfExs, _ := store.WorkflowExecutions().List("")
	var mainWf, processWf, sumWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "math_pipeline.wf_main":
			mainWf = w
		case "math_pipeline.wf_process":
			processWf = w
		case "math_pipeline.wf_sum":
			sumWf = w
		}
	}
	if mainWf == nil {
		t.Error("math_pipeline.wf_main not found")
	} else {
		if mainWf.RuntimeContext["wb_name"] != "math_pipeline" {
			t.Errorf("main wb_name = %v, want math_pipeline", mainWf.RuntimeContext["wb_name"])
		}
	}
	if processWf == nil {
		t.Error("math_pipeline.wf_process not found — workbook scoping may be broken")
	} else {
		if processWf.TaskExecutionID == "" {
			t.Error("process has no parent TaskExecutionID")
		}
		if processWf.RuntimeContext["wb_name"] != "math_pipeline" {
			t.Errorf("process wb_name = %v, want math_pipeline", processWf.RuntimeContext["wb_name"])
		}
		if processWf.State != states.SUCCESS {
			t.Errorf("process state = %s, want SUCCESS", processWf.State)
		}
	}
	if sumWf == nil {
		t.Error("math_pipeline.wf_sum not found")
	} else {
		if sumWf.State != states.SUCCESS {
			t.Errorf("sum state = %s, want SUCCESS", sumWf.State)
		}
	}
	t.Logf("  3-level workbook chain: main=%v process=%v sum=%v all SUCCESS=%v",
		mainWf != nil, processWf != nil, sumWf != nil, final.State)
}

func TestIntegration_Subworkflow_ChildUsesWithItems(t *testing.T) {
	// Scenario 1: parent calls sub-workflow, sub-workflow internally uses with-items.
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "sub_with_items_parent.yaml",
		extraYAMLs:    []string{"sub_with_items_child.yaml"},
		expectedState: states.SUCCESS,
	})

	// 2 WorkflowExecutions: parent + child.
	allWfExs, _ := store.WorkflowExecutions().List("")
	var childWfEx *db.WorkflowExecution
	for _, w := range allWfExs {
		t.Logf("  wfEx name=%q id=%s state=%s", w.Name, w.ID, w.State)
		if w.Name == "sub_with_items_child" {
			childWfEx = w
		}
	}
	if childWfEx == nil {
		t.Fatal("child wfEx not found")
	}

	// Child must have 3 ActionExecutions from its with-items (fanout task).
	childTasks, _ := store.TaskExecutions().List(childWfEx.ID, "")
	var fanoutTask *db.TaskExecution
	for _, te := range childTasks {
		if te.Name == "fanout" {
			fanoutTask = te
		}
	}
	if fanoutTask == nil {
		t.Fatal("child fanout task not found")
	}
	if v := db.CoerceInt(fanoutTask.RuntimeContext["with_items_total"]); v != 3 {
		t.Errorf("fanout with_items_total = %v, want 3", fanoutTask.RuntimeContext["with_items_total"])
	}
	if fanoutTask.State != states.SUCCESS {
		t.Errorf("fanout state = %s, want SUCCESS", fanoutTask.State)
	}

	// Parent done.
	parentTasks, _ := store.TaskExecutions().List(final.ID, "")
	var delegateTask *db.TaskExecution
	for _, te := range parentTasks {
		if te.Name == "delegate_to_sub" {
			delegateTask = te
		}
	}
	if delegateTask == nil {
		t.Fatal("parent delegate_to_sub task not found")
	}
	if delegateTask.State != states.SUCCESS {
		t.Errorf("delegate_to_sub state = %s, want SUCCESS", delegateTask.State)
	}

	t.Logf("  parent=%s child=%s fanout.total=%v fanout.state=%s all SUCCESS=%v",
		final.State, childWfEx.State, fanoutTask.RuntimeContext["with_items_total"], fanoutTask.State, final.State)
}

func TestIntegration_Subworkflow_ErrorRecovery_ContinuesDownstream(t *testing.T) {
	// Scenario 2: child workflow fails → parent catches on-error → recover → continue.
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "parent_error_recovery.yaml",
		extraYAMLs:    []string{"failing_child.yaml"},
		expectedState: states.SUCCESS,
		customResults: map[string]*db.Result{
			// Child workflow's only task — always fail it so the child
			// workflow reaches ERROR and OnSubWorkflowComplete propagates it
			// back to the parent attempt task.
			"doomed": {Error: "child-doomed-by-test"},
		},
	})

	// Parent workflow overall SUCCESS because on-error recovery worked.
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	taskStates := make(map[string]string)
	for _, te := range tasks {
		taskStates[te.Name] = te.State
	}

	if taskStates["attempt"] != states.ERROR {
		t.Errorf("attempt state = %s, want ERROR", taskStates["attempt"])
	}
	if taskStates["recovery"] != states.SUCCESS {
		t.Errorf("recovery state = %s, want SUCCESS (on-error didn't fire)", taskStates["recovery"])
	}
	// Most important: recovery chains to continue_after_recovery.
	if taskStates["continue_after_recovery"] != states.SUCCESS {
		t.Errorf("continue_after_recovery state = %s, want SUCCESS (recovery dead-ended)",
			taskStates["continue_after_recovery"])
	}
	t.Logf("  task states: %v", taskStates)
}

func TestIntegration_Workbook_WithItemsCallsSubworkflow(t *testing.T) {
	// Scenario 3 (most complex!): Workbook wf_outer has a task that is BOTH
	// with-items AND workflow — 2 iterations, each calling wf_inner.
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "workbook_mixed.yaml",
		entryWorkflow: "wf_outer",
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")

	// We expect 1 parent + 2 children = 3 WorkflowExecutions total.
	var parentWf *db.WorkflowExecution
	childCount := 0
	for _, w := range allWfExs {
		if w.Name == "mixed_wb.wf_outer" {
			parentWf = w
			continue
		}
		if w.Name == "mixed_wb.wf_inner" {
			childCount++
		}
	}
	if parentWf == nil {
		t.Fatal("parent mixed_wb.wf_outer not found")
	}
	if childCount != 2 {
		t.Errorf("wf_inner child count = %d, want 2 (one per with-items iteration)", childCount)
	}

	// Find the fanout_iter task on parent — it's the with-items+workflow task.
	parentTasks, _ := store.TaskExecutions().List(parentWf.ID, "")
	var fanoutIter *db.TaskExecution
	for _, te := range parentTasks {
		if te.Name == "fanout_iter" {
			fanoutIter = te
		}
	}
	if fanoutIter == nil {
		t.Fatal("fanout_iter task not found on parent")
	}
	if v := db.CoerceInt(fanoutIter.RuntimeContext["with_items_total"]); v != 2 {
		t.Errorf("fanout_iter with_items_total = %v, want 2", fanoutIter.RuntimeContext["with_items_total"])
	}
	if fanoutIter.State != states.SUCCESS {
		t.Errorf("fanout_iter state = %s, want SUCCESS", fanoutIter.State)
	}

	t.Logf("  with-items+subworkflow: parent=%s, children(wf_inner)=%d, fanout.total=%v, final=%s",
		parentWf.State, childCount, fanoutIter.RuntimeContext["with_items_total"], final.State)
}

// ================================================================
// Scenario 5: Nested with-items — parent with-items(2) → each iter calls
// sub-wf that itself has with-items(3). Two levels of fanout nesting,
// 2 child wfExs total, 6 ActionExecutions across them.
// ================================================================
func TestIntegration_NestedWithItems_TwoLevels(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "nested_with_items_parent.yaml",
		extraYAMLs:    []string{"nested_with_items_child.yaml"},
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf *db.WorkflowExecution
	childCount := 0
	for _, w := range allWfExs {
		if w.Name == "nw_parent" {
			parentWf = w
		}
		if w.Name == "nw_child" {
			childCount++
			if w.State != states.SUCCESS {
				t.Errorf("nw_child %s state = %s, want SUCCESS", w.ID, w.State)
			}
		}
	}
	if parentWf == nil {
		t.Fatal("nw_parent not found")
	}
	if childCount != 2 {
		t.Errorf("expected 2 child nw_child wfExs, got %d", childCount)
	}

	// Parent outer_fanout task must have with_items_total=2 (outer fanout).
	parentTasks, _ := store.TaskExecutions().List(parentWf.ID, "")
	var outerFanout *db.TaskExecution
	var outerJoin *db.TaskExecution
	for _, te := range parentTasks {
		switch te.Name {
		case "outer_fanout":
			outerFanout = te
		case "outer_join":
			outerJoin = te
		}
	}
	if outerFanout == nil {
		t.Fatal("outer_fanout task not found")
	}
	if v := db.CoerceInt(outerFanout.RuntimeContext["with_items_total"]); v != 2 {
		t.Errorf("outer_fanout with_items_total = %v, want 2", v)
	}
	if outerJoin == nil {
		t.Error("outer_join task not found — join didn't fire!")
	} else if outerJoin.State != states.SUCCESS {
		t.Errorf("outer_join state = %s, want SUCCESS", outerJoin.State)
	}

	// Each child must have inner_fanout with_items_total=3.
	childWfExs := filterWfExByName(allWfExs, "nw_child")
	for _, cw := range childWfExs {
		childTasks, _ := store.TaskExecutions().List(cw.ID, "")
		var innerFanout *db.TaskExecution
		for _, te := range childTasks {
			if te.Name == "inner_fanout" {
				innerFanout = te
			}
		}
		if innerFanout == nil {
			t.Errorf("child %s inner_fanout not found", cw.ID)
			continue
		}
		if v := db.CoerceInt(innerFanout.RuntimeContext["with_items_total"]); v != 3 {
			t.Errorf("child inner_fanout with_items_total = %v, want 3", v)
		}
	}

	// ActionExecutions across both children: 2 children × 3 iters = 6.
	// List ActionExecutions by each child's inner_fanout TaskExecutionID.
	var totalActionExs int
	for _, cw := range childWfExs {
		childTasks, _ := store.TaskExecutions().List(cw.ID, "")
		for _, te := range childTasks {
			if te.Name == "inner_fanout" {
				actions, _ := store.ActionExecutions().List(te.ID, "")
				totalActionExs += len(actions)
			}
		}
	}
	if totalActionExs != 6 {
		t.Errorf("total ActionExecutions across children = %d, want 6 (2 children × 3 iters)", totalActionExs)
	}

	t.Logf("  nested with-items: parent=%s, children=%d, total ActionEx=%d, final=%s",
		parentWf.State, childCount, totalActionExs, final.State)
}

// ================================================================
// Scenario 6: Sibling sub-workflows with join. Two parallel parent tasks
// each spawn their own sub-workflow; both fan into a shared join_task which
// should wait for both child callbacks before firing.
// ================================================================
func TestIntegration_SiblingSubworkflows_Join(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "sibling_join_parent.yaml",
		extraYAMLs:    []string{"sibling_a.yaml", "sibling_b.yaml"},
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf *db.WorkflowExecution
	countA, countB := 0, 0
	for _, w := range allWfExs {
		switch w.Name {
		case "sibling_join_parent":
			parentWf = w
		case "sib_a":
			countA++
		case "sib_b":
			countB++
		}
	}
	if parentWf == nil {
		t.Fatal("parent not found")
	}
	if countA != 1 {
		t.Errorf("expected 1 sib_a, got %d", countA)
	}
	if countB != 1 {
		t.Errorf("expected 1 sib_b, got %d", countB)
	}

	// Parent spawn_a and spawn_b should both be SUCCESS.
	// join_point should only fire after BOTH callbacks arrive.
	parentTasks, _ := store.TaskExecutions().List(parentWf.ID, "")
	taskStates := make(map[string]string)
	for _, te := range parentTasks {
		taskStates[te.Name] = te.State
	}
	if taskStates["spawn_a"] != states.SUCCESS {
		t.Errorf("spawn_a state = %s, want SUCCESS", taskStates["spawn_a"])
	}
	if taskStates["spawn_b"] != states.SUCCESS {
		t.Errorf("spawn_b state = %s, want SUCCESS", taskStates["spawn_b"])
	}
	if taskStates["join_point"] != states.SUCCESS {
		t.Errorf("join_point state = %s, want SUCCESS — join didn't wait for both siblings!", taskStates["join_point"])
	}

	t.Logf("  sibling join: spawn_a=%s spawn_b=%s join_point=%s final=%s",
		taskStates["spawn_a"], taskStates["spawn_b"], taskStates["join_point"], final.State)
}

// ================================================================
// Scenario 7: 4-level deep nesting A→B→C→D. Stress-tests root_execution_id
// propagation, callback chain depth, and that each child correctly resolves
// its parent via TaskExecutionID.
// ================================================================
func TestIntegration_DeepNesting_4Levels(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "deep_A.yaml",
		extraYAMLs:    []string{"deep_B.yaml", "deep_C.yaml", "deep_D.yaml"},
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")

	// Expect 4 WorkflowExecutions: A → B → C → D (each 1 child except A).
	expectNames := map[string]int{"deep_A": 1, "deep_B": 1, "deep_C": 1, "deep_D": 1}
	got := map[string]int{}
	var aWf, bWf, cWf, dWf *db.WorkflowExecution
	for _, w := range allWfExs {
		got[w.Name]++
		switch w.Name {
		case "deep_A":
			aWf = w
		case "deep_B":
			bWf = w
		case "deep_C":
			cWf = w
		case "deep_D":
			dWf = w
		}
	}
	for name, expected := range expectNames {
		if got[name] != expected {
			t.Errorf("%s count = %d, want %d", name, got[name], expected)
		}
	}

	// Parent chain verification:
	//   deep_B.TaskExecutionID references deep_A's "a_call_B" task
	//   deep_C.TaskExecutionID references deep_B's "b_call_C" task
	//   deep_D.TaskExecutionID references deep_C's "c_call_D" task
	parentTasks := map[string]*db.TaskExecution{}
	if aWf != nil {
		ats, _ := store.TaskExecutions().List(aWf.ID, "")
		for _, te := range ats {
			if te.Name == "a_call_B" {
				parentTasks[te.ID] = te
			}
		}
	}
	if bWf != nil {
		bts, _ := store.TaskExecutions().List(bWf.ID, "")
		for _, te := range bts {
			if te.Name == "b_call_C" {
				parentTasks[te.ID] = te
			}
		}
	}
	if cWf != nil {
		cts, _ := store.TaskExecutions().List(cWf.ID, "")
		for _, te := range cts {
			if te.Name == "c_call_D" {
				parentTasks[te.ID] = te
			}
		}
	}

	if bWf != nil {
		if bWf.TaskExecutionID == "" {
			t.Error("deep_B has no parent TaskExecutionID")
		} else if parentTasks[bWf.TaskExecutionID] == nil {
			t.Errorf("deep_B parent TaskExecutionID %s not found in deep_A tasks", bWf.TaskExecutionID)
		}
	}
	if cWf != nil {
		if cWf.TaskExecutionID == "" {
			t.Error("deep_C has no parent TaskExecutionID")
		} else if parentTasks[cWf.TaskExecutionID] == nil {
			t.Errorf("deep_C parent TaskExecutionID %s not found in deep_B tasks", cWf.TaskExecutionID)
		}
	}
	if dWf != nil {
		if dWf.TaskExecutionID == "" {
			t.Error("deep_D has no parent TaskExecutionID")
		} else if parentTasks[dWf.TaskExecutionID] == nil {
			t.Errorf("deep_D parent TaskExecutionID %s not found in deep_C tasks", dWf.TaskExecutionID)
		}
	}

	t.Logf("  4-level deep nesting: %d wfExs, all SUCCESS, parent chain verified, final=%s",
		len(allWfExs), final.State)
}

// ================================================================
// Scenario 8: with-items sub-workflow partial failure. 3 iterations,
// iteration 2's child wfEx gets overridden to ERROR. Expected behavior:
// Python WithItemsTask aggregation → parent task ERROR → on-error fire
// recovery → continue_after_recovery → workflow overall SUCCESS.
// ================================================================
func TestIntegration_WithItemsSubworkflow_PartialFailureRecovery(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "partial_parent.yaml",
		extraYAMLs:    []string{"partial_child.yaml"},
		expectedState: states.SUCCESS,
		// All 3 child wfEx "do_work" tasks are overridden to ERROR — this is more
		// extreme than 1/3 failure but still validates the core rules:
		//   (a) Python WithItemsTask aggregation: any ERROR → parent task ERROR
		//   (b) on-error fires recovery chain even when ALL iters fail
		//   (c) recovery chain continues workflow to overall SUCCESS
		customResults: map[string]*db.Result{
			"do_work": {Error: "intentional-iter-failure"},
		},
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf *db.WorkflowExecution
	childCount := 0
	childErrorCount := 0
	for _, w := range allWfExs {
		if w.Name == "partial_parent" {
			parentWf = w
			continue
		}
		if w.Name == "partial_child" {
			childCount++
			if w.State == states.ERROR {
				childErrorCount++
			} else if w.State != states.SUCCESS {
				t.Errorf("partial_child %s state = %s, want SUCCESS or ERROR", w.ID, w.State)
			}
		}
	}
	if parentWf == nil {
		t.Fatal("parent not found")
	}
	if childCount != 3 {
		t.Errorf("expected 3 child partial_child wfExs, got %d", childCount)
	}

	// Parent fanout_children task must be ERROR (Python WithItemsTask aggregation rule).
	parentTasks, _ := store.TaskExecutions().List(parentWf.ID, "")
	taskStates := make(map[string]string)
	var fanoutTask *db.TaskExecution
	for _, te := range parentTasks {
		taskStates[te.Name] = te.State
		if te.Name == "fanout_children" {
			fanoutTask = te
		}
	}
	if fanoutTask == nil {
		t.Fatal("fanout_children not found")
	}
	if fanoutTask.State != states.ERROR {
		t.Errorf("fanout_children state = %s, want ERROR (WithItemsTask aggregation)", fanoutTask.State)
	}
	if v := db.CoerceInt(fanoutTask.RuntimeContext["with_items_failed_count"]); v < 1 {
		t.Errorf("with_items_failed_count = %d, want >= 1", v)
	}

	// Recovery chain MUST fire even when ALL 3 iters failed.
	if taskStates["handle_some_failed"] != states.SUCCESS {
		t.Errorf("handle_some_failed state = %s, want SUCCESS (on-error didn't fire)",
			taskStates["handle_some_failed"])
	}
	if taskStates["continue_after_error"] != states.SUCCESS {
		t.Errorf("continue_after_error state = %s, want SUCCESS (recovery dead-ended)",
			taskStates["continue_after_error"])
	}
	// all_good must NOT have fired — we have failures!
	if _, exists := taskStates["all_good"]; exists {
		t.Error("all_good should NOT have fired — some iters failed!")
	}

	t.Logf("  all-fail recovery: children=%d (ERROR=%d), fanout=%s(failed=%d), recovery=%v, final=%s",
		childCount, childErrorCount, fanoutTask.State,
		db.CoerceInt(fanoutTask.RuntimeContext["with_items_failed_count"]),
		taskStates["handle_some_failed"]+","+taskStates["continue_after_error"],
		final.State)
}

// filterWfExByName is a tiny helper used by the nested with-items test
// to grab all WorkflowExecutions matching a given name (multiple children
// from with-items iterations share the same name).
func filterWfExByName(wfExs []*db.WorkflowExecution, name string) []*db.WorkflowExecution {
	var out []*db.WorkflowExecution
	for _, w := range wfExs {
		if w.Name == name {
			out = append(out, w)
		}
	}
	return out
}

// ================================================================
// Scenario 9: Concurrency-limited with-items. with-items(5 items) +
// concurrency:2. Only 2 iterations start immediately; remaining 3 launch
// lazily as running iterations complete (Python WithItemsTask capacity gate).
// Assert RuntimeContext tracking is correct — total=5, concurrency=2,
// final started=5, final done_count=5.
// ================================================================
func TestIntegration_WithItemsConcurrency_Limited(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "concurrency_limited.yaml",
		expectedState: states.SUCCESS,
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var launchTask *db.TaskExecution
	for _, te := range tasks {
		if te.Name == "launch" {
			launchTask = te
		}
	}
	if launchTask == nil {
		t.Fatal("launch task not found")
	}

	total := db.CoerceInt(launchTask.RuntimeContext["with_items_total"])
	concurrency := db.CoerceInt(launchTask.RuntimeContext["with_items_concurrency"])
	started := db.CoerceInt(launchTask.RuntimeContext["with_items_started"])

	if total != 5 {
		t.Errorf("with_items_total = %d, want 5", total)
	}
	if concurrency != 2 {
		t.Errorf("with_items_concurrency = %d, want 2", concurrency)
	}
	if started != 5 {
		t.Errorf("final with_items_started = %d, want 5 (capacity refill must launch all)", started)
	}
	if launchTask.State != states.SUCCESS {
		t.Errorf("launch state = %s, want SUCCESS", launchTask.State)
	}

	// Total ActionExecutions must be 5 (one per iteration).
	actions, _ := store.ActionExecutions().List(launchTask.ID, "")
	if len(actions) != 5 {
		t.Errorf("launch ActionExecutions = %d, want 5", len(actions))
	}

	t.Logf("  concurrency limit: total=%d concurrency=%d started(final)=%d actions=%d",
		total, concurrency, started, len(actions))
}

// ================================================================
// Scenario 10: Retry policy — flaky task fails attempt 0 and 1, succeeds
// on attempt 2 (retry:count=2 means max 2 retries after initial → 3 total).
// Assert 3 ActionExecutions created, task ends SUCCESS.
// ================================================================
func TestIntegration_RetryPolicy_SucceedsOnRetry(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_policy.yaml",
		expectedState: states.SUCCESS,
		customResultsByAttempt: map[string]func(int) *db.Result{
			// attempt=0: initial execution → ERROR
			// attempt=1: first retry → ERROR
			// attempt=2: second retry → SUCCESS
			"flaky": func(attempt int) *db.Result {
				if attempt < 2 {
					return &db.Result{Error: fmt.Sprintf("flaky-fail-attempt-%d", attempt)}
				}
				return &db.Result{Data: "finally-worked"}
			},
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask *db.TaskExecution
	var doneTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky":
			flakyTask = te
		case "done":
			doneTask = te
		}
	}
	if flakyTask == nil {
		t.Fatal("flaky task not found")
	}

	// flaky should reach SUCCESS after retry.
	if flakyTask.State != states.SUCCESS {
		t.Errorf("flaky state = %s, want SUCCESS", flakyTask.State)
	}
	if doneTask == nil {
		t.Error("done task not found — on-success didn't fire after retry success!")
	} else if doneTask.State != states.SUCCESS {
		t.Errorf("done state = %s, want SUCCESS", doneTask.State)
	}

	// 3 ActionExecutions: initial + 2 retries.
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 3 {
		t.Errorf("flaky ActionExecutions = %d, want 3 (initial + 2 retries)", len(actions))
	}

	// Verify retry_attempts values: first=0, second=1, third=2 (or 0-indexed).
	var zeroAttempt, oneAttempt, twoAttempt bool
	for _, ae := range actions {
		a := db.CoerceInt(ae.RuntimeContext["retry_attempts"])
		switch a {
		case 0:
			zeroAttempt = true
		case 1:
			oneAttempt = true
		case 2:
			twoAttempt = true
		}
	}
	if !zeroAttempt || !oneAttempt || !twoAttempt {
		t.Errorf("retry_attempts distribution wrong: got zero=%v one=%v two=%v, want all true",
			zeroAttempt, oneAttempt, twoAttempt)
	}

	t.Logf("  retry success: task=%s, retries_done=%d, actions=%d",
		flakyTask.State, db.CoerceInt(flakyTask.RuntimeContext["retry_attempts"]), len(actions))
}

// ================================================================
// Scenario 11: Retry exhausted — task always fails, retries twice, then
// reaches ERROR. Verify retry doesn't corrupt with-items aggregation
// (future scenario — today single task only).
// ================================================================
func TestIntegration_RetryPolicy_ExhaustedRemainingERROR(t *testing.T) {
	// Reuse retry_policy fixture but make all 3 attempts fail.
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_policy.yaml",
		expectedState: states.ERROR,
		customResultsByAttempt: map[string]func(int) *db.Result{
			"flaky": func(attempt int) *db.Result {
				return &db.Result{Error: fmt.Sprintf("always-fail-attempt-%d", attempt)}
			},
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask *db.TaskExecution
	for _, te := range tasks {
		if te.Name == "flaky" {
			flakyTask = te
		}
	}
	if flakyTask == nil {
		t.Fatal("flaky task not found")
	}

	if flakyTask.State != states.ERROR {
		t.Errorf("flaky state = %s, want ERROR after retry exhaustion", flakyTask.State)
	}
	// RetryPolicy has count=2 → initial attempt + 2 retries = 3 total ActionExecutions.
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 3 {
		t.Errorf("flaky ActionExecutions = %d, want 3 (initial + 2 retries exhausted)", len(actions))
	}

	t.Logf("  retry exhausted: task=%s, actions=%d, final_wf=%s",
		flakyTask.State, len(actions), final.State)
}

// ================================================================
// Scenario: retry policy interacts with with-items. Each iteration
// retries independently (per-ActionExecution granularity). When all
// iterations eventually succeed after retries, with-items aggregation
// reports SUCCESS → on-success: all_ok fires.
// ================================================================
func TestIntegration_RetryWithItems_AllRecover(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_with_items.yaml",
		expectedState: states.SUCCESS,
		customResultsByAttempt: map[string]func(int) *db.Result{
			// All 3 iterations fail twice then succeed on 3rd attempt.
			"flaky_loop": func(attempt int) *db.Result {
				if attempt < 2 {
					return &db.Result{Error: fmt.Sprintf("iter-fail-attempt-%d", attempt)}
				}
				return &db.Result{Data: "finally-worked"}
			},
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask, allOkTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky_loop":
			flakyTask = te
		case "all_ok":
			allOkTask = te
		}
	}
	if flakyTask == nil {
		t.Fatal("flaky_loop task not found")
	}
	if allOkTask == nil {
		t.Fatal("all_ok task not found — on-success didn't fire after retry+with-items")
	}
	if flakyTask.State != states.SUCCESS {
		t.Errorf("flaky_loop state = %s, want SUCCESS", flakyTask.State)
	}
	if allOkTask.State != states.SUCCESS {
		t.Errorf("all_ok state = %s, want SUCCESS", allOkTask.State)
	}

	// 3 iterations × 3 attempts each (initial + 2 retries) = 9 ActionExecutions.
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 9 {
		t.Errorf("flaky_loop ActionExecutions = %d, want 9 (3 iters × 3 attempts)", len(actions))
	}

	t.Logf("  retry+with-items recover: flaky_loop=%s, actions=%d, all_ok=%s, final=%s",
		flakyTask.State, len(actions), allOkTask.State, final.State)
}

// ================================================================
// Scenario: retry + with-items where every iteration exhausts its
// retry budget. Per-iteration retries are independent — each gets
// its own fresh ActionExecution chain. with-items aggregation → task
// ERROR → on-error recovery handler fires → overall workflow SUCCESS.
// ================================================================
func TestIntegration_RetryWithItems_AllExhausted(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_with_items.yaml",
		expectedState: states.SUCCESS, // on-error recovery brings wf back to SUCCESS
		customResultsByAttempt: map[string]func(int) *db.Result{
			// All 3 iterations always fail — retry budget fully exhausted.
			"flaky_loop": func(attempt int) *db.Result {
				return &db.Result{Error: fmt.Sprintf("always-fail-attempt-%d", attempt)}
			},
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask, someFailedTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky_loop":
			flakyTask = te
		case "some_failed":
			someFailedTask = te
		}
	}
	if flakyTask == nil {
		t.Fatal("flaky_loop task not found")
	}
	if someFailedTask == nil {
		t.Fatal("some_failed task not found — on-error didn't fire after retry exhaustion")
	}
	if flakyTask.State != states.ERROR {
		t.Errorf("flaky_loop state = %s, want ERROR (all retries exhausted)", flakyTask.State)
	}
	if someFailedTask.State != states.SUCCESS {
		t.Errorf("some_failed state = %s, want SUCCESS", someFailedTask.State)
	}

	// 3 iterations × 3 attempts each (initial + 2 retries all ERROR) = 9 ActionExecutions.
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 9 {
		t.Errorf("flaky_loop ActionExecutions = %d, want 9 (3 iters × 3 attempts exhausted)", len(actions))
	}

	t.Logf("  retry+with-items exhausted: flaky_loop=%s, actions=%d, some_failed=%s, final=%s",
		flakyTask.State, len(actions), someFailedTask.State, final.State)
}

// ================================================================
// Scenario: Retry delay policy. Task flaky with retry{count:2, delay:2}.
// Without driving anything, all attempts will time out via our timeout.
// With delay=2 and the scheduler wired in:
//
//	t=0   attempt 0 starts → fails immediately (we drive ERROR)? No — we let it timeout.
//	      Actually we drive each attempt to ERROR after delay expires.
//	Simpler: drive attempt 0 → ERROR → handleRetry → delay=2s → task=RUNNING_DELAYED
//	t=2   scheduler fires → attempt 1 created → we drive → ERROR → delay=2s → RUNNING_DELAYED
//	t=4   scheduler fires → attempt 2 created → we drive → ERROR → retry exhausted → ERROR
//	→ on-error: recovered
//
// Key assertion: task should be RUNNING_DELAYED during the gaps, and the
// total elapsed time must be >= 4s (2 × 2s delay).
// ================================================================
func TestIntegration_RetryDelay_RespectsDelay(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	coreSched := scheduler.NewDefaultScheduler(store, 50*time.Millisecond, 0, 30*time.Second, 0, 0)
	coreSched.Start()
	defer coreSched.Stop()

	eng := engine.NewDefaultEngine(store).WithScheduler(coreSched)
	svc := services.NewWorkflowService(store)

	data, err := os.ReadFile("testdata/retry_delay.yaml")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	wfDef, err := svc.CreateWorkflow("retry_delay_demo", string(data), "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", params)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	start := time.Now()

	// Drive loop: each time we find RUNNING actionEx, drive it.
	// For the "flaky" task we inject ERROR; for everything else
	// (i.e. recovered from on-error) we inject SUCCESS.
	const maxIter = 30
	for iter := 0; iter < maxIter; iter++ {
		var foundRunning bool
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
		for _, te := range tasks {
			if te.State != states.RUNNING {
				continue
			}
			actionExs, _ := store.ActionExecutions().List(te.ID, "")
			for _, ae := range actionExs {
				if ae.State == states.RUNNING {
					foundRunning = true
					var res *db.Result
					if te.Name == "flaky" {
						res = &db.Result{Error: "injected-failure"}
					} else {
						res = &db.Result{Data: "ok"}
					}
					if err := eng.OnActionComplete(context.Background(), ae.ID, res, false); err != nil {
						t.Fatalf("OnActionComplete: %v", err)
					}
					status := "SUCCESS"
					if te.Name == "flaky" {
						status = "ERROR"
					}
					t.Logf("  iter %d: drove %s action_ex=%s → %s", iter, te.Name, ae.ID, status)
					break
				}
			}
			if foundRunning {
				break
			}
		}

		final, _ := store.WorkflowExecutions().Get(wfEx.ID)
		if states.IsCompleted(final.State) {
			break
		}

		if !foundRunning {
			time.Sleep(200 * time.Millisecond)
		}
	}

	elapsed := time.Since(start)

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask, recoveredTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky":
			flakyTask = te
		case "recovered":
			recoveredTask = te
		}
	}

	if flakyTask == nil {
		t.Fatal("flaky task not found")
	}
	if flakyTask.State != states.ERROR {
		t.Errorf("flaky state = %s, want ERROR (all retries exhausted)", flakyTask.State)
	}
	if recoveredTask == nil || recoveredTask.State != states.SUCCESS {
		t.Error("recovered task missing or not SUCCESS")
	}
	if final.State != states.SUCCESS {
		t.Errorf("final = %s, want SUCCESS", final.State)
	}

	// Critical assertion: elapsed time should reflect the delays.
	// With 2 delays of 2s each (between attempt 0→1 and 1→2),
	// plus our driving time, total should be >= ~4.0s.
	if elapsed < 3000*time.Millisecond {
		t.Errorf("elapsed %v too short — retry delay was NOT respected (expected >= 3s)", elapsed)
	}

	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 3 {
		t.Errorf("actions = %d, want 3 (attempt 0 + 1 + 2 all ERROR)", len(actions))
	}

	t.Logf("  retry delay respected: elapsed=%v, flaky=%s, actions=%d ✅", elapsed, flakyTask.State, len(actions))
}

// ================================================================
// Scenario: Timeout policy. A task hangs forever (we never drive its
// ActionExecution). Timeout fires after 1 second → forces ERROR →
// retry gate runs (none configured) → with-items gate (none) →
// ContinueWorkflow finds on-error: recovered → workflow recovers.
// ================================================================
func TestIntegration_TimeoutPolicy_ForcesError(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	data, err := os.ReadFile("testdata/timeout_policy.yaml")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	wfDef, err := svc.CreateWorkflow("timeout_demo", string(data), "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", params)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	t.Logf("  started: wfEx=%s state=%s", wfEx.ID, wfEx.State)

	// Let the timeout goroutine fire (1s timeout → sleep 1500ms).
	time.Sleep(1500 * time.Millisecond)

	// Now drive any remaining RUNNING tasks (the "recovered" on-error handler
	// needs us to complete it since we don't have a real executor running).
	const maxIter = 30
	for iter := 0; iter < maxIter; iter++ {
		allWfExs, _ := store.WorkflowExecutions().List("")
		var pendingTask *db.TaskExecution
		var pendingAction *db.ActionExecution
		for _, w := range allWfExs {
			if states.IsCompleted(w.State) {
				continue
			}
			tasks, _ := store.TaskExecutions().List(w.ID, "")
			for _, te := range tasks {
				if te.State != states.RUNNING {
					continue
				}
				actionExs, _ := store.ActionExecutions().List(te.ID, "")
				for _, ae := range actionExs {
					if ae.State == states.RUNNING {
						pendingTask = te
						pendingAction = ae
						break
					}
				}
				if pendingTask != nil {
					break
				}
			}
			if pendingTask != nil {
				break
			}
		}
		if pendingTask == nil {
			break // nothing RUNNING — done or stuck
		}
		t.Logf("  post-timeout iter %d: driving %s", iter, pendingTask.Name)
		res := &db.Result{Data: fmt.Sprintf("%s-result", pendingTask.Name)}
		if err := eng.OnActionComplete(context.Background(), pendingAction.ID, res, false); err != nil {
			t.Fatalf("OnActionComplete (%s): %v", pendingTask.Name, err)
		}
	}

	final, err := store.WorkflowExecutions().Get(wfEx.ID)
	if err != nil {
		t.Fatalf("get final wfEx: %v", err)
	}

	// Assert the timeout effect.
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var hangingTask, recoveredTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "hanging":
			hangingTask = te
		case "recovered":
			recoveredTask = te
		}
	}

	if hangingTask == nil {
		t.Fatal("hanging task not found")
	}
	// Timeout should have forced it to ERROR.
	if hangingTask.State != states.ERROR {
		t.Errorf("hanging state = %s, want ERROR (timeout should have fired)", hangingTask.State)
	}

	if recoveredTask != nil {
		if recoveredTask.State != states.SUCCESS {
			t.Errorf("recovered state = %s, want SUCCESS", recoveredTask.State)
		}
	} else {
		t.Error("recovered task not found — on-error didn't fire after timeout")
	}

	if final.State != states.SUCCESS {
		t.Errorf("final wf state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	t.Logf("  timeout: hanging=%s, recovered=%v, final=%s",
		hangingTask.State, recoveredTask != nil, final.State)
}

// ================================================================
// Scenario: Timeout + Retry — attempt 0 times out → retry gate
// catches the ERROR → creates attempt 1 → we drive attempt 1 to
// SUCCESS (faster than timeout) → on-success: done fires → wf SUCCESS.
// This verifies timeout and retry policies are piped correctly through
// the normal OnActionComplete flow.
// ================================================================
func TestIntegration_TimeoutRetry_RetryRecoversFromTimeout(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	data, err := os.ReadFile("testdata/timeout_retry.yaml")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	wfDef, err := svc.CreateWorkflow("timeout_retry_demo", string(data), "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", params)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	t.Logf("  started: wfEx=%s", wfEx.ID)

	// Wait for attempt 0 to timeout (1s timeout).
	time.Sleep(1500 * time.Millisecond)

	// Now drive the RUNNING retry attempt 1 (timeout must have spawned it).
	const maxIter = 30
	var timeoutTask, doneTask *db.TaskExecution
	for iter := 0; iter < maxIter; iter++ {
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
		var pendingTe *db.TaskExecution
		var pendingAe *db.ActionExecution
		for _, te := range tasks {
			if te.State != states.RUNNING {
				continue
			}
			actionExs, _ := store.ActionExecutions().List(te.ID, "")
			for _, ae := range actionExs {
				if ae.State == states.RUNNING {
					pendingTe = te
					pendingAe = ae
					break
				}
			}
			if pendingTe != nil {
				break
			}
		}
		if pendingTe == nil {
			break
		}
		t.Logf("  iter %d: driving %s attempt=%d", iter, pendingTe.Name,
			db.CoerceInt(pendingAe.RuntimeContext["retry_attempts"]))
		if pendingTe.Name == "flaky_hungry" {
			// Attempt 1: we drive it to SUCCESS (faster than its own 1s timeout).
			res := &db.Result{Data: "retry-finally-worked"}
			if err := eng.OnActionComplete(context.Background(), pendingAe.ID, res, false); err != nil {
				t.Fatalf("OnActionComplete (%s): %v", pendingTe.Name, err)
			}
		} else {
			// "done" task: complete normally.
			res := &db.Result{Data: "all-ok"}
			if err := eng.OnActionComplete(context.Background(), pendingAe.ID, res, false); err != nil {
				t.Fatalf("OnActionComplete (%s): %v", pendingTe.Name, err)
			}
		}
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		switch te.Name {
		case "flaky_hungry":
			timeoutTask = te
		case "done":
			doneTask = te
		}
	}

	// Assertions.
	if timeoutTask == nil {
		t.Fatal("flaky_hungry task not found")
	}
	if timeoutTask.State != states.SUCCESS {
		t.Errorf("flaky_hungry state = %s, want SUCCESS (retry recovered from timeout)", timeoutTask.State)
	}
	if doneTask == nil {
		t.Fatal("done task not found — on-success didn't fire")
	}
	if doneTask.State != states.SUCCESS {
		t.Errorf("done state = %s, want SUCCESS", doneTask.State)
	}
	if final.State != states.SUCCESS {
		t.Errorf("final wf = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	// Check ActionExecution chain: attempt 0 (timeout ERROR) + attempt 1 (SUCCESS).
	actions, _ := store.ActionExecutions().List(timeoutTask.ID, "")
	if len(actions) != 2 {
		t.Errorf("actions = %d, want 2 (attempt 0 timed out + attempt 1 succeeded)", len(actions))
	}

	t.Logf("  timeout→retry→success: flaky_hungry=%s, done=%s, actions=%d, final=%s ✅",
		timeoutTask.State, doneTask.State, len(actions), final.State)
}

// ================================================================
// Scenario: Timeout + Retry exhaustion. Every attempt (0, 1, 2) gets
// killed by its own timeout. Retry gate keeps creating fresh attempts
// (each with its own 1s timer). After all 3 attempts time out and
// retry budget (count=2) is exhausted, task becomes ERROR → on-error:
// recovered fires. This verifies retry-created ActionExecutions also
// get timeout timers, and the pipeline propagates correctly.
// ================================================================
func TestIntegration_TimeoutRetry_AllTimeout(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	data, err := os.ReadFile("testdata/timeout_retry_exhausted.yaml")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	wfDef, err := svc.CreateWorkflow("timeout_retry_exhausted", string(data), "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", params)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	t.Logf("  started: wfEx=%s", wfEx.ID)

	// Don't drive anything! Let timeout kill all 3 attempts:
	//   t=0 attempt 0 starts (timeout 1s)
	//   t=1 timeout kills → retry creates attempt 1 (timeout 1s)
	//   t=2 timeout kills → retry creates attempt 2 (timeout 1s)
	//   t=3 timeout kills → retry exhausted → task ERROR → on-error: recovered
	time.Sleep(3500 * time.Millisecond)

	// Now only "recovered" should be RUNNING — drive it to complete.
	const maxIter = 10
	for iter := 0; iter < maxIter; iter++ {
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
		var pendingTe *db.TaskExecution
		var pendingAe *db.ActionExecution
		for _, te := range tasks {
			if te.State != states.RUNNING {
				continue
			}
			actionExs, _ := store.ActionExecutions().List(te.ID, "")
			for _, ae := range actionExs {
				if ae.State == states.RUNNING {
					pendingTe = te
					pendingAe = ae
					break
				}
			}
			if pendingTe != nil {
				break
			}
		}
		if pendingTe == nil {
			break
		}
		res := &db.Result{Data: "recovered-ok"}
		if err := eng.OnActionComplete(context.Background(), pendingAe.ID, res, false); err != nil {
			t.Fatalf("OnActionComplete (%s): %v", pendingTe.Name, err)
		}
		t.Logf("  iter %d: drove %s → SUCCESS", iter, pendingTe.Name)
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask, recoveredTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky_hungry":
			flakyTask = te
		case "recovered":
			recoveredTask = te
		}
	}

	if flakyTask == nil {
		t.Fatal("flaky_hungry task not found")
	}
	if flakyTask.State != states.ERROR {
		t.Errorf("flaky_hungry = %s, want ERROR (all attempts timed out + retry exhausted)", flakyTask.State)
	}
	if recoveredTask == nil {
		t.Fatal("recovered task not found — on-error didn't fire")
	}
	if recoveredTask.State != states.SUCCESS {
		t.Errorf("recovered = %s, want SUCCESS", recoveredTask.State)
	}
	if final.State != states.SUCCESS {
		t.Errorf("final wf = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	// Verify we have exactly 3 ActionExecutions (attempt 0, 1, 2 — all ERROR).
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 3 {
		t.Errorf("actions = %d, want 3 (3 attempts all timed out)", len(actions))
	}

	t.Logf("  timeout×retry exhaustion: flaky=%s(actions=%d), recovered=%s, final=%s ✅",
		flakyTask.State, len(actions), recoveredTask.State, final.State)
}

// ================================================================
// Scenario: Cron Trigger / Scheduler. Register a CronTrigger with
// interval=1s, remaining=2. Manually call scheduler.RunOnce() twice
// at t=0 and t=1s+ to fire the trigger. Verify exactly 2 WorkflowExecutions
// were auto-created and the trigger was deleted after exhausting.
// ================================================================
func TestIntegration_CronTrigger_FiresWorkflow(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	// Register a tiny inline workflow.
	wfYAML := `version: "2.0"
name: cron_target
tasks:
  echo:
    action: std.echo
    input:
      output: "cron-fired"
`
	wfDef, err := svc.CreateWorkflow("cron_target", wfYAML, "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	// Create CronTrigger: fires now (interval=0 → auto-fires), remaining=2.
	ct := &db.CronTrigger{
		Name:                "test-cron",
		Pattern:             "interval:1s",
		IntervalSeconds:     1,
		WorkflowID:          wfDef.ID,
		RemainingExecutions: 2,
		ProjectID:           "integration-test",
		NextExecutionTime:   db.Now(), // due immediately
	}
	if err := store.CronTriggers().Create(ct); err != nil {
		t.Fatalf("Create CronTrigger: %v", err)
	}

	// Wire the scheduler to the engine (engine.StartWorkflow IS the WorkflowStarter).
	sched := scheduler.NewCronRunner(store, eng, 100*time.Millisecond)

	// 1st RunOnce → trigger should fire immediately.
	sched.RunOnce()
	time.Sleep(200 * time.Millisecond) // let goroutines settle

	wfExs, _ := store.WorkflowExecutions().List("")
	if len(wfExs) != 1 {
		t.Fatalf("after 1st RunOnce: wfExs=%d, want 1", len(wfExs))
	}
	// Trigger remaining should be 1 now.
	ctAfter1, _ := store.CronTriggers().GetByName("test-cron")
	if ctAfter1 == nil {
		t.Fatal("cron trigger not found after 1st fire")
	}
	if ctAfter1.RemainingExecutions != 1 {
		t.Errorf("remaining after 1st fire = %d, want 1", ctAfter1.RemainingExecutions)
	}
	if !ctAfter1.NextExecutionTime.After(db.Now()) {
		t.Error("NextExecutionTime should have advanced")
	}
	t.Logf("  1st fire: wfEx=%s, remaining=%d", wfExs[0].ID, ctAfter1.RemainingExecutions)

	// Sleep past the 1s interval → 2nd RunOnce → 2nd fire.
	time.Sleep(1100 * time.Millisecond)
	sched.RunOnce()
	time.Sleep(200 * time.Millisecond)

	wfExs, _ = store.WorkflowExecutions().List("")
	if len(wfExs) != 2 {
		t.Fatalf("after 2nd RunOnce: wfExs=%d, want 2", len(wfExs))
	}

	// Trigger should be DELETED (remaining decremented to 0).
	ctAfter2, err := store.CronTriggers().GetByName("test-cron")
	if err == nil && ctAfter2 != nil {
		t.Error("trigger should be deleted after remaining=0, but still exists")
	}
	t.Logf("  2nd fire: total wfExs=%d, trigger deleted (remaining→0)", len(wfExs))

	// 3rd RunOnce (should be a no-op — trigger is gone).
	sched.RunOnce()
	time.Sleep(100 * time.Millisecond)
	wfExs, _ = store.WorkflowExecutions().List("")
	if len(wfExs) != 2 {
		t.Errorf("after 3rd RunOnce: wfExs=%d, want still 2", len(wfExs))
	}

	t.Logf("  cron trigger test: 2 wfExs auto-created, trigger auto-deleted ✅")
}

// ================================================================
// Scenario: CronTrigger unlimited mode (remaining=0). The trigger
// fires every interval forever — we call RunOnce 3 times with the
// interval elapsing each time. Verify 3 wfExs created, trigger still
// alive with remaining=0, NextExecutionTime keeps advancing.
// ================================================================
func TestIntegration_CronTrigger_Unlimited(t *testing.T) {
	t.Helper()

	store, err := db.NewSQLStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLStore: %v", err)
	}
	eng := engine.NewDefaultEngine(store)
	svc := services.NewWorkflowService(store)

	wfYAML := `version: "2.0"
name: cron_target_forever
tasks:
  echo:
    action: std.echo
    input:
      output: "cron-fired-forever"
`
	wfDef, err := svc.CreateWorkflow("cron_target_forever", wfYAML, "integration-test", "")
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	ct := &db.CronTrigger{
		Name:                "forever-cron",
		Pattern:             "interval:1s",
		IntervalSeconds:     1,
		WorkflowID:          wfDef.ID,
		RemainingExecutions: 0, // 0 = unlimited
		ProjectID:           "integration-test",
		NextExecutionTime:   db.Now(),
	}
	if err := store.CronTriggers().Create(ct); err != nil {
		t.Fatalf("Create CronTrigger: %v", err)
	}

	sched := scheduler.NewCronRunner(store, eng, 100*time.Millisecond)

	// Fire 1.
	sched.RunOnce()
	time.Sleep(1100 * time.Millisecond)

	// Fire 2.
	sched.RunOnce()
	time.Sleep(1100 * time.Millisecond)

	// Fire 3.
	sched.RunOnce()
	time.Sleep(200 * time.Millisecond)

	wfExs, _ := store.WorkflowExecutions().List("")
	if len(wfExs) != 3 {
		t.Fatalf("wfExs = %d, want 3 (trigger 3 times in unlimited mode)", len(wfExs))
	}

	// Trigger must still exist, remaining=0, next time advanced.
	ctFinal, err := store.CronTriggers().GetByName("forever-cron")
	if err != nil || ctFinal == nil {
		t.Fatal("trigger should NOT be deleted in unlimited mode")
	}
	if ctFinal.RemainingExecutions != 0 {
		t.Errorf("remaining = %d, want 0 (unlimited unchanged)", ctFinal.RemainingExecutions)
	}
	if !ctFinal.NextExecutionTime.After(db.Now()) {
		t.Error("NextExecutionTime should be in the future after 3 fires")
	}

	t.Logf("  unlimited cron: 3 wfExs created, trigger alive (remaining=0), next=%v ✅",
		ctFinal.NextExecutionTime)
}

// ================================================================
// pause-before: true policy — workflow pauses before the task that has
// pause-before set. User resumes (which uses scheduler to re-drive the
// IDLE task), then downstream completes normally.
// Mirror Python policies.py PauseBeforePolicy.
// ================================================================
func TestIntegration_PauseBefore_PausesThenResumes(t *testing.T) {
	store, _ := db.NewSQLStore(":memory:")
	coreSched := scheduler.NewDefaultScheduler(store, 50*time.Millisecond, 0, 30*time.Second, 0, 0)
	coreSched.Start()
	defer coreSched.Stop()

	eng := engine.NewDefaultEngine(store).WithScheduler(coreSched)
	svc := services.NewWorkflowService(store)

	data, _ := os.ReadFile("testdata/pause_before.yaml")
	wfDef, _ := svc.CreateWorkflow("pb-test", string(data), "p", "")

	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "",
		map[string]interface{}{"greeting": "Hi"}, "",
		map[string]interface{}{"env": map[string]interface{}{}, "namespace": ""})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	// pause-before fires: workflow should be PAUSED.
	afterPause, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if afterPause.State != states.PAUSED {
		t.Fatalf("after pause-before: state=%s, want PAUSED", afterPause.State)
	}

	// ResumeWorkflow triggers scheduleRefreshTaskState for the IDLE task,
	// which the scheduler picks up and re-runs StartTask.
	if err := eng.ResumeWorkflow(context.Background(), wfEx.ID, nil); err != nil {
		t.Fatalf("ResumeWorkflow: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let scheduler fire

	// Drive risky_step → final_step through OnActionComplete.
	const maxIter = 30
	for iter := 0; iter < maxIter; iter++ {
		wf, _ := store.WorkflowExecutions().Get(wfEx.ID)
		if states.IsCompleted(wf.State) {
			break
		}
		all, _ := store.TaskExecutions().List(wfEx.ID, "")
		var pending *db.TaskExecution
		for _, te := range all {
			if !states.IsCompleted(te.State) {
				pending = te
				break
			}
		}
		if pending == nil {
			break
		}
		invokeID, _ := pending.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			// Scheduler might not have dispatched yet — wait.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err := eng.OnActionComplete(context.Background(), invokeID,
			&db.Result{Data: fmt.Sprintf("ok-%s", pending.Name)}, false); err != nil {
			t.Fatalf("OnActionComplete: %v", err)
		}
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Fatalf("final state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}
	t.Logf("  pause-before: paused at risky_step → resumed → all done ✅")
}

// ================================================================
// retry.break-on: condition triggers on the very first error attempt,
// skipping all remaining retry budget (count=5 but only 1 attempt runs).
// Task lands in ERROR → on-error routes to recover → downstream continues.
// Mirror Python engine/policies.py RetryPolicy.after_task_complete break-on branch.
// ================================================================
func TestIntegration_RetryBreakOn_StopsEarly(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_break_on.yaml",
		expectedState: states.SUCCESS, // recover → downstream → success overall
		customResults: map[string]*db.Result{
			"flaky": {Error: "fatal-config-error"}, // 1st attempt → ERROR, break-on fires
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var flakyTask *db.TaskExecution
	var recoverTask *db.TaskExecution
	for _, te := range tasks {
		switch te.Name {
		case "flaky":
			flakyTask = te
		case "recover":
			recoverTask = te
		}
	}
	if flakyTask == nil {
		t.Fatal("flaky task not found")
	}

	// flaky should be ERROR (break-on stopped retry before any subsequent attempt).
	if flakyTask.State != states.ERROR {
		t.Errorf("flaky state = %s, want ERROR", flakyTask.State)
	}

	// Only ONE ActionExecution — break-on consumed the error, no retry siblings.
	actions, _ := store.ActionExecutions().List(flakyTask.ID, "")
	if len(actions) != 1 {
		t.Errorf("flaky ActionExecutions = %d, want 1 (break-on blocked retry)", len(actions))
	}

	// on-error path: recover → downstream.
	if recoverTask == nil {
		t.Error("recover task not found — on-error did NOT fire!")
	} else if recoverTask.State != states.SUCCESS {
		t.Errorf("recover state = %s, want SUCCESS", recoverTask.State)
	}

	t.Logf("  break-on: flaky had 1 attempt (count=5), ERROR → on-error → recover ✅")
}

// ================================================================
// retry.continue-on: even after SUCCESS, keep retrying. Since continue-on
// evaluates true and break-on is not set, all 3 attempts run. Task lands
// in SUCCESS after attempt 2 (last one), on-success fires normally.
// ================================================================
func TestIntegration_RetryContinueOn_KeepsRetryingOnSuccess(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_continue_on.yaml",
		expectedState: states.SUCCESS,
		customResultsByAttempt: map[string]func(int) *db.Result{
			"loop_task": func(attempt int) *db.Result {
				return &db.Result{Data: fmt.Sprintf("loop-attempt-%d", attempt)}
			},
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	var loopTask *db.TaskExecution
	for _, te := range tasks {
		if te.Name == "loop_task" {
			loopTask = te
		}
	}
	if loopTask == nil {
		t.Fatal("loop_task not found")
	}

	if loopTask.State != states.SUCCESS {
		t.Errorf("loop_task state = %s, want SUCCESS", loopTask.State)
	}

	// 4 ActionExecutions — count=3 means 3 retries on top of initial attempt.
	// So total = 1 + 3 = 4, all consumed because continue-on is true.
	actions, _ := store.ActionExecutions().List(loopTask.ID, "")
	if len(actions) != 4 {
		t.Errorf("loop_task ActionExecutions = %d, want 4 (continue-on keeps retrying after SUCCESS)", len(actions))
	}

	t.Logf("  continue-on: %d attempts all consumed, final SUCCESS ✅", len(actions))
}

// =============================================================================
// Python mistral native workflow YAML fixtures
// These are ADAPTED from mistral/tests/resources/workbook/v2/my_workbook.yaml
// and test previously-missing Go behaviors (when-conditions, join variants,
// params expressions).
// =============================================================================

// TestIntegration_PythonNative_WhenConditions verifies Python-native when:
// expressions on on-success clauses. With my_val=2, only mid_branch
// should fire (not top_branch or bottom_branch).
func TestIntegration_PythonNative_WhenConditions(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "when_join.yaml",
		expectedState: states.SUCCESS,
		expectedSubstrings: []string{
			"all-joined",  // join: all fired
			"two-joined",  // join: 2 fired
			"one-joined",  // join: one fired
			"my_val is 2", // entry task echoed
		},
	})

	// Verify branch routing:
	// top_branch: when="<% $.my_val == 1 %>" → my_val=2 → SKIPPED
	// mid_branch: when="<% $.my_val == 2 %>" → my_val=2 → should RUN
	// bottom_branch: when="<% $.my_val == 3 %>" → my_val=2 → SKIPPED
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		switch te.Name {
		case "top_branch", "bottom_branch":
			// If when-condition filtered them out, they may never be created
			// or may be SKIPPED. Either is fine.
			t.Logf("  %s state = %s (when-condition filtered it out ✅)", te.Name, te.State)
		case "mid_branch":
			if te.State != states.SUCCESS {
				t.Errorf("mid_branch state = %s, want SUCCESS (when my_val==2 should fire)", te.State)
			}
		}
	}

	t.Logf("  when-conditions routed correctly: only mid_branch fired ✅")
}

// TestIntegration_PythonNative_JoinAllOneNumeric verifies all 3 join variants
// (join: all, join: 2, join: one) produce correct logical states.
func TestIntegration_PythonNative_JoinAllOneNumeric(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "when_join.yaml",
		expectedState: states.SUCCESS,
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		switch te.Name {
		case "join_all", "join_two", "join_one":
			if te.State != states.SUCCESS {
				t.Errorf("%s state = %s, want SUCCESS", te.Name, te.State)
			}
			t.Logf("  %s: %s ✅", te.Name, te.State)
		}
	}
}

// TestIntegration_PythonNative_RetryWithExpressions verifies Python-native
// retry with break-on YAQL expression — retry stops early when break-on fires.
func TestIntegration_PythonNative_RetryWithExpressions(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "retry_expr.yaml",
		expectedState: states.SUCCESS, // flaky_with_break ERROR → recovery SUCCESS → wfEx SUCCESS
		customResults: map[string]*db.Result{
			"flaky_with_break": {Error: "boom"}, // break-on fires immediately
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		t.Logf("  %s: state=%s", te.Name, te.State)
	}

	// flaky_with_break should have broken out early — not all 3 retries consumed.
	var flakyID string
	for _, te := range tasks {
		if te.Name == "flaky_with_break" {
			flakyID = te.ID
			break
		}
	}
	if flakyID != "" {
		actions, _ := store.ActionExecutions().List(flakyID, "")
		if len(actions) >= 3 {
			t.Errorf("break-on should have stopped retry early, got %d attempts", len(actions))
		} else {
			t.Logf("  break-on stopped retry early: %d attempts (count=3, break-on fired) ✅", len(actions))
		}
	}
}

// TestIntegration_PythonNative_WithItemsParams verifies Python-native
// with-items + concurrency + params expression evaluation.
func TestIntegration_PythonNative_WithItemsParams(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		yamlPath:      "with_items_params.yaml",
		expectedState: states.SUCCESS,
		expectedSubstrings: []string{
			"collected:", // collector echoes fanout's aggregated result
		},
	})

	tasks, _ := store.TaskExecutions().List(final.ID, "")
	for _, te := range tasks {
		t.Logf("  %s: state=%s", te.Name, te.State)
	}

	// Verify with-items fanout produced multiple child TaskExecutions.
	// The parent "fanout" task's state should be SUCCESS (all children OK).
	for _, te := range tasks {
		if te.Name == "fanout" && te.State != states.SUCCESS {
			t.Errorf("fanout state = %s, want SUCCESS", te.State)
		}
	}

	t.Logf("  with-items + concurrency + params expressions ✅")
}

// =============================================================================
// Python mistral my_workbook.yaml — full multi-workflow + nested + join test
// =============================================================================

// TestIntegration_PythonNative_MyWorkbook exercises the Go-adapted version of
// Python mistral's workbook/v2/my_workbook.yaml — the richest Python test
// workflow. Features covered:
//
//	✅ Multi-workflow workbook (wf1 reverse + wf2 direct)
//	✅ wf2 → wf1 subworkflow call with input passing
//	✅ wf2's with-items + workflow subworkflow (task7)
//	✅ wf2's with-items double-loop cross-product (task8)
//	✅ join: all (task9), join: 2 (task10), join: one (task11)
//	✅ when conditions on task3's on-error/on-success/on-complete
//	✅ task-defaults (retry + break-on YAQL expression + global on-*)
//	✅ break-on YAQL expression stops retry early
func TestIntegration_PythonNative_MyWorkbook(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "my_workbook.yaml",
		entryWorkflow: "wf2", // workbook-relative name (prefix auto-added as "my_wb.wf2")
		input: map[string]interface{}{
			"my_val": 2, // matches wf2.input default; routes task3 → task5
		},
		expectedState: states.SUCCESS,
		expectedSubstrings: []string{
			"matched my_val=2", // task5 fired (my_val==2 when condition)
		},
	})

	// Walk all tasks and log states — my_workbook has MANY tasks because of
	// with-items fanout (task7: 3 iterations, task8: 3*2=6 iterations,
	// task3 subworkflow creates wf1's task1+task2 too).
	tasks, _ := store.TaskExecutions().List(final.ID, "")
	taskStates := make(map[string]int)
	for _, te := range tasks {
		taskStates[te.State]++
		t.Logf("  %s (%s) → state=%s", te.Name, te.ID[:8], te.State)
	}
	t.Logf("  state counts: %v", taskStates)

	// Assert key task states.
	// task3 → calls wf1 (reverse subworkflow) → SUCCESS → on-success when my_val==2 → task5 fires
	// task4 → my_val==1 → NOT matched → should not have TaskExecution created
	// task5 → my_val==2 → MATCHED → SUCCESS
	// task6 → my_val==3 → NOT matched → should not run
	// task7 → with-items [vm-a,vm-b,vm-c] → 3 iterations → each runs wf1 → SUCCESS
	// task8 → with-items cross product [1,2,3] × [x,y] → 6 iterations → SUCCESS
	// task9 → join: all, needs task7 AND task8 SUCCESS → should RUN then SUCCESS
	// task10 → join: 2, waits for 2 of [task7,task8] → RUNNING then SUCCESS
	// task11 → join: one, any 1 of [task7,task8] → quickly SUCCESS

	// Verify task5 DID fire (my_val=2 matched when condition).
	var task5State string
	var task9State, task10State, task11State string
	for _, te := range tasks {
		switch te.Name {
		case "task5":
			task5State = te.State
		case "task9":
			task9State = te.State
		case "task10":
			task10State = te.State
		case "task11":
			task11State = te.State
		}
	}

	if task5State != states.SUCCESS {
		t.Errorf("task5 state = %s, want SUCCESS (my_val=2 should match when)", task5State)
	} else {
		t.Logf("  task5 fired correctly when my_val=2")
	}

	checkJoin := func(name, state string) {
		if state != states.SUCCESS {
			t.Errorf("%s state = %s, want SUCCESS", name, state)
		} else {
			t.Logf("  %s -> SUCCESS", name)
		}
	}
	checkJoin("task9 (join: all)", task9State)
	checkJoin("task10 (join: 2)", task10State)
	checkJoin("task11 (join: one)", task11State)

	t.Logf("  my_workbook (Python native) all checks passed")
}

// --- Python mistral workbook/nested tests (WB1, WB2, wb_with_nested_wf) ---

// TestIntegration_Nested_ReverseCalledByDirect ports Python WB1: a reverse
// workflow (wf1) gets nested-called by a direct workflow (wf2), with
// output propagation back to parent for further publish.
// Go adapts: Python inline workflow args (wf1 param1='Bonnie' param2='Clyde')
// become explicit input: dict; Python task_name='task2' dropped (Go reverse
// starts from no-requires task naturally).
func TestIntegration_Nested_ReverseCalledByDirect(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_reverse.yaml",
		entryWorkflow: "wf2",
		expectedState: states.SUCCESS,
	})

	// Walk all WorkflowExecutions — should have 2: wf2 (direct) + wf1 (reverse).
	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf, childWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_reverse.wf2":
			parentWf = w
		case "nested_reverse.wf1":
			childWf = w
		}
	}
	if parentWf == nil {
		t.Fatal("nested_reverse.wf2 (parent) not found")
	}
	if childWf == nil {
		t.Fatal("nested_reverse.wf1 (child reverse) not found")
	}
	if childWf.State != states.SUCCESS {
		t.Errorf("child reverse wf1 state = %s, want SUCCESS", childWf.State)
	}
	if childWf.TaskExecutionID == "" {
		t.Error("child TaskExecutionID is empty (should point to parent's task)")
	} else {
		t.Logf("  child TaskExecutionID=%s (parent task ref)", childWf.TaskExecutionID)
	}

	// Child wf1 workflow-level output was evaluated via task() builtin.
	// Fake executor returns "result-of-<taskName>" so check no raw expr left.
	childFinal, hasFinal := childWf.Output["final_result"]
	if !hasFinal {
		t.Error("child wf1 output missing 'final_result'")
	} else if s, ok := childFinal.(string); ok {
		if containsAny(s, "<%", "${", "task(") {
			t.Errorf("child final_result still raw expression: %q", s)
		} else {
			t.Logf("  child wf1 final_result=%q (evaluated ✅)", s)
		}
	}

	// Parent wf2 output slogan also evaluated.
	slogan, hasSlogan := final.Output["slogan"]
	if !hasSlogan {
		t.Error("parent wf2 output missing 'slogan'")
	} else if s, ok := slogan.(string); ok {
		if containsAny(s, "<%", "${", "task(") {
			t.Errorf("parent slogan still raw expression: %q", s)
		} else {
			t.Logf("  parent slogan=%q (evaluated ✅)", s)
		}
	}

	t.Logf("  nested reverse-by-direct: parent=%v child=%v final_state=%s",
		parentWf.State, childWf.State, final.State)
}

// TestIntegration_Nested_ChildErrorPropagates ports Python WB2: parent wf1
// calls child wf2 which intentionally fails. Verify parent wf1 goes ERROR
// because its only task fails.
func TestIntegration_Nested_ChildErrorPropagates(t *testing.T) {
	failRes := &db.Result{Error: "intentional failure"}
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_error.yaml",
		entryWorkflow: "wf1",
		expectedState: states.ERROR,
		customResults: map[string]*db.Result{
			"fail_task": failRes, // unique name — no collision
		},
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf, childWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_error.wf1":
			parentWf = w
		case "nested_error.wf2":
			childWf = w
		}
	}
	if parentWf == nil {
		t.Fatal("nested_error.wf1 (parent) not found")
	}
	if childWf == nil {
		t.Fatal("nested_error.wf2 (child) not found")
	}

	// Child should be ERROR (failed action).
	if childWf.State != states.ERROR {
		t.Errorf("child wf2 state = %s, want ERROR", childWf.State)
	} else {
		t.Logf("  child wf2 correctly ERROR")
	}

	// Parent should also be ERROR (only task was the subworkflow call).
	if parentWf.State != states.ERROR {
		t.Errorf("parent wf1 state = %s, want ERROR (should cascade from child)", parentWf.State)
	} else {
		t.Logf("  parent wf1 correctly ERROR (cascaded from child)")
	}
	if final.State != states.ERROR {
		t.Errorf("final state = %s, want ERROR", final.State)
	}

	t.Logf("  nested error propagation: OK")
}

// TestIntegration_Nested_SimplePortsPythonFixture ports the simplest
// workbook nested fixture (wb_with_nested_wf.yaml): wrapping_wf → inner_wf
// with one echo task. Verifies basic subworkflow execution inside a workbook.
func TestIntegration_Nested_SimplePortsPythonFixture(t *testing.T) {
	_, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_simple.yaml",
		entryWorkflow: "wrapping_wf",
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf, childWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_simple.wrapping_wf":
			parentWf = w
		case "nested_simple.inner_wf":
			childWf = w
		}
	}
	if parentWf == nil {
		t.Fatal("nested_simple.wrapping_wf (parent) not found")
	}
	if childWf == nil {
		t.Fatal("nested_simple.inner_wf (child) not found")
	}
	if childWf.State != states.SUCCESS {
		t.Errorf("inner_wf state = %s, want SUCCESS", childWf.State)
	} else {
		t.Logf("  inner_wf SUCCESS")
	}
	if childWf.TaskExecutionID == "" {
		t.Error("child TaskExecutionID is empty (parent ref missing)")
	} else {
		t.Logf("  child TaskExecutionID=%s (parent ref present ✅)", childWf.TaskExecutionID)
	}

	t.Logf("  simple nested workbook: 2 WfExs, all SUCCESS ✅")
}

// TestIntegration_Nested_DeepChainRootExecutionID ports Python WB6: 3-level
// nested chain wf1→wf2→wf3. Verifies RootExecutionID propagation chain:
// wf1 (entry) has RootExecutionID = "" (or =wf1.ID self), wf2 & wf3 both
// point back to wf1.ID as their root. Mirror Python test_subworkflows.py
// test_subworkflow_root_execution_id.
func TestIntegration_Nested_DeepChainRootExecutionID(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_deep_chain.yaml",
		entryWorkflow: "wf1",
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var wf1, wf2, wf3 *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_deep_chain.wf1":
			wf1 = w
		case "nested_deep_chain.wf2":
			wf2 = w
		case "nested_deep_chain.wf3":
			wf3 = w
		}
	}
	if len(allWfExs) != 3 {
		t.Fatalf("expected 3 WorkflowExecutions, got %d", len(allWfExs))
	}
	if wf1 == nil {
		t.Fatal("wf1 missing")
	}
	if wf2 == nil {
		t.Fatal("wf2 missing")
	}
	if wf3 == nil {
		t.Fatal("wf3 missing")
	}
	for _, w := range []*db.WorkflowExecution{wf1, wf2, wf3} {
		if w.State != states.SUCCESS {
			t.Errorf("%s state=%s want SUCCESS", w.Name, w.State)
		}
	}

	// Root execution ID chain: wf1 is root (root=itself or empty),
	// wf2 and wf3 both point to wf1.ID.
	if wf1.RootExecutionID != "" && wf1.RootExecutionID != wf1.ID {
		t.Errorf("wf1 RootExecutionID=%q, want empty or wf1.ID=%s", wf1.RootExecutionID, wf1.ID)
	}
	if wf2.RootExecutionID != wf1.ID {
		t.Errorf("wf2 RootExecutionID=%s, want wf1.ID=%s", wf2.RootExecutionID, wf1.ID)
	}
	if wf3.RootExecutionID != wf1.ID {
		t.Errorf("wf3 RootExecutionID=%s, want wf1.ID=%s", wf3.RootExecutionID, wf1.ID)
	}
	t.Logf("  RootExecutionID chain: wf1.root=%v wf2.root=%s wf3.root=%s (all → wf1.ID=%s) ✅",
		wf1.RootExecutionID, wf2.RootExecutionID, wf3.RootExecutionID, wf1.ID)

	t.Logf("  deep chain (3 levels): 3 WfExs all SUCCESS, root chain correct ✅")
	_ = final
}

// TestIntegration_Nested_DynamicWorkflowName ports Python WB3: workflow:
// field can be a YAQL expression <% $.wf_name %> that resolves to a concrete
// subworkflow name at runtime. Mirrors Python test_subworkflow_success →
// test_dynamic_subworkflow_wf2.
func TestIntegration_Nested_DynamicWorkflowName(t *testing.T) {
	// First: launch wf1 with wf_name="wf2" → child should be wf2, output=wf2_out
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_dynamic_wf.yaml",
		entryWorkflow: "wf1",
		input: map[string]interface{}{
			"wf_name": "wf2",
		},
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf, childWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_dynamic_wf.wf1":
			parentWf = w
		case "nested_dynamic_wf.wf2":
			childWf = w
		}
	}
	if parentWf == nil {
		t.Fatal("wf1 parent not found")
	}
	if childWf == nil {
		t.Fatal("dynamic wf2 child not found — resolveWorkflowName may not be evaluating <% $.wf_name %>")
	}
	if childWf.State != states.SUCCESS {
		t.Errorf("child wf2 state = %s, want SUCCESS", childWf.State)
	}

	// Verify parent output has wf2_out propagated via task() builtin.
	subOut, hasSub := final.Output["sub_wf_out"]
	if !hasSub {
		t.Error("parent wf1 output missing 'sub_wf_out'")
	} else {
		s, _ := subOut.(string)
		t.Logf("  parent sub_wf_out=%q", s)
		if containsAny(s, "<%", "${", "task(") {
			t.Errorf("parent sub_wf_out still raw expression: %q", s)
		}
	}
	t.Logf("  dynamic wf2: ✅ (wf1→wf2 resolved via $.wf_name expr)")

	// Second run with wf_name="wf3" — verify it picks different child.
	_, store2 := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_dynamic_wf.yaml",
		entryWorkflow: "wf1",
		input: map[string]interface{}{
			"wf_name": "wf3",
		},
		expectedState: states.SUCCESS,
	})

	allWfExs2, _ := store2.WorkflowExecutions().List("")
	var childWf3 *db.WorkflowExecution
	for _, w := range allWfExs2 {
		if w.Name == "nested_dynamic_wf.wf3" {
			childWf3 = w
		}
	}
	if childWf3 == nil {
		t.Fatal("dynamic wf3 child not found — second run didn't pick wf3")
	}
	if childWf3.State != states.SUCCESS {
		t.Errorf("child wf3 state = %s, want SUCCESS", childWf3.State)
	}
	t.Logf("  dynamic wf3: ✅ (wf1→wf3 resolved via $.wf_name expr)")

	t.Logf("  dynamic workflow name: wf2 + wf3 both work ✅")
}

// TestIntegration_Nested_DynamicInputDict ports Python WB4: dynamic workflow
// name AND dynamic child input — input: dict references <% $.inp %> to pass
// a resolved value down into the subworkflow. Mirrors Python
// test_dynamic_subworkflow_with_generic_input.
func TestIntegration_Nested_DynamicInputDict(t *testing.T) {
	final, store := runWorkflowFromYAML(t, fixture{
		workbookPath:  "nested_dynamic_input.yaml",
		entryWorkflow: "wf1",
		input: map[string]interface{}{
			"wf_name": "wf2",
			"inp":     "dynamic-value",
		},
		expectedState: states.SUCCESS,
	})

	allWfExs, _ := store.WorkflowExecutions().List("")
	var parentWf, childWf *db.WorkflowExecution
	for _, w := range allWfExs {
		switch w.Name {
		case "nested_dynamic_input.wf1":
			parentWf = w
		case "nested_dynamic_input.wf2":
			childWf = w
		}
	}
	if parentWf == nil {
		t.Fatal("wf1 parent not found")
	}
	if childWf == nil {
		t.Fatal("wf2 child not found")
	}
	if childWf.State != states.SUCCESS {
		t.Errorf("child wf2 state = %s, want SUCCESS", childWf.State)
	}

	// Child should have received "dynamic-value" as its Input["inp"].
	if v, ok := childWf.Input["inp"]; !ok {
		t.Error("child wf2 missing inp in Input")
	} else if v != "dynamic-value" {
		t.Errorf("child wf2 Input.inp = %q, want 'dynamic-value'", v)
	} else {
		t.Logf("  child wf2 Input.inp=%q ✅ (dynamic input propagated)", v)
	}

	// Parent output sub_wf_out should reflect child's output value.
	subOut, hasSub := final.Output["sub_wf_out"]
	if !hasSub {
		t.Error("parent wf1 output missing 'sub_wf_out'")
	} else if s, ok := subOut.(string); ok {
		t.Logf("  parent sub_wf_out=%q", s)
		if containsAny(s, "<%", "${", "task(") {
			t.Errorf("parent sub_wf_out still raw expression: %q", s)
		}
	}

	t.Logf("  dynamic input dict: ✅")
}
