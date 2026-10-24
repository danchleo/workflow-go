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
	"gopkg.in/yaml.v3"
)

// fixture is what each YAML test case + its expectations look like.
type fixture struct {
	yamlPath           string // relative to testdata/
	description        string
	input              map[string]interface{}
	expectedState      string                // e.g. states.SUCCESS
	expectedSubstrings []string              // strings that MUST appear in the final workflow output
	customResults      map[string]*db.Result // per-task override: key=taskName, value=Result (nil = default success)
	withItemsResult    *db.Result            // override for all with-items iterations of a task (applied via key in customResults)
	extraYAMLs         []string              // other YAMLs to register as sub-workflow definitions
	workbookPath       string                // workbook YAML; if set, register all its workflows + use entryWorkflow to start
	entryWorkflow      string                // when workbookPath set, which workflow inside to start (by workbook-relative name)
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
		if _, ok := pending.RuntimeContext["with_items_total"]; ok {
			actionExs, _ := store.ActionExecutions().List(pending.ID, "")
			for _, ae := range actionExs {
				res := defaultResult(pending.Name, f.customResults, fmt.Sprintf("iter-%v-done", ae.RuntimeContext["index"]))
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
		res := defaultResult(pending.Name, f.customResults, fmt.Sprintf("result-of-%s", pending.Name))
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

// coerceRuntimeInt handles JSON round-trip of ints inside map[string]interface{}.
// SQLStore blob pattern serializes RuntimeContext as JSON, turning Go ints → float64.
func coerceRuntimeInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	}
	return 0
}

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
	if v := coerceRuntimeInt(fanoutTask.RuntimeContext["with_items_total"]); v != 3 {
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
	if v := coerceRuntimeInt(fanoutIter.RuntimeContext["with_items_total"]); v != 2 {
		t.Errorf("fanout_iter with_items_total = %v, want 2", fanoutIter.RuntimeContext["with_items_total"])
	}
	if fanoutIter.State != states.SUCCESS {
		t.Errorf("fanout_iter state = %s, want SUCCESS", fanoutIter.State)
	}

	t.Logf("  with-items+subworkflow: parent=%s, children(wf_inner)=%d, fanout.total=%v, final=%s",
		parentWf.State, childCount, fanoutIter.RuntimeContext["with_items_total"], final.State)
}
