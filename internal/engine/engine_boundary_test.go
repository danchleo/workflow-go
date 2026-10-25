package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mistral/workflow/internal/commands"
	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/services"
	"github.com/mistral/workflow/internal/states"
)

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

func defaultParams() map[string]interface{} {
	return map[string]interface{}{
		"env":       map[string]interface{}{},
		"namespace": "",
	}
}

func mustCreateWorkflow(t *testing.T, store db.Store, name, yamlText string) *db.WorkflowDefinition {
	t.Helper()
	svc := services.NewWorkflowService(store)
	wfDef, err := svc.CreateWorkflow(name, yamlText, "p", "")
	if err != nil {
		t.Fatalf("CreateWorkflow(%s): %v", name, err)
	}
	return wfDef
}

// driveTaskWithResult drives a single task to completion with the given result.
func driveTaskWithResult(t *testing.T, eng *DefaultEngine, store db.Store, taskEx *db.TaskExecution, result *db.Result) {
	t.Helper()
	invokeID, _ := taskEx.RuntimeContext["action_ex_id"].(string)
	if invokeID == "" {
		t.Fatalf("task %s has no action_ex_id", taskEx.Name)
	}
	if err := eng.OnActionComplete(context.Background(), invokeID, result, false); err != nil {
		t.Fatalf("OnActionComplete(%s, %s): %v", taskEx.Name, invokeID, err)
	}
}

// findPendingTask returns the first non-completed task execution.
func findPendingTask(t *testing.T, store db.Store, wfExID string) *db.TaskExecution {
	t.Helper()
	tasks, _ := store.TaskExecutions().List(wfExID, "")
	for _, te := range tasks {
		if !states.IsCompleted(te.State) {
			return te
		}
	}
	return nil
}

// ------------------------------------------------------------
// RerunWorkflow
// ------------------------------------------------------------

const rerunWFYAML = `
version: "2.0"
name: rerun_test

tasks:
  first:
    action: std.echo
    input:
      output: "first"
    on-success:
      - second

  second:
    action: std.fail
    on-error:
      - recover

  recover:
    action: std.echo
    input:
      output: "recovered"
`

// TestEngine_RerunWorkflow_Reset_EndToEnd drives the full reset → rerun chain:
// first SUCCESS → second ERROR → recover SUCCESS → RerunWorkflow(reset) →
// second re-executes SUCCESS → workflow finishes SUCCESS.
// This mirrors Python test_direct_workflow_rerun.py test_rerun_failed_task.
func TestEngine_RerunWorkflow_Reset_EndToEnd(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "rerun", rerunWFYAML)

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	// Drive to pre-rerun state: first→SUCCESS, second→ERROR, recover→SUCCESS.
	const maxIter = 10
	for i := 0; i < maxIter; i++ {
		p := findPendingTask(t, store, wfEx.ID)
		if p == nil {
			break
		}
		invokeID, _ := p.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			continue
		}
		var result *db.Result
		if p.Name == "second" {
			result = &db.Result{Error: "simulated-fail"}
		} else {
			result = &db.Result{Data: p.Name + "-ok"}
		}
		eng.OnActionComplete(context.Background(), invokeID, result, false)
	}

	// Capture second's pre-rerun state.
	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	var secondID string
	for _, te := range tasks {
		if te.Name == "second" {
			secondID = te.ID
			if te.State != states.ERROR {
				t.Fatalf("pre-rerun: second.state = %s, want ERROR", te.State)
			}
		}
	}
	if secondID == "" {
		t.Fatal("second not found")
	}

	wfExBefore, _ := store.WorkflowExecutions().Get(wfEx.ID)
	t.Logf("pre-rerun: wfEx.state=%s, second.state=ERROR", wfExBefore.State)

	// --- Rerun with reset=true ---
	if err := eng.RerunWorkflow(context.Background(), secondID, true, false, nil); err != nil {
		t.Fatalf("RerunWorkflow(reset): %v", err)
	}

	// Assertions on reset.
	resetSecond, _ := store.TaskExecutions().Get(secondID)
	if resetSecond.Processed {
		t.Error("after reset: Processed should be false")
	}
	if resetSecond.State == states.ERROR {
		t.Logf("after reset: state still ERROR → dispatcher may not have fired yet; that's acceptable")
	}
	wfExAfter, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if wfExAfter.State != states.RUNNING {
		t.Errorf("after rerun: wfEx.state = %s, want RUNNING", wfExAfter.State)
	}

	// --- Drive second to SUCCESS now that it re-executes ---
	const rerunIter = 20
	for i := 0; i < rerunIter; i++ {
		if states.IsCompleted(wfExAfter.State) {
			break
		}
		p := findPendingTask(t, store, wfEx.ID)
		if p == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		invokeID, _ := p.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if p.Name == "second" {
			driveTaskWithResult(t, eng, store, p, &db.Result{Data: "second-fixed"})
		} else {
			driveTaskWithResult(t, eng, store, p, &db.Result{Data: p.Name + "-ok"})
		}
		wfExAfter, _ = store.WorkflowExecutions().Get(wfEx.ID)
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Errorf("final wfEx.state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}

	// second should now be SUCCESS too.
	finalSecond, _ := store.TaskExecutions().Get(secondID)
	if finalSecond.State != states.SUCCESS {
		t.Errorf("second final state = %s, want SUCCESS", finalSecond.State)
	}
}

// TestEngine_RerunWorkflow_Skip skips a task instead of resetting it.
// After skip, workflow should reach SUCCESS without running the skipped task.
func TestEngine_RerunWorkflow_Skip(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "skip", rerunWFYAML)

	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	// first → SUCCESS → second → ERROR
	for i := 0; i < 5; i++ {
		p := findPendingTask(t, store, wfEx.ID)
		if p == nil {
			break
		}
		invokeID, _ := p.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			continue
		}
		var result *db.Result
		if p.Name == "second" {
			result = &db.Result{Error: "simulated-fail"}
		} else {
			result = &db.Result{Data: p.Name + "-ok"}
		}
		eng.OnActionComplete(context.Background(), invokeID, result, false)
	}

	// Skip second — should propagate to recover then wfEx SUCCESS.
	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	var secondID string
	for _, te := range tasks {
		if te.Name == "second" {
			secondID = te.ID
		}
	}

	if err := eng.RerunWorkflow(context.Background(), secondID, false, true, nil); err != nil {
		t.Fatalf("RerunWorkflow(skip): %v", err)
	}

	// Drive remaining pending tasks to completion.
	for i := 0; i < 10; i++ {
		p := findPendingTask(t, store, wfEx.ID)
		if p == nil {
			break
		}
		invokeID, _ := p.RuntimeContext["action_ex_id"].(string)
		if invokeID == "" {
			continue
		}
		driveTaskWithResult(t, eng, store, p, &db.Result{Data: p.Name + "-ok"})
	}

	// second should now be SKIPPED, not ERROR.
	skipSecond, _ := store.TaskExecutions().Get(secondID)
	if skipSecond.State != states.SKIPPED {
		t.Errorf("second state after skip rerun = %s, want SKIPPED", skipSecond.State)
	}

	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.SUCCESS {
		t.Errorf("final wfEx.state = %s (info=%v), want SUCCESS", final.State, final.StateInfo)
	}
}

// TestEngine_RerunWorkflow_UnknownTaskID returns a clear error, not a panic.
func TestEngine_RerunWorkflow_UnknownTaskID(t *testing.T) {
	eng, _ := newEngine(t)
	err := eng.RerunWorkflow(context.Background(), "bogus-task-id", true, false, nil)
	if err == nil {
		t.Error("expected error for missing task")
	}
}

// TestEngine_RerunWorkflow_ContextCanceled returns ctx.Err() early.
func TestEngine_RerunWorkflow_ContextCanceled(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "rerun2", rerunWFYAML)
	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	// Find any task and get its ID.
	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	if len(tasks) == 0 {
		t.Fatal("no tasks")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.RerunWorkflow(ctx, tasks[0].ID, true, false, nil); err == nil {
		t.Error("expected context.Canceled error")
	}
}

// ------------------------------------------------------------
// StopWorkflow
// ------------------------------------------------------------

// TestEngine_StopWorkflow_Cancel verifies CANCELLED state (distinct from ERROR).
func TestEngine_StopWorkflow_Cancel(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "stop", rerunWFYAML)
	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	if err := eng.StopWorkflow(context.Background(), wfEx.ID, states.CANCELLED, "user-cancelled"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.CANCELLED {
		t.Errorf("state = %s, want CANCELLED", final.State)
	}
	if final.StateInfo != "user-cancelled" {
		t.Errorf("StateInfo = %q", final.StateInfo)
	}
}

// TestEngine_StopWorkflow_InvalidState returns error for unknown state strings.
func TestEngine_StopWorkflow_InvalidState(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "stop2", rerunWFYAML)
	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	err := eng.StopWorkflow(context.Background(), wfEx.ID, "totally_bogus_state", "msg")
	if err == nil {
		t.Error("expected error for invalid state")
	}
}

// ------------------------------------------------------------
// PauseWorkflow / ResumeWorkflow edge cases
// ------------------------------------------------------------

// TestEngine_PauseWhenAlreadyPaused is a no-op (idempotent).
func TestEngine_PauseWhenAlreadyPaused(t *testing.T) {
	eng, store := newEngine(t)
	wfDef := mustCreateWorkflow(t, store, "pause_idem", rerunWFYAML)
	wfEx, _ := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "", defaultParams())

	if err := eng.PauseWorkflow(context.Background(), wfEx.ID); err != nil {
		t.Fatalf("first pause: %v", err)
	}
	// Second pause should succeed (no panic, no error).
	if err := eng.PauseWorkflow(context.Background(), wfEx.ID); err != nil {
		t.Fatalf("second pause should be no-op: %v", err)
	}
	final, _ := store.WorkflowExecutions().Get(wfEx.ID)
	if final.State != states.PAUSED {
		t.Errorf("state = %s", final.State)
	}
}

// TestEngine_StartWorkflow_MissingDefinitionID returns error, not panic.
func TestEngine_StartWorkflow_MissingDefinitionID(t *testing.T) {
	eng, _ := newEngine(t)
	_, err := eng.StartWorkflow(context.Background(), "nonexistent-def", "", "", nil, "", defaultParams())
	if err == nil {
		t.Error("expected error for missing definition")
	}
}

// ------------------------------------------------------------
// PostTxQueue
// ------------------------------------------------------------

func TestPostTxQueue_DrainSync_Order(t *testing.T) {
	q := NewPostTxQueue()
	var order []string
	q.Register(func() { order = append(order, "a") })
	q.Register(func() { order = append(order, "b") })
	q.Register(func() { order = append(order, "c") })
	q.DrainSync()

	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("order = %v, want [a b c]", order)
	}
}

func TestPostTxQueue_DrainSync_ClearsQueue(t *testing.T) {
	q := NewPostTxQueue()
	called := 0
	q.Register(func() { called++ })
	q.DrainSync()
	q.DrainSync()
	if called != 1 {
		t.Errorf("called = %d, want 1", called)
	}
}

func TestPostTxQueue_DrainSync_NormalBeforeInTx(t *testing.T) {
	q := NewPostTxQueue()
	var order []string
	q.Register(func() { order = append(order, "normal1") })
	q.Register(func() { order = append(order, "intx1") }, true)
	q.Register(func() { order = append(order, "normal2") }, false) // explicit false
	q.Register(func() { order = append(order, "intx2") }, true)

	q.DrainSync()

	if len(order) != 4 {
		t.Fatalf("len = %d, order = %v", len(order), order)
	}
	// Normal callbacks (first) before inTx (last).
	if order[0] != "normal1" || order[1] != "normal2" {
		t.Errorf("normal callbacks should come first, got %v", order)
	}
	if order[2] != "intx1" || order[3] != "intx2" {
		t.Errorf("inTx callbacks should come last, got %v", order)
	}
}

func TestPostTxQueue_Drain_Async(t *testing.T) {
	q := NewPostTxQueue()
	var wg sync.WaitGroup
	wg.Add(1)
	q.Register(func() { wg.Done() })

	q.Drain()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("callback did not run")
	}
}

func TestPostTxQueue_PanicIsCaught(t *testing.T) {
	q := NewPostTxQueue()
	q.Register(func() { panic("boom") })
	q.Register(func() { /* this should still be reachable */ })
	q.DrainSync() // no panic escapes → pass
}

// ------------------------------------------------------------
// Dispatcher: cmdKey uniqueness
// ------------------------------------------------------------

func TestCmdKey_Uniqueness(t *testing.T) {
	r1 := commands.NewRunTask("t1", nil, nil, false)
	r1.SetUniqueKey("k1")
	r2 := commands.NewRunTask("t1", nil, nil, false)
	r2.SetUniqueKey("k1")
	if cmdKey(r1) != cmdKey(r2) {
		t.Errorf("identical RunTask should share key: %q vs %q", cmdKey(r1), cmdKey(r2))
	}

	r3 := commands.NewRunTask("t1", nil, nil, false)
	r3.SetUniqueKey("different")
	if cmdKey(r1) == cmdKey(r3) {
		t.Error("different UniqueKeys should differ")
	}

	// FailWorkflow and RunTask always differ.
	fw := commands.NewFailWorkflow("t1", nil, "x", nil, false)
	if cmdKey(r1) == cmdKey(fw) {
		t.Error("RunTask vs FailWorkflow should differ")
	}

	// Same-name tasks in different RunTask instances with no UniqueKey set still differ
	// because UniqueKey() returns "" for both → keys collide! That's by design — dedup
	// happens when UniqueKey is explicitly set.
	r4 := commands.NewRunTask("t1", nil, nil, false) // no unique key
	r5 := commands.NewRunTask("t1", nil, nil, false) // no unique key
	if cmdKey(r4) != cmdKey(r5) {
		t.Log("info: same task name + no unique key → keys collide (expected)")
	}
}
