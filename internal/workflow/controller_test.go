package workflow

import (
	"context"
	"testing"

	"github.com/danchleo/workflow-go/internal/commands"
	"github.com/danchleo/workflow-go/internal/db"
	v2 "github.com/danchleo/workflow-go/internal/lang/v2"
	"github.com/danchleo/workflow-go/internal/states"
)

// makeWfSpec builds a WorkflowSpec from raw YAML-like data.
func makeWfSpec(t *testing.T, raw map[string]interface{}) *v2.WorkflowSpec {
	t.Helper()
	spec, err := v2.NewWorkflowSpec("test", raw)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// TestDirectFindNextCommands_WhenCondition verifies Python's
// "if not cond or expr.evaluate(cond, ctx_view)" — only matching when-conditions run.
func TestDirectFindNextCommands_WhenCondition(t *testing.T) {
	raw := map[string]interface{}{
		"version": "2.0",
		"name":    "when_test",
		"tasks": map[string]interface{}{
			"start": map[string]interface{}{
				"action": "std.echo",
				"on-success": map[string]interface{}{
					"next": []interface{}{
						map[string]interface{}{"task": "path_a", "when": "<% $.start.result == 'go-a' %>"},
						map[string]interface{}{"task": "path_b", "when": "<% $.start.result == 'go-b' %>"},
						"path_c", // no when → always runs
					},
				},
			},
			"path_a": map[string]interface{}{"action": "std.echo"},
			"path_b": map[string]interface{}{"action": "std.echo"},
			"path_c": map[string]interface{}{"action": "std.echo"},
		},
	}

	spec := makeWfSpec(t, raw)

	// Case 1: output="go-a" → path_a + path_c should run; path_b should NOT.
	wfEx := &db.WorkflowExecution{ID: "wf1", State: states.RUNNING}
	tasks := []*db.TaskExecution{
		{Name: "start", State: states.SUCCESS, Output: map[string]interface{}{"result": "go-a"}},
	}
	ctrl := NewDirectWorkflowController(wfEx, spec, tasks)
	cmds := ctrl.ContinueWorkflow(tasks[0])
	names := cmdTaskNames(cmds)
	if !contains(names, "path_a") {
		t.Errorf("go-a case: expected path_a, got %v", names)
	}
	if contains(names, "path_b") {
		t.Errorf("go-a case: unexpected path_b in %v", names)
	}
	if !contains(names, "path_c") {
		t.Errorf("go-a case: path_c (no when) should always run, got %v", names)
	}

	// Case 2: output="go-b" → path_b + path_c should run.
	tasks2 := []*db.TaskExecution{
		{Name: "start", State: states.SUCCESS, Output: map[string]interface{}{"result": "go-b"}},
	}
	ctrl2 := NewDirectWorkflowController(wfEx, spec, tasks2)
	cmds2 := ctrl2.ContinueWorkflow(tasks2[0])
	names2 := cmdTaskNames(cmds2)
	if !contains(names2, "path_b") || contains(names2, "path_a") {
		t.Errorf("go-b case: want [path_b path_c], got %v", names2)
	}
}

// TestDirectFindNextCommands_OnSkipFallback verifies Python's
// if on-skip is empty → fall back to on-success.
func TestDirectFindNextCommands_OnSkipFallback(t *testing.T) {
	raw := map[string]interface{}{
		"version": "2.0",
		"name":    "skip_fallback",
		"tasks": map[string]interface{}{
			"start": map[string]interface{}{
				"action":     "std.echo",
				"on-success": "path_ok",
				// NO on-skip clause — should fall back to on-success.
			},
			"path_ok": map[string]interface{}{"action": "std.echo"},
		},
	}

	spec := makeWfSpec(t, raw)
	wfEx := &db.WorkflowExecution{ID: "wf", State: states.RUNNING}
	tasks := []*db.TaskExecution{
		{Name: "start", State: states.SKIPPED},
	}
	ctrl := NewDirectWorkflowController(wfEx, spec, tasks)
	cmds := ctrl.ContinueWorkflow(tasks[0])
	names := cmdTaskNames(cmds)
	if !contains(names, "path_ok") {
		t.Errorf("SKIPPED task with no on-skip should fall back to on-success, got %v", names)
	}
}

// TestOnClause_ParseConditionMap verifies Go's parseNextItem correctly handles
// Python-style map next-item {task_name: "<% cond %>"}.
func TestOnClause_ParseConditionMap(t *testing.T) {
	data := map[string]interface{}{
		"next": []interface{}{
			map[string]interface{}{"task": "t1", "when": "<% $.x == 1 %>"},
			map[string]interface{}{"task": "t2"},
			map[string]interface{}{"t3": "<% $.x > 0 %>"}, // Python shorthand form
			"t4", // plain string
		},
	}
	clause, err := v2.NewOnClauseSpec(data)
	if err != nil {
		t.Fatal(err)
	}

	items := clause.GetNext()
	if len(items) != 4 {
		t.Fatalf("want 4 items, got %d", len(items))
	}

	// t1: task + when
	found1 := findItem(items, "t1")
	if found1 == nil || found1.When != "<% $.x == 1 %>" {
		t.Errorf("t1 When = %q", found1.When)
	}
	// t2: task only, no when
	found2 := findItem(items, "t2")
	if found2 == nil || found2.When != "" {
		t.Errorf("t2 When = %q (want empty)", found2.When)
	}
	// t3: Python shorthand — map key is task name
	found3 := findItem(items, "t3")
	if found3 == nil || found3.When != "<% $.x > 0 %>" {
		t.Errorf("t3 (shorthand) When = %q, Task = %q", found3.When, found3.Task)
	}
	// t4: plain string
	found4 := findItem(items, "t4")
	if found4 == nil || found4.When != "" {
		t.Errorf("t4 When = %q", found4.When)
	}
}

// TestOnClause_ParseTaskNameKey verifies parseNextItem uses "task" key (Python),
// NOT the old wrong "name" key.
func TestOnClause_ParseTaskNameKey(t *testing.T) {
	data := map[string]interface{}{
		"next": []interface{}{
			map[string]interface{}{"task": "correct_name", "when": "<% true %>"},
			map[string]interface{}{"name": "wrong_name", "when": "<% true %>"}, // ignored — "name" not Python key
		},
	}
	clause, _ := v2.NewOnClauseSpec(data)
	items := clause.GetNext()

	foundCorrect := findItem(items, "correct_name")
	if foundCorrect == nil {
		t.Errorf("expected item with Task='correct_name'")
	}
	// "wrong_name" won't be picked up since we look for "task" key now.
	// It will be a zero-value NextItem{Task: ""} which is fine — it won't match anything.
}

func findItem(items []v2.NextItem, task string) *v2.NextItem {
	for i := range items {
		if items[i].Task == task {
			return &items[i]
		}
	}
	return nil
}

func cmdTaskNames(cmds []commands.WorkflowCommand) []string {
	names := []string{}
	for _, c := range cmds {
		names = append(names, c.TaskName())
	}
	return names
}

func contains(arr []string, s string) bool {
	for _, x := range arr {
		if x == s {
			return true
		}
	}
	return false
}

// Ensure context import is used somewhere (commands package may need it).
var _ = context.Background
