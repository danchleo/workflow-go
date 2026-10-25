package parser

import (
	"strings"
	"testing"
)

const simpleDirect = `
version: "2.0"
name: test_wf
description: A test workflow
tags: [test, demo]

input:
  - greeting: "Hello"
  - name

vars:
  region: "us-east-1"

tasks:
  task1:
    action: std.echo
    input:
      output: "<% $.greeting %>, <% $.name %>!"
    publish:
      result: "<% $.task1.result %>"
    on-success:
      - task2

  task2:
    action: std.echo
    input:
      output: "done"
`

func TestParseYAML(t *testing.T) {
	data, err := ParseYAML(simpleDirect)
	if err != nil {
		t.Fatalf("ParseYAML failed: %v", err)
	}
	if data["name"] != "test_wf" {
		t.Errorf("name = %v, want test_wf", data["name"])
	}
	tasks, ok := data["tasks"].(map[string]interface{})
	if !ok {
		t.Fatalf("tasks is not a map: %T", data["tasks"])
	}
	if len(tasks) != 2 {
		t.Errorf("got %d tasks, want 2", len(tasks))
	}
}

func TestGetWorkflowSpecFromYAML(t *testing.T) {
	spec, err := GetWorkflowSpecFromYAML("test_wf", simpleDirect)
	if err != nil {
		t.Fatalf("GetWorkflowSpecFromYAML failed: %v", err)
	}
	if spec.GetName() != "test_wf" {
		t.Errorf("name = %v, want test_wf", spec.GetName())
	}
	if spec.GetType() != "direct" {
		t.Errorf("type = %v, want direct", spec.GetType())
	}
	if spec.GetDescription() != "A test workflow" {
		t.Errorf("description = %v", spec.GetDescription())
	}
	if len(spec.GetTags()) != 2 {
		t.Errorf("tags count = %d, want 2", len(spec.GetTags()))
	}
	if len(spec.GetInput()) != 2 {
		t.Errorf("input count = %d, want 2", len(spec.GetInput()))
	}
	if len(spec.GetVars()) != 1 {
		t.Errorf("vars count = %d, want 1", len(spec.GetVars()))
	}
	tasks := spec.GetTasks()
	if len(tasks) != 2 {
		t.Errorf("tasks count = %d, want 2", len(tasks))
	}

	task1 := spec.GetTask("task1")
	if task1 == nil {
		t.Fatal("task1 not found")
	}
	if task1.GetActionName() != "std.echo" {
		t.Errorf("task1.action = %v, want std.echo", task1.GetActionName())
	}
}

func TestDirectWorkflowStartTasks(t *testing.T) {
	spec, _ := GetWorkflowSpecFromYAML("test_wf", simpleDirect)
	start := spec.StartTasks()
	if len(start) != 1 {
		t.Fatalf("start tasks count = %d, want 1", len(start))
	}
	if start[0].GetName() != "task1" {
		t.Errorf("start task name = %v, want task1", start[0].GetName())
	}
}

func TestReverseWorkflowParse(t *testing.T) {
	yaml := `
version: "2.0"
name: rev_wf

tasks:
  step1:
    action: std.echo
    input:
      output: "1"
  step2:
    action: std.echo
    input:
      output: "2"
    requires: [step1]
  step3:
    action: std.echo
    input:
      output: "3"
    requires: [step2]
`
	spec, err := GetWorkflowSpecFromYAML("rev_wf", yaml)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec.GetType() != "direct" {
		t.Errorf("type = %v, want direct", spec.GetType())
	}
	if strings.Contains(yaml, "type") {
		t.Fatalf("type should be defaulted, not present")
	}
}

func TestMissingTasksIsError(t *testing.T) {
	yaml := `
version: "2.0"
name: bad_wf
`
	_, err := GetWorkflowSpecFromYAML("bad_wf", yaml)
	if err == nil {
		t.Error("expected error for workflow with no tasks")
	}
}

func TestCacheLifecycle(t *testing.T) {
	ClearCaches()
	if WfExCacheSize() != 0 {
		t.Errorf("WfExCacheSize = %d, want 0", WfExCacheSize())
	}

	spec, _ := GetWorkflowSpecFromYAML("test", simpleDirect)
	CacheWorkflowSpecByExecution("exec1", spec)
	if WfExCacheSize() != 1 {
		t.Errorf("WfExCacheSize after cache = %d, want 1", WfExCacheSize())
	}

	got, ok := GetCachedWorkflowSpecByExecution("exec1")
	if !ok || got == nil {
		t.Error("cached spec not found")
	}

	ClearCaches()
	if WfExCacheSize() != 0 {
		t.Errorf("not cleared")
	}
}

// ------------------------------------------------------------
// Version validation tests
// ------------------------------------------------------------

func TestParseYAML_VersionOK(t *testing.T) {
	// String "2.0" — the common form.
	if _, err := ParseYAML("version: \"2.0\"\ntasks: {}\n"); err != nil {
		t.Errorf("version '2.0' string rejected: %v", err)
	}
	// Plain scalar 2.0 (YAML parses this as float64).
	if _, err := ParseYAML("version: 2.0\ntasks: {}\n"); err != nil {
		t.Errorf("version 2.0 float rejected: %v", err)
	}
	// Integer 2 — we tolerate it (normalized to "2.0").
	if _, err := ParseYAML("version: 2\ntasks: {}\n"); err != nil {
		t.Errorf("version int 2 rejected: %v", err)
	}
}

func TestParseYAML_VersionMissing(t *testing.T) {
	_, err := ParseYAML("tasks:\n  t1:\n    action: std.echo\n")
	if err == nil {
		t.Fatal("missing version should be rejected")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("error = %q, should mention missing version", err.Error())
	}
}

func TestParseYAML_VersionUnsupported(t *testing.T) {
	_, err := ParseYAML("version: \"3.0\"\ntasks: {}\n")
	if err == nil {
		t.Fatal("version 3.0 should be rejected")
	}
	if !strings.Contains(err.Error(), "unsupported DSL version") {
		t.Errorf("error = %q", err.Error())
	}

	_, err = ParseYAML("version: \"1.0\"\ntasks: {}\n")
	if err == nil {
		t.Fatal("version 1.0 should be rejected")
	}
}

func TestParseYAML_VersionBadType(t *testing.T) {
	_, err := ParseYAML("version: [1, 2]\ntasks: {}\n")
	if err == nil {
		t.Fatal("list version should be rejected")
	}
}
