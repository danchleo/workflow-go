# workflow-go

A Go implementation of the [OpenStack Mistral](https://docs.openstack.org/mistral/latest/) workflow engine. Load Mistral v2 YAML definitions, execute them with full support for **nested workflows, dynamic subworkflow resolution, with-items fan-out/join, retry policies, on-clauses, YAQL expressions, and workbooks**.

Design goal: keep the core engine dependency-light (no goroutine-per-workflow, no heavy framework), runnable as an in-memory library or behind a minimal REST API.

## Status

- ✅ Core engine (task execution, direct/reverse/workflow-on-success/workflow-on-error, with-items fan-out/join, join-by-key)
- ✅ **Nested workflows** with `workflow:` — both static names and **dynamic names via YAQL** (`<% $.wf_name %>`)
- ✅ YAQL expression engine — lexer + recursive-descent parser + interpreter (`$.x`, `task()`, `env()`, `execution()`, arithmetic, comparisons, member/index access)
- ✅ Retry policies (`count`, `delay`, `break-on`, `continue-on`, YAQL guards)
- ✅ Error handling (`on-error`, `on-success`, `on-complete` with when-conditions)
- ✅ Workbook mode (multi-workbook registration, workflow name → `<workbook>.<name>` resolution)
- ✅ **RootExecutionID propagation** across 3+ levels of subworkflow chain
- ✅ Full suite of integration tests ported from Python Mistral (`test_subworkflows.py`, `test_workflows.py`, `test_with_items.py`, `test_retry_policy.py`, `test_error_handling.py`) — **9 packages, 100% race-clean**
- 🚧 Pause/resume (single-workflow granularity, cascade across subworkflows not yet implemented)
- 🚧 Cascade delete
- 🚧 Jinja `{{ }}` expressions (YAQL only for now)

## Quick Start

```bash
go version       # needs Go 1.22+
go test -race ./...
go run ./cmd/test --demo          # run an example workflow
go run ./cmd/test --addr :8080    # start REST API server
```

## Architecture

```
cmd/test/                 # Entry point — --demo prints a sample run, default starts HTTP API
internal/
  db/                     # Store abstraction: in-memory + SQL schemas
  engine/                 # Core engine: StartWorkflow, OnActionComplete, handlers.go (dispatch, subworkflow launch)
  expressions/            # YAQL engine — lexer, parser, evaluator. Context-safe, no global state.
  lang/
    v2/                   # Mistral v2 spec model: WorkflowSpec, TaskSpec, OnClause, Policies, RetryPolicy, Workbook
    parser/               # YAML → v2 spec parser
  workflow/controller.go  # ContinueWorkflow — the "brain" that picks next tasks given current state
  scheduler/              # Cron / delayed-run scheduling
  actions/                # Action registry (std.echo, std.noop, ...) + fake executor for tests
  services/               # WorkflowDefinition CRUD on top of db.Store
  integration/            # Python Mistral parity tests — YAML fixtures in testdata/, assertions in integration_test.go
pkg/api/                  # HTTP handler layer
```

### How a workflow runs

1. `services.CreateWorkflow(name, yaml)` — parse YAML into `v2.WorkflowSpec`, persist as `WorkflowDefinition`.
2. `engine.StartWorkflow(defID, input)` — create `WorkflowExecution` (state `RUNNING`), call `controller.ContinueWorkflow(nil)` to find first tasks, dispatch actions.
3. For each started task the action executor runs, then calls `engine.OnActionComplete(taskExID, result)` → update `TaskExecution`, run `controller.ContinueWorkflow` again.
4. If a task's action is `workflow:` → resolve workflow name (supports `<% $.wf_name %>`), create a child `WorkflowExecution`, propagate `RootExecutionID`, store task output = child workflow result.

### Context model

Mistral flattens the workflow execution input into the top-level context so expressions like `<% $.my_param %>` work directly. The Go engine mirrors this:

```
ctx = merge(
  wfEx.Input,                // workflow-level input dict, flattened
  "__task_results": {...},   // prior task results by name (task('name'))
  "__env": ...,              // env()
  "__execution": {           // execution()
    "id": wfEx.ID,
    "workflow": wfEx.Name,
    "project_id": wfEx.ProjectID,
    "root_execution_id": wfEx.RootExecutionID,
    "task_execution_id": ...,
  },
)
```

### RootExecutionID propagation

The root workflow in a chain starts with `RootExecutionID=""`. When launching a subworkflow, the child copies the **parent's** `RootExecutionID` if present, otherwise falls back to the parent's own workflow execution ID. This gives every workflow in a nested tree a single common root for correlation:

```
wf1 (root="")
  └─ wf2 (root=wf1.ID)
       └─ wf3 (root=wf1.ID)   ← not wf2.ID
```

Verified by `testdata/nested_deep_chain.yaml` — 3-level deep chain test.

## Expression Engine

Custom YAQL implementation, no external dependency. Lexer → recursive-descent parser → tree-walking evaluator.

Builtins:

| Name | Description |
|------|-------------|
| `task('name')` | Access prior task result |
| `env()` | Environment variables |
| `execution()` | Current execution metadata |
| `global_('key')` | Global parameter lookup |
| `uuid_()` | Generate UUID |
| `json_dump_(obj)` / `json_parse_(str)` | JSON helpers |
| `yaml_dump_` / `yaml_parse_` | YAML helpers |
| `range(n)` | Generate 0..n-1 list |
| `length(x)` / `len(x)` | Length |
| `sum(x)` / `abs(x)` / `first(x)` | Numeric / list helpers |
| `list(...)` / `dict(k, v, ...)` | Collection constructors |

The engine is **concurrency-safe**: `BuiltinFunc` takes `(eval *yaqEval, args []interface{})` so every evaluator carries its own per-call ctx — no globals, no thread-local trickery. A 16-goroutine stress test (`go run -race`) produces zero data races.

## Test Matrix

```
go test -race ./...            # all 9 packages, race detector
go test -race ./internal/integration/...   # Python parity suite (~55 YAML fixtures)
go test -race -run TestIntegration_Nested ./internal/integration/...   # nested workflow tests only
```

Integration fixture coverage:

| Fixture | Python parity | Feature |
|---------|--------------|---------|
| `nested_simple.yaml` | wb_with_nested_wf.yaml | Basic direct subworkflow |
| `nested_reverse.yaml` | WB1 reverse | Reverse-mode subworkflow + output flatten |
| `nested_error.yaml` | WB2 error propagation | Child ERROR → parent ERROR |
| `nested_dynamic_wf.yaml` | WB3 | **Dynamic workflow name** `workflow: <% $.wf_name %>` |
| `nested_dynamic_input.yaml` | WB4 | Dynamic name + **dynamic child input** dict |
| `nested_deep_chain.yaml` | WB6 | **3-level chain**, RootExecutionID propagation |
| `my_workbook.yaml` | workbook/v2/my_workbook.yaml | Workbook mode: join, with-items, task-defaults |
| `diamond_join.yaml`, `join_parallel.yaml` | test_with_items.py | Fan-out / join |
| `retry_*.yaml` | test_retry_policy.py | Retry count, delay, break-on, YAQL guards |
| `error_handling.yaml`, `error_recovery_chain.yaml` | test_error_handling.py | On-clauses, when-conditions, failure recovery |
| `publish_chain.yaml` | — | Task publish → next-task input chain |
| `branching_fanout_join.yaml`, `when_join.yaml` | — | Join-by-key with when-gating |

## License

MIT. No affiliation with OpenStack — this is an independent Go reimplementation.
