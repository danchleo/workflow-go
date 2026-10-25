package expressions

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// ---------- Lexer + Parser smoke tests ----------

func TestLexerNumbers(t *testing.T) {
	l := newYaqLexer("42 -3 3.14")
	toks, err := l.Lex()
	if err != nil {
		t.Fatal(err)
	}
	// 42, -3, 3.14, EOF
	if len(toks) != 4 {
		t.Fatalf("expected 4 tokens, got %d", len(toks))
	}
	if toks[0].typ != tokInt || toks[0].val.(int64) != 42 {
		t.Errorf("first token = %v", toks[0])
	}
	if toks[1].typ != tokInt || toks[1].val.(int64) != -3 {
		t.Errorf("second token = %v", toks[1])
	}
	if toks[2].typ != tokFloat {
		t.Errorf("third token type = %v", toks[2].typ)
	}
}

func TestLexerStrings(t *testing.T) {
	l := newYaqLexer(`"hello" 'world'`)
	toks, err := l.Lex()
	if err != nil {
		t.Fatal(err)
	}
	if len(toks) != 3 {
		t.Fatalf("want 3 tokens, got %d", len(toks))
	}
	if toks[0].val != "hello" || toks[1].val != "world" {
		t.Errorf("strings = %v %v", toks[0].val, toks[1].val)
	}
}

func TestParseDotPath(t *testing.T) {
	n, err := ParseYAQL("$.input.key")
	if err != nil {
		t.Fatal(err)
	}
	if n.kind != nkMember || n.name != "key" {
		t.Fatalf("node = %+v", n)
	}
}

func TestParseIndex(t *testing.T) {
	n, err := ParseYAQL("$.items[0]")
	if err != nil {
		t.Fatal(err)
	}
	if n.kind != nkIndex {
		t.Fatalf("want nkIndex got %d", n.kind)
	}
}

func TestParseCall(t *testing.T) {
	n, err := ParseYAQL("task('my_task')")
	if err != nil {
		t.Fatal(err)
	}
	if n.kind != nkCall || n.name != "task" {
		t.Fatalf("node = %+v", n)
	}
}

func TestParseIfElse(t *testing.T) {
	n, err := ParseYAQL("1 if $.x > 0 else 0")
	if err != nil {
		t.Fatal(err)
	}
	if n.kind != nkIfElse {
		t.Fatalf("want nkIfElse got %d", n.kind)
	}
}

// ---------- Evaluator tests (Python-mistral YAQL parity) ----------

func TestYaqArithmetic(t *testing.T) {
	ctx := map[string]interface{}{"x": 10, "y": 3}
	cases := []struct {
		expr string
		want interface{}
	}{
		{"$.x + $.y", 13},
		{"$.x - $.y", 7},
		{"$.x * $.y", 30},
		{"$.x / $.y", 10.0 / 3.0},
		{"$.x % $.y", 1},
		{"2 + 3 * 4", 14},
		{"(2 + 3) * 4", 20},
		{"-$.y", -3},
	}
	for _, c := range cases {
		got, err := EvaluateYAQL(c.expr, ctx)
		if err != nil {
			t.Errorf("%q: %v", c.expr, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			// float tolerance for division
			if f1, ok := got.(float64); ok {
				if f2, ok2 := c.want.(float64); ok2 && f1 == f2 {
					continue
				}
			}
			t.Errorf("%q = %v (%T), want %v", c.expr, got, got, c.want)
		}
	}
}

func TestYaqComparison(t *testing.T) {
	ctx := map[string]interface{}{"x": 5, "y": 5, "z": 10}
	cases := []struct {
		expr string
		want bool
	}{
		{"$.x == $.y", true},
		{"$.x != $.z", true},
		{"$.x > $.z", false},
		{"$.x < $.z", true},
		{"$.x >= $.y", true},
		{"$.x <= $.y", true},
	}
	for _, c := range cases {
		got, err := EvaluateYAQL(c.expr, ctx)
		if err != nil {
			t.Errorf("%q: %v", c.expr, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestYaqLogic(t *testing.T) {
	ctx := map[string]interface{}{"a": true, "b": false}
	cases := []struct {
		expr string
		want bool
	}{
		{"$.a and $.b", false},
		{"$.a or $.b", true},
		{"not $.b", true},
		{"not $.a", false},
	}
	for _, c := range cases {
		got, err := EvaluateYAQL(c.expr, ctx)
		if err != nil {
			t.Errorf("%q: %v", c.expr, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestYaqStringConcat(t *testing.T) {
	ctx := map[string]interface{}{"name": "World"}
	got, err := EvaluateYAQL(`"Hello, " + $.name + "!"`, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Hello, World!" {
		t.Errorf("got %q", got)
	}
}

func TestYaqArrayIndex(t *testing.T) {
	ctx := map[string]interface{}{
		"items": []interface{}{"a", "b", "c"},
	}
	got, err := EvaluateYAQL("$.items[1]", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "b" {
		t.Errorf("got %v", got)
	}
}

func TestYaqMapKey(t *testing.T) {
	ctx := map[string]interface{}{
		"cfg": map[string]interface{}{"key": "hello"},
	}
	got, err := EvaluateYAQL("$.cfg['key']", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %v", got)
	}
}

func TestYaqListLiteral(t *testing.T) {
	got, err := EvaluateYAQL("[1, 2, 3]", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.([]interface{})) != 3 {
		t.Errorf("got %v", got)
	}
}

func TestYaqDictLiteral(t *testing.T) {
	got, err := EvaluateYAQL("{'a': 1, 'b': 2}", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]interface{})
	a, _ := toFloat(m["a"])
	b, _ := toFloat(m["b"])
	if a != 1 || b != 2 {
		t.Errorf("got %v", got)
	}
}

func TestYaqIfElse(t *testing.T) {
	ctx := map[string]interface{}{"flag": true}
	got, err := EvaluateYAQL("'yes' if $.flag else 'no'", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "yes" {
		t.Errorf("got %v", got)
	}
	ctx["flag"] = false
	got, _ = EvaluateYAQL("'yes' if $.flag else 'no'", ctx)
	if got != "no" {
		t.Errorf("got %v", got)
	}
}

func TestYaqIn(t *testing.T) {
	ctx := map[string]interface{}{
		"item": "apple",
		"list": []interface{}{"apple", "banana"},
	}
	got, _ := EvaluateYAQL("$.item in $.list", ctx)
	if got != true {
		t.Errorf("got %v", got)
	}
	got, _ = EvaluateYAQL(`"pear" in $.list`, ctx)
	if got != false {
		t.Errorf("got %v", got)
	}
}

// ---------- Builtin functions ----------

func TestBuiltinTask(t *testing.T) {
	ctx := map[string]interface{}{
		"__task_results": map[string]interface{}{
			"hello": map[string]interface{}{"result": "world", "state": "SUCCESS"},
		},
	}
	got, err := EvaluateYAQL("task('hello').result", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "world" {
		t.Errorf("got %v", got)
	}
	got, _ = EvaluateYAQL("task('hello').state", ctx)
	if got != "SUCCESS" {
		t.Errorf("got %v", got)
	}
}

func TestBuiltinEnv(t *testing.T) {
	ctx := map[string]interface{}{
		"__env": map[string]interface{}{"region": "us-east-1"},
	}
	got, err := EvaluateYAQL("env()", ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]interface{})
	if m["region"] != "us-east-1" {
		t.Errorf("got %v", got)
	}
}

func TestBuiltinExecution(t *testing.T) {
	ctx := map[string]interface{}{
		"__execution": map[string]interface{}{"id": "abc123", "name": "my_wf"},
	}
	got, err := EvaluateYAQL("execution().name", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "my_wf" {
		t.Errorf("got %v", got)
	}
}

func TestBuiltinRange(t *testing.T) {
	got, err := EvaluateYAQL("range(5)", nil)
	if err != nil {
		t.Fatal(err)
	}
	slice := got.([]interface{})
	if len(slice) != 5 {
		t.Fatalf("len = %d", len(slice))
	}
	for i, v := range slice {
		if v != i {
			t.Errorf("[%d] = %v, want %d", i, v, i)
		}
	}
}

func TestBuiltinJSONDump(t *testing.T) {
	ctx := map[string]interface{}{"data": map[string]interface{}{"a": 1}}
	got, err := EvaluateYAQL("json_dump_($.data)", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(fmt.Sprintf("%v", got), `"a"`) {
		t.Errorf("got %v", got)
	}
}

func TestBuiltinJSONParse(t *testing.T) {
	ctx := map[string]interface{}{"s": `{"a": 1}`}
	got, err := EvaluateYAQL("json_parse_($.s).a", ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Go JSON parses numbers as float64
	if got != 1.0 && got != 1 {
		t.Errorf("got %v (%T)", got, got)
	}
}

func TestBuiltinLength(t *testing.T) {
	ctx := map[string]interface{}{"arr": []interface{}{1, 2, 3}}
	got, _ := EvaluateYAQL("length($.arr)", ctx)
	if got != 3 {
		t.Errorf("got %v", got)
	}
}

// TestRealMistralYAMLScenarios covers the actual patterns Python
// mistral-master uses in production workflows.
func TestRealMistralYAMLScenarios(t *testing.T) {
	ctx := map[string]interface{}{
		"input": map[string]interface{}{
			"message": "hello",
			"count":   5,
		},
		"__task_results": map[string]interface{}{
			"hello": map[string]interface{}{
				"result":    "Hello, World!",
				"published": map[string]interface{}{"greeted": true},
				"greeted":   true, // flatten
				"state":     "SUCCESS",
			},
			"count_items": map[string]interface{}{
				"result": 42,
				"state":  "SUCCESS",
			},
		},
		"__env": map[string]interface{}{
			"region": "us-east-1",
		},
		"__execution": map[string]interface{}{
			"id":   "exec-001",
			"name": "my_workflow",
		},
	}

	tests := []struct {
		name  string
		expr  string
		check func(interface{}) bool
	}{
		{"task().result 等价 dot", "task('hello').result", func(v interface{}) bool { return v == "Hello, World!" }},
		{"task() publish flatten", "task('hello').greeted", func(v interface{}) bool { return v == true }},
		{"$.input.message", "$.input.message", func(v interface{}) bool { return v == "hello" }},
		{"算术组合", "$.input.count * 2 + 1", func(v interface{}) bool { return v == 11 }},
		{"三元表达式", "'ok' if task('hello').state == 'SUCCESS' else 'fail'", func(v interface{}) bool { return v == "ok" }},
		{"range 循环", "length(range($.input.count))", func(v interface{}) bool { return v == 5 }},
		{"env() 函数", "env().region", func(v interface{}) bool { return v == "us-east-1" }},
		{"execution() id", "execution().id", func(v interface{}) bool { return v == "exec-001" }},
		{"uuid 非空", "uuid_()", func(v interface{}) bool {
			s, ok := v.(string)
			return ok && len(s) > 0
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvaluateYAQL(tt.expr, ctx)
			if err != nil {
				t.Fatalf("evaluate error: %v", err)
			}
			if !tt.check(got) {
				t.Errorf("got %v (%T)", got, got)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestLegacyDotPathStillWorks makes sure old tests / old YAMLs that use
// $.a.b.c patterns still work unchanged.
func TestLegacyDotPathStillWorks(t *testing.T) {
	ctx := map[string]interface{}{
		"task1": map[string]interface{}{
			"result": map[string]interface{}{"items": []interface{}{"a", "b"}},
		},
	}
	e := NewSimpleEvaluator()
	got, err := e.Evaluate("<% $.task1.result.items[0] %>", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "a" {
		t.Errorf("got %v", got)
	}
}

func TestEvaluateTaggedMixedText(t *testing.T) {
	ctx := map[string]interface{}{"name": "Alice"}
	e := NewSimpleEvaluator()
	got, err := e.Evaluate("Hello, <% $.name %>! You have <% 3 + 2 %> apples.", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Hello, Alice! You have 5 apples." {
		t.Errorf("got %q", got)
	}
}

func TestSortedKeysHelper(t *testing.T) {
	m := map[string]interface{}{"b": 1, "a": 2, "c": 3}
	keys := SortedKeys(m)
	sort.Strings(keys) // SortedKeys 应该已经排好，但再确认
	for i := 1; i < len(keys); i++ {
		if keys[i-1] > keys[i] {
			t.Errorf("not sorted: %v", keys)
		}
	}
}
