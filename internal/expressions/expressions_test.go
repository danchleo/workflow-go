package expressions

import "testing"

func TestIsYAQLExpression(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"<% $.input.name %>", true},
		{"no markers here", false},
		{"{{ $.x }}", false}, // Jinja, not YAQL
		{"", false},
	}
	for _, tt := range tests {
		if got := IsYAQLExpression(tt.input); got != tt.want {
			t.Errorf("IsYAQLExpression(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestEvaluateDotPath(t *testing.T) {
	ctx := map[string]interface{}{
		"input": map[string]interface{}{
			"name":    "Mistral",
			"greeting": "Hi",
		},
		"env": map[string]interface{}{
			"region": "us-east-1",
		},
	}

	e := NewSimpleEvaluator()

	// Single-level path.
	result, err := e.Evaluate("<% $.input.name %>", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result != "Mistral" {
		t.Errorf("got %v (%T), want Mistral", result, result)
	}

	// Different key.
	result, _ = e.Evaluate("<% $.env.region %>", ctx)
	if result != "us-east-1" {
		t.Errorf("got %v, want us-east-1", result)
	}
}

func TestEvaluateEntireStringIsExpression(t *testing.T) {
	ctx := map[string]interface{}{
		"input": map[string]interface{}{
			"msg": "hello",
		},
	}
	e := NewSimpleEvaluator()

	// When the ENTIRE string is an expression, we want the typed value back.
	result, _ := e.Evaluate("<% $.input.msg %>", ctx)
	if result != "hello" {
		t.Errorf("got %v (%T), want string hello", result, result)
	}
}

func TestEvaluateLiteral(t *testing.T) {
	e := NewSimpleEvaluator()

	result, _ := e.Evaluate("plain text", nil)
	if result != "plain text" {
		t.Errorf("got %v, want plain text", result)
	}
}

func TestEvaluateRecursivelyMap(t *testing.T) {
	ctx := map[string]interface{}{
		"input": map[string]interface{}{"x": 42},
	}
	data := map[string]interface{}{
		"greeting": "<% $.input.x %>",
		"nested": map[string]interface{}{
			"deep": "<% $.input.x %>",
		},
		"literal": "abc",
	}

	result := EvaluateRecursively(data, ctx)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatal("result is not a map")
	}
	if m["greeting"] != 42 {
		t.Errorf("greeting = %v, want 42", m["greeting"])
	}
	nested, _ := m["nested"].(map[string]interface{})
	if nested["deep"] != 42 {
		t.Errorf("nested.deep = %v, want 42", nested["deep"])
	}
	if m["literal"] != "abc" {
		t.Errorf("literal = %v, want abc", m["literal"])
	}
}

func TestEvaluateJinjaExpression(t *testing.T) {
	ctx := map[string]interface{}{
		"vars": map[string]interface{}{"name": "World"},
	}
	e := NewSimpleEvaluator()

	result, _ := e.Evaluate("{{ $.vars.name }}", ctx)
	if result != "World" {
		t.Errorf("got %v, want World", result)
	}
}
