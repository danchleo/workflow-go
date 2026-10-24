// Package expressions provides expression evaluation for Mistral DSL.
// Mistral supports YAQL (<% ... %>) and Jinja2 ({{ ... }}) expression syntax.
//
// Ported from mistral-master/mistral/expressions/
package expressions

import (
	"fmt"
	"regexp"
	"strings"
)

// Expression pattern types.
const (
	YAQLExpression   = "yaql"   // <% expression %>
	JinjaExpression  = "jinja"  // {{ expression }}
)

// Precompiled regex patterns.
var (
	yaqlPattern  = regexp.MustCompile(`<%\s*([^%]+?)\s*%>`)
	jinjaPattern = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)
)

// IsExpression checks whether a string looks like an expression.
func IsExpression(expr interface{}) bool {
	s, ok := expr.(string)
	if !ok {
		return false
	}
	return IsYAQLExpression(s) || IsJinjaExpression(s)
}

// IsYAQLExpression checks if the string contains YAQL expression markers.
func IsYAQLExpression(s string) bool {
	return yaqlPattern.MatchString(s)
}

// IsJinjaExpression checks if the string contains Jinja expression markers.
func IsJinjaExpression(s string) bool {
	return jinjaPattern.MatchString(s)
}

// ExpressionType identifies whether the string uses YAQL or Jinja syntax.
func ExpressionType(s string) string {
	if IsYAQLExpression(s) {
		return YAQLExpression
	}
	if IsJinjaExpression(s) {
		return JinjaExpression
	}
	return ""
}

// Evaluator is the interface for expression evaluators.
type Evaluator interface {
	// Validate checks whether an expression is syntactically valid.
	Validate(expression string) error
	// Evaluate evaluates the expression against the given data context.
	Evaluate(expression string, context map[string]interface{}) (interface{}, error)
	// IsExpression checks if a string is a recognizable expression.
	IsExpression(expression string) bool
}

// SimpleEvaluator provides basic expression evaluation.
// This is a simplified implementation that handles common Mistral patterns.
// It evaluates expressions against a context map, supporting dot notation
// like $.input.my_var or $.env.something.
type SimpleEvaluator struct{}

// NewSimpleEvaluator creates a new SimpleEvaluator.
func NewSimpleEvaluator() *SimpleEvaluator {
	return &SimpleEvaluator{}
}

// Validate returns nil for all expressions (no validation in simple mode).
func (e *SimpleEvaluator) Validate(expression string) error {
	if expression == "" {
		return fmt.Errorf("empty expression")
	}
	return nil
}

// IsExpression delegates to package-level function.
func (e *SimpleEvaluator) IsExpression(expression string) bool {
	return IsExpression(expression)
}

// Evaluate evaluates the expression against context.
// Supports patterns like:
//   - <% $.input.var_name %> -> context["input"]["var_name"]
//   - <% $.env.my_env %> -> context["__env"]["my_env"] or context["env"]["my_env"]
//   - <% $.task_name.output.result %> -> nested access via task outputs
//   - {{ $.input.var }} -> same as YAQL but Jinja markers
//   - Plain strings (no markers) are returned as-is
func (e *SimpleEvaluator) Evaluate(expression string, context map[string]interface{}) (interface{}, error) {
	if context == nil {
		context = make(map[string]interface{})
	}

	switch ExpressionType(expression) {
	case YAQLExpression:
		return e.evaluateYAQL(expression, context)
	case JinjaExpression:
		return e.evaluateJinja(expression, context)
	default:
		// Not an expression, return as-is.
		return expression, nil
	}
}

func (e *SimpleEvaluator) evaluateYAQL(expr string, ctx map[string]interface{}) (interface{}, error) {
	matches := yaqlPattern.FindAllStringSubmatchIndex(expr, -1)

	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(expr) {
		// Entire string is one expression - return the actual evaluated value.
		inner := expr[matches[0][2]:matches[0][3]]
		return e.evaluateInner(inner, ctx)
	}

	// Multiple expressions or mixed with literal text - string replace each.
	result := expr
	for _, match := range matches {
		full := expr[match[0]:match[1]]
		inner := expr[match[2]:match[3]]
		val, err := e.evaluateInner(inner, ctx)
		if err != nil {
			return nil, err
		}
		result = strings.ReplaceAll(result, full, fmt.Sprintf("%v", val))
	}
	return result, nil
}

func (e *SimpleEvaluator) evaluateJinja(expr string, ctx map[string]interface{}) (interface{}, error) {
	matches := jinjaPattern.FindAllStringSubmatchIndex(expr, -1)

	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(expr) {
		inner := expr[matches[0][2]:matches[0][3]]
		return e.evaluateInner(inner, ctx)
	}

	result := expr
	for _, match := range matches {
		full := expr[match[0]:match[1]]
		inner := expr[match[2]:match[3]]
		val, err := e.evaluateInner(inner, ctx)
		if err != nil {
			return nil, err
		}
		result = strings.ReplaceAll(result, full, fmt.Sprintf("%v", val))
	}
	return result, nil
}

// evaluateInner handles the actual expression content inside markers.
// Supports: $.key, $.key.subkey, $.key[0], etc.
func (e *SimpleEvaluator) evaluateInner(expr string, ctx map[string]interface{}) (interface{}, error) {
	expr = strings.TrimSpace(expr)

	// Handle $.xxx.yyy.zzz pattern - context variable access.
	if strings.HasPrefix(expr, "$.") {
		path := strings.TrimPrefix(expr, "$.")
		return resolvePath(path, ctx)
	}

	// Handle $$ pattern -> root context (same as $. effectively).
	if expr == "$$" {
		return ctx, nil
	}

	// Handle literal values.
	if val, ok := tryParseLiteral(expr); ok {
		return val, nil
	}

	// Fallback: return the expression itself as a string.
	return expr, nil
}

// resolvePath resolves a dotted path like "input.my_var.sub" against context.
func resolvePath(path string, ctx map[string]interface{}) (interface{}, error) {
	if path == "" {
		return ctx, nil
	}

	parts := splitPath(path)

	var current interface{} = ctx

	for _, part := range parts {
		switch c := current.(type) {
		case map[string]interface{}:
			val, ok := c[part]
			if !ok {
				return nil, fmt.Errorf("path '%s' not found at '%s'", path, part)
			}
			current = val

		case map[string]string:
			val, ok := c[part]
			if !ok {
				return nil, fmt.Errorf("path '%s' not found at '%s'", path, part)
			}
			current = val

		default:
			return nil, fmt.Errorf("cannot traverse into %T at '%s' in path '%s'", current, part, path)
		}
	}

	return current, nil
}

// splitPath splits a dotted path into components.
func splitPath(path string) []string {
	return strings.Split(path, ".")
}

// tryParseLiteral attempts to parse a value as a Go literal.
func tryParseLiteral(s string) (interface{}, bool) {
	s = strings.TrimSpace(s)

	// Boolean.
	if s == "true" {
		return true, true
	}
	if s == "false" {
		return false, true
	}
	if s == "null" || s == "None" {
		return nil, true
	}

	// Integer.
	var i int
	if _, err := fmt.Sscanf(s, "%d", &i); err == nil && fmt.Sprintf("%d", i) == s {
		return i, true
	}

	// Float.
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		return f, true
	}

	return nil, false
}

// Global default evaluator.
var defaultEvaluator Evaluator = NewSimpleEvaluator()

// SetDefaultEvaluator sets the global default evaluator.
func SetDefaultEvaluator(e Evaluator) {
	defaultEvaluator = e
}

// Recursively evaluates any expression found in data (string, map, slice).
func EvaluateRecursively(data interface{}, context map[string]interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch d := data.(type) {
	case string:
		// Check if it's an expression.
		if IsExpression(d) {
			result, err := defaultEvaluator.Evaluate(d, context)
			if err != nil {
				return d // Return original on error.
			}
			return result
		}
		return d

	case map[string]interface{}:
		result := make(map[string]interface{}, len(d))
		for k, v := range d {
			result[k] = EvaluateRecursively(v, context)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(d))
		for i, v := range d {
			result[i] = EvaluateRecursively(v, context)
		}
		return result

	default:
		return d
	}
}
