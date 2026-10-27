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
	YAQLExpression  = "yaql"  // <% expression %>
	JinjaExpression = "jinja" // {{ expression }}
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

// SimpleEvaluator is the full YAQL-capable evaluator. It supports:
//   - $.path, $.path[0], $.map[key]  member/index access
//   - task("name").result, env(), execution(), range(n), uuid_() ...
//   - +, -, *, /, %, ==, !=, >, <, >=, <=, and, or, not, in
//   - a if cond else b  conditional expression
//   - [a, b, c]  list literals, {k: v}  dict literals
//
// Jinja ({{ ... }}) and YAQL (<% ... %>) markers both invoke this evaluator;
// the underlying YAQL engine is what actually does the work.
type SimpleEvaluator struct{}

// NewSimpleEvaluator creates a new evaluator backed by the YAQL engine.
func NewSimpleEvaluator() *SimpleEvaluator {
	return &SimpleEvaluator{}
}

// Validate parses the expression to check syntax.
func (e *SimpleEvaluator) Validate(expression string) error {
	if expression == "" {
		return fmt.Errorf("empty expression")
	}
	inner := unwrapFirst(expression)
	if inner == "" {
		return nil // plain text, not an expression
	}
	_, err := ParseYAQL(inner)
	return err
}

// IsExpression delegates to package-level function.
func (e *SimpleEvaluator) IsExpression(expression string) bool {
	return IsExpression(expression)
}

// unwrapFirst returns the first YAQL or Jinja inner content, or empty if
// neither marker is present.
func unwrapFirst(expr string) string {
	if m := yaqlPattern.FindStringSubmatchIndex(expr); len(m) > 0 {
		return strings.TrimSpace(expr[m[2]:m[3]])
	}
	if m := jinjaPattern.FindStringSubmatchIndex(expr); len(m) > 0 {
		return strings.TrimSpace(expr[m[2]:m[3]])
	}
	return ""
}

// Evaluate evaluates the expression against context.
func (e *SimpleEvaluator) Evaluate(expression string, context map[string]interface{}) (interface{}, error) {
	if context == nil {
		context = make(map[string]interface{})
	}

	switch ExpressionType(expression) {
	case YAQLExpression:
		return e.evaluateTagged(expression, context, yaqlPattern)
	case JinjaExpression:
		return e.evaluateTagged(expression, context, jinjaPattern)
	default:
		// Not an expression, return as-is.
		return expression, nil
	}
}

func (e *SimpleEvaluator) evaluateTagged(expr string, ctx map[string]interface{}, pat *regexp.Regexp) (interface{}, error) {
	matches := pat.FindAllStringSubmatchIndex(expr, -1)

	// Single-expression case: <% ... %> alone → return the typed value.
	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(expr) {
		inner := strings.TrimSpace(expr[matches[0][2]:matches[0][3]])
		return EvaluateYAQL(inner, ctx)
	}

	// Multiple expressions or mixed with literal text → string-interpolate.
	result := expr
	for _, m := range matches {
		full := expr[m[0]:m[1]]
		inner := strings.TrimSpace(expr[m[2]:m[3]])
		val, err := EvaluateYAQL(inner, ctx)
		if err != nil {
			return nil, err
		}
		result = strings.Replace(result, full, fmt.Sprintf("%v", val), 1)
	}
	return result, nil
}

// ---------- Package-level entrypoints ----------

// Global default evaluator — set once at package init (NewSimpleEvaluator),
// read-only thereafter. Safe for concurrent use.
var defaultEvaluator Evaluator = NewSimpleEvaluator()

// GetDefaultEvaluator returns the current default evaluator.
func GetDefaultEvaluator() Evaluator { return defaultEvaluator }

// EvaluateBool evaluates the expression and coerces the result to bool.
// Used for "when:" conditions in on-clauses — returns true on error
// (fail-open matches Python mistral's resilient expression evaluation).
func EvaluateBool(expr string, ctx map[string]interface{}) (bool, error) {
	val, err := defaultEvaluator.Evaluate(expr, ctx)
	if err != nil {
		return true, err
	}
	return truthy(val), nil
}

func truthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x != ""
	case float64:
		return x != 0
	case int:
		return x != 0
	default:
		return true // non-nil non-zero → truthy
	}
}

// Recursively evaluates any expression found in data (string, map, slice).
func EvaluateRecursively(data interface{}, context map[string]interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch d := data.(type) {
	case string:
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
