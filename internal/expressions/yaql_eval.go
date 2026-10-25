package expressions

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// yaqEval evaluates a YAQL AST node against a context map.
type yaqEval struct {
	ctx      map[string]interface{}
	builtins map[string]BuiltinFunc
}

// BuiltinFunc is the signature for a registered YAQL builtin function.
type BuiltinFunc func(args []interface{}) (interface{}, error)

// NewYaqEval creates a YAQL evaluator with Mistral-standard builtins.
func NewYaqEval(ctx map[string]interface{}) *yaqEval {
	e := &yaqEval{ctx: ctx, builtins: map[string]BuiltinFunc{}}
	e.registerStd()
	return e
}

func (e *yaqEval) register(name string, fn BuiltinFunc) {
	e.builtins[name] = fn
}

// RegisterBuiltin allows external callers to inject additional functions.
func RegisterBuiltin(name string, fn BuiltinFunc) {
	// stored globally for now; we can scope later if needed
	globalBuiltins[name] = fn
}

var globalBuiltins = map[string]BuiltinFunc{}

func (e *yaqEval) registerStd() {
	// task() → lookup task result from context["__task_results"] or single arg
	e.register("task", yaqTask)
	e.register("env", yaqEnv)
	e.register("execution", yaqExecution)
	e.register("range", yaqRange)
	e.register("uuid_", yaqUUID)
	e.register("json_dump_", yaqJSONDump)
	e.register("json_parse_", yaqJSONParse)
	e.register("yaml_dump_", yaqJSONDump) // yaml_dump_ → reuse json for now
	e.register("yaml_parse_", yaqJSONParse)
	e.register("global_", yaqGlobal)
	e.register("length", yaqLength)
	e.register("first", yaqFirst)
	e.register("len", yaqLength)
	e.register("sum", yaqSum)
	e.register("abs", yaqAbs)
	e.register("list", yaqList)
	e.register("dict", yaqDict)
	// plus any globally injected ones
	for k, v := range globalBuiltins {
		e.register(k, v)
	}
}

// Eval is the top-level eval entry.
func (e *yaqEval) Eval(n *node) (interface{}, error) {
	return e.evalNode(n)
}

func (e *yaqEval) evalNode(n *node) (interface{}, error) {
	switch n.kind {
	case nkLiteral:
		return n.val, nil
	case nkVar:
		return e.evalVar(n.varName)
	case nkMember:
		return e.evalMember(n.expr, n.name)
	case nkIndex:
		return e.evalIndex(n.expr, n.args[0])
	case nkCall:
		return e.evalCall(n)
	case nkUnary:
		return e.evalUnary(n.op, n.expr)
	case nkBinary:
		return e.evalBinary(n.op, n.expr, n.args[0])
	case nkIfElse:
		return e.evalIfElse(n.args[0], n.args[1], n.args[2])
	case nkList:
		return e.evalList(n.list)
	case nkDict:
		return e.evalDict(n.dict)
	}
	return nil, fmt.Errorf("unknown node kind %d", n.kind)
}

func (e *yaqEval) evalVar(name string) (interface{}, error) {
	if name == "$" || name == "$$" {
		return e.ctx, nil
	}
	// $name
	key := strings.TrimPrefix(name, "$")
	// 支持 __env, __execution, __task_execution 这些特殊键
	if v, ok := e.ctx[key]; ok {
		return v, nil
	}
	if v, ok := e.ctx[name]; ok {
		return v, nil
	}
	// Python YAQL: bare identifier not bound in context returns its name as string.
	// This lets task(task1) work like task('task1') — the bare identifier auto-coerces.
	return name, nil
}

func (e *yaqEval) evalMember(target *node, name string) (interface{}, error) {
	v, err := e.evalNode(target)
	if err != nil {
		return nil, err
	}
	return deepGet(v, name)
}

func (e *yaqEval) evalIndex(target *node, idxNode *node) (interface{}, error) {
	v, err := e.evalNode(target)
	if err != nil {
		return nil, err
	}
	idx, err := e.evalNode(idxNode)
	if err != nil {
		return nil, err
	}
	return indexGet(v, idx)
}

func (e *yaqEval) evalCall(n *node) (interface{}, error) {
	args := make([]interface{}, 0, len(n.args))
	for _, a := range n.args {
		v, err := e.evalNode(a)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}

	if n.expr != nil {
		// method-style call: target.method(args)
		target, err := e.evalNode(n.expr)
		if err != nil {
			return nil, err
		}
		// treat as builtin with implicit this prepended
		fn, ok := e.builtins[n.name]
		if !ok {
			return nil, fmt.Errorf("unknown method %s()", n.name)
		}
		return fn(append([]interface{}{target}, args...))
	}

	// plain function call: func(args)
	fn, ok := e.builtins[n.name]
	if !ok {
		return nil, fmt.Errorf("unknown function %s()", n.name)
	}
	return fn(args)
}

func (e *yaqEval) evalUnary(op tokType, n *node) (interface{}, error) {
	v, err := e.evalNode(n)
	if err != nil {
		return nil, err
	}
	switch op {
	case tokMinus:
		return negNum(v)
	case tokPlus:
		f, ok := toFloat(v)
		if !ok {
			return nil, fmt.Errorf("cannot apply unary + to %T", v)
		}
		return f, nil
	case tokNot:
		return !toBool(v), nil
	}
	return nil, fmt.Errorf("unknown unary op %s", op)
}

func (e *yaqEval) evalBinary(op tokType, leftN *node, rightN *node) (interface{}, error) {
	// short-circuit for and/or
	if op == tokAnd {
		lv, err := e.evalNode(leftN)
		if err != nil {
			return nil, err
		}
		if !toBool(lv) {
			return false, nil
		}
		rv, err := e.evalNode(rightN)
		if err != nil {
			return nil, err
		}
		return toBool(rv), nil
	}
	if op == tokOr {
		lv, err := e.evalNode(leftN)
		if err != nil {
			return nil, err
		}
		if toBool(lv) {
			return true, nil
		}
		rv, err := e.evalNode(rightN)
		if err != nil {
			return nil, err
		}
		return toBool(rv), nil
	}

	lv, err := e.evalNode(leftN)
	if err != nil {
		return nil, err
	}
	rv, err := e.evalNode(rightN)
	if err != nil {
		return nil, err
	}

	switch op {
	case tokPlus:
		// string + anything
		if ls, ok := lv.(string); ok {
			return ls + fmt.Sprintf("%v", rv), nil
		}
		if rs, ok := rv.(string); ok {
			return fmt.Sprintf("%v", lv) + rs, nil
		}
		return arithOp(lv, rv, '+')
	case tokMinus:
		return arithOp(lv, rv, '-')
	case tokStar:
		return arithOp(lv, rv, '*')
	case tokSlash:
		return arithOp(lv, rv, '/')
	case tokPercent:
		return arithOp(lv, rv, '%')
	case tokEq:
		return deepEqual(lv, rv), nil
	case tokNeq:
		return !deepEqual(lv, rv), nil
	case tokGt:
		return cmpNum(lv, rv) > 0, nil
	case tokLt:
		return cmpNum(lv, rv) < 0, nil
	case tokGte:
		return cmpNum(lv, rv) >= 0, nil
	case tokLte:
		return cmpNum(lv, rv) <= 0, nil
	case tokIn:
		return isIn(lv, rv), nil
	}
	return nil, fmt.Errorf("unknown binary op %s", op)
}

func (e *yaqEval) evalIfElse(condN, thenN, elseN *node) (interface{}, error) {
	c, err := e.evalNode(condN)
	if err != nil {
		return nil, err
	}
	if toBool(c) {
		return e.evalNode(thenN)
	}
	return e.evalNode(elseN)
}

func (e *yaqEval) evalList(items []*node) ([]interface{}, error) {
	out := make([]interface{}, 0, len(items))
	for _, it := range items {
		v, err := e.evalNode(it)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (e *yaqEval) evalDict(pairs []kv) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(pairs))
	for _, p := range pairs {
		k, err := e.evalNode(p.key)
		if err != nil {
			return nil, err
		}
		v, err := e.evalNode(p.val)
		if err != nil {
			return nil, err
		}
		out[fmt.Sprintf("%v", k)] = v
	}
	return out, nil
}

// ---------- Helpers ----------

func deepGet(v interface{}, name string) (interface{}, error) {
	switch x := v.(type) {
	case map[string]interface{}:
		if val, ok := x[name]; ok {
			return val, nil
		}
		return nil, nil
	case map[interface{}]interface{}:
		for k, val := range x {
			if fmt.Sprintf("%v", k) == name {
				return val, nil
			}
		}
		return nil, nil
	}
	return nil, nil
}

func indexGet(v interface{}, idx interface{}) (interface{}, error) {
	switch x := v.(type) {
	case []interface{}:
		i, ok := toInt(idx)
		if !ok {
			return nil, fmt.Errorf("array index must be int")
		}
		if i < 0 || i >= len(x) {
			return nil, fmt.Errorf("index %d out of range (len %d)", i, len(x))
		}
		return x[i], nil
	case map[string]interface{}:
		key := fmt.Sprintf("%v", idx)
		if val, ok := x[key]; ok {
			return val, nil
		}
		return nil, nil
	case map[interface{}]interface{}:
		for k, val := range x {
			if fmt.Sprintf("%v", k) == fmt.Sprintf("%v", idx) {
				return val, nil
			}
		}
		return nil, nil
	}
	return nil, fmt.Errorf("cannot index into %T", v)
}

func toInt(v interface{}) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		return int(x), true
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	}
	return 0, false
}

func toFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func negNum(v interface{}) (interface{}, error) {
	switch x := v.(type) {
	case int:
		return -x, nil
	case int64:
		return -x, nil
	case float64:
		return -x, nil
	}
	return nil, fmt.Errorf("cannot negate %T", v)
}

func arithOp(l, r interface{}, op rune) (interface{}, error) {
	li, lok := toInt(l)
	ri, rok := toInt(r)
	if lok && rok {
		switch op {
		case '+':
			return li + ri, nil
		case '-':
			return li - ri, nil
		case '*':
			return li * ri, nil
		case '/':
			if ri == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return float64(li) / float64(ri), nil
		case '%':
			if ri == 0 {
				return nil, fmt.Errorf("mod by zero")
			}
			return li % ri, nil
		}
	}
	lf, lok := toFloat(l)
	rf, rok := toFloat(r)
	if lok && rok {
		switch op {
		case '+':
			return lf + rf, nil
		case '-':
			return lf - rf, nil
		case '*':
			return lf * rf, nil
		case '/':
			if rf == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return lf / rf, nil
		case '%':
			return math.Mod(lf, rf), nil
		}
	}
	return nil, fmt.Errorf("cannot %c %T and %T", op, l, r)
}

func cmpNum(l, r interface{}) int {
	lf, lok := toFloat(l)
	rf, rok := toFloat(r)
	if lok && rok {
		switch {
		case lf < rf:
			return -1
		case lf > rf:
			return 1
		}
		return 0
	}
	// fallback: string compare
	ls := fmt.Sprintf("%v", l)
	rs := fmt.Sprintf("%v", r)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	}
	return 0
}

func deepEqual(a, b interface{}) bool {
	// numbers: normalize to float
	af, aOK := toFloat(a)
	bf, bOK := toFloat(b)
	if aOK && bOK {
		return af == bf
	}
	// slices
	if as, ok := a.([]interface{}); ok {
		if bs, ok2 := b.([]interface{}); ok2 {
			if len(as) != len(bs) {
				return false
			}
			for i := range as {
				if !deepEqual(as[i], bs[i]) {
					return false
				}
			}
			return true
		}
		return false
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x != ""
	case []interface{}:
		return len(x) > 0
	case map[string]interface{}:
		return len(x) > 0
	}
	f, ok := toFloat(v)
	if ok {
		return f != 0
	}
	return true
}

func isIn(needle, haystack interface{}) bool {
	switch h := haystack.(type) {
	case []interface{}:
		for _, it := range h {
			if deepEqual(it, needle) {
				return true
			}
		}
	case string:
		n, ok := needle.(string)
		if ok {
			return strings.Contains(h, n)
		}
	case map[string]interface{}:
		_, ok := h[fmt.Sprintf("%v", needle)]
		return ok
	}
	return false
}

// ---------- Standard builtins ----------

func yaqTask(args []interface{}) (interface{}, error) {
	// task() → current task (via __task_execution.name or __execution.current_task_id)
	// task("name") → named task result
	ctx := currentEvalCtx()
	if len(args) == 0 {
		if cur, ok := ctx["__current_task"]; ok {
			return cur, nil
		}
		return nil, nil
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("task() takes 0 or 1 arg")
	}

	// If the arg is already a taskData map (bare identifier collided with
	// ctx["taskName"]), return it directly — it IS the task result the user wants.
	if m, ok := args[0].(map[string]interface{}); ok {
		return m, nil
	}

	name := fmt.Sprintf("%v", args[0])
	if results, ok := ctx["__task_results"]; ok {
		if m, ok := results.(map[string]interface{}); ok {
			if v, ok2 := m[name]; ok2 {
				return v, nil
			}
		}
	}
	return nil, nil
}

// currentEvalCtx is set temporarily during builtin evaluation so builtins
// like task() / env() / execution() can access context without threading.
var currentCtxHolder map[string]interface{}

func currentEvalCtx() map[string]interface{} {
	return currentCtxHolder
}

func yaqEnv(args []interface{}) (interface{}, error) {
	ctx := currentEvalCtx()
	if v, ok := ctx["__env"]; ok {
		return v, nil
	}
	return map[string]interface{}{}, nil
}

func yaqExecution(args []interface{}) (interface{}, error) {
	ctx := currentEvalCtx()
	if v, ok := ctx["__execution"]; ok {
		return v, nil
	}
	return map[string]interface{}{}, nil
}

func yaqRange(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("range() takes 1 arg")
	}
	n, ok := toInt(args[0])
	if !ok {
		return nil, fmt.Errorf("range(arg) must be int")
	}
	out := make([]interface{}, n)
	for i := 0; i < n; i++ {
		out[i] = i
	}
	return out, nil
}

func yaqUUID(args []interface{}) (interface{}, error) {
	return uuid.New().String(), nil
}

func yaqJSONDump(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("json_dump_() takes 1 arg")
	}
	b, err := json.Marshal(args[0])
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func yaqJSONParse(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("json_parse_() takes 1 arg")
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("json_parse_ arg must be string")
	}
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

func yaqGlobal(args []interface{}) (interface{}, error) {
	ctx := currentEvalCtx()
	if v, ok := ctx["__global"]; ok {
		return v, nil
	}
	return nil, nil
}

func yaqLength(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("length() takes 1 arg")
	}
	v := args[0]
	switch x := v.(type) {
	case []interface{}:
		return len(x), nil
	case string:
		return len(x), nil
	case map[string]interface{}:
		return len(x), nil
	}
	return 0, nil
}

func yaqFirst(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("first() takes 1 arg")
	}
	if slice, ok := args[0].([]interface{}); ok {
		if len(slice) == 0 {
			return nil, nil
		}
		return slice[0], nil
	}
	return nil, nil
}

func yaqSum(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sum() takes 1 arg")
	}
	slice, ok := args[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("sum() arg must be list")
	}
	total := 0.0
	allInt := true
	for _, it := range slice {
		f, ok := toFloat(it)
		if !ok {
			return nil, fmt.Errorf("sum() element not numeric")
		}
		if _, isInt := it.(int); !isInt {
			allInt = false
		}
		total += f
	}
	if allInt {
		return int(total), nil
	}
	return total, nil
}

func yaqAbs(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("abs() takes 1 arg")
	}
	f, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("abs() arg must be numeric")
	}
	if fi, ok := args[0].(int); ok {
		if fi < 0 {
			return -fi, nil
		}
		return fi, nil
	}
	if f < 0 {
		return -f, nil
	}
	return f, nil
}

func yaqList(args []interface{}) (interface{}, error) {
	out := make([]interface{}, len(args))
	copy(out, args)
	return out, nil
}

func yaqDict(args []interface{}) (interface{}, error) {
	m := make(map[string]interface{})
	for i := 0; i+1 < len(args); i += 2 {
		m[fmt.Sprintf("%v", args[i])] = args[i+1]
	}
	return m, nil
}

// ---------- Top-level API ----------

// EvaluateYAQL evaluates the given YAQL expression string against context.
func EvaluateYAQL(expr string, ctx map[string]interface{}) (interface{}, error) {
	n, err := ParseYAQL(expr)
	if err != nil {
		return nil, err
	}
	ev := NewYaqEval(ctx)
	// set the context holder so builtins can reach it
	prev := currentCtxHolder
	currentCtxHolder = ctx
	defer func() { currentCtxHolder = prev }()
	return ev.Eval(n)
}

// --- exported helpers used elsewhere ---

// SortedKeys returns sorted keys of a string-keyed map (convenience).
func SortedKeys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
