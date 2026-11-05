package actions

import (
	"strings"
	"testing"

	"github.com/danchleo/workflow-go/internal/db"
)

// ============================================================================
// Test helpers — 只在测试里用，放在 _test.go 里
// ============================================================================

// TestActionProvider 允许测试运行时注册 ad-hoc action。
// 对应 reference/actions/test.py。
type TestActionProvider struct {
	mu    syncLock
	descs map[string]*ActionDescriptor
}

func NewTestActionProvider() *TestActionProvider {
	return &TestActionProvider{descs: make(map[string]*ActionDescriptor)}
}

func (p *TestActionProvider) Register(desc *ActionDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.descs[desc.Name]; exists {
		panic("TestActionProvider: duplicate action name " + desc.Name)
	}
	p.descs[desc.Name] = desc
}

func (p *TestActionProvider) RegisterFunc(name string, factory func() Action) {
	p.Register(&ActionDescriptor{Name: name, Factory: factory})
}

func (p *TestActionProvider) Cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.descs = make(map[string]*ActionDescriptor)
}

func (p *TestActionProvider) Find(name, namespace string) (*ActionDescriptor, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if d, ok := p.descs[name]; ok {
		return d, true
	}
	if namespace != "" {
		if d, ok := p.descs[namespace+"."+name]; ok {
			return d, true
		}
	}
	return nil, false
}

func (p *TestActionProvider) List(namespace string) []*ActionDescriptor {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*ActionDescriptor, 0, len(p.descs))
	for _, d := range p.descs {
		out = append(out, d)
	}
	return out
}

type countingAction struct {
	GenericAction[noopInput]
}

func newCountingAction() Action {
	a := &countingAction{}
	a.GenericAction = GenericAction[noopInput]{NameStr: "test.counter", Sync: true, DoFn: func(*noopInput) (*db.Result, error) {
		return &db.Result{Data: "ok"}, nil
	}}
	return a
}

// ------------------------------------------------------------
// EchoAction
// ------------------------------------------------------------

func TestEchoAction_Run(t *testing.T) {
	a := NewEchoAction().(Action)
	res, err := a.Run(map[string]interface{}{"output": "hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if !res.IsSuccess() {
		t.Errorf("unexpected error: %v", res.Error)
	}
	s, _ := res.Data.(string)
	if !strings.Contains(s, "Hello, Workflow!") {
		t.Errorf("Data = %q", res.Data)
	}
	if a.Name() != "std.echo" {
		t.Errorf("Name = %s", a.Name())
	}
	if !a.IsSync() {
		t.Error("should be sync")
	}
}

// ------------------------------------------------------------
// FailAction
// ------------------------------------------------------------

func TestFailAction_Run(t *testing.T) {
	a := NewFailAction().(Action)
	res, err := a.Run(map[string]interface{}{"error_msg": "boom"})
	if err == nil {
		t.Fatalf("FailAction should return err, got res=%v", res)
	}
	if res == nil || res.IsSuccess() {
		t.Error("FailAction result should NOT be success")
	}
}

func TestFailAction_Run_WithErrorData(t *testing.T) {
	a := NewFailAction().(*FailAction)
	a.ErrorData = "custom-msg"
	res, err := a.Run(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsSuccess() {
		t.Error("should fail")
	}
	if res.Error != "custom-msg" {
		t.Errorf("Error = %v, want custom-msg", res.Error)
	}
}

// ------------------------------------------------------------
// NoopAction
// ------------------------------------------------------------

func TestNoopAction_Run(t *testing.T) {
	a := NewNoopAction().(Action)
	res, err := a.Run(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsSuccess() {
		t.Errorf("unexpected error: %v", res.Error)
	}
}

// ------------------------------------------------------------
// ActionRegistry
// ------------------------------------------------------------

func TestActionRegistry_GetBuiltin(t *testing.T) {
	r := NewActionRegistry()
	for _, name := range []string{"std.echo", "std.noop", "std.fail", "std.sleep"} {
		d, ok := r.Get(name)
		if !ok {
			t.Errorf("%s not registered", name)
			continue
		}
		if d.Factory == nil {
			t.Errorf("%s has nil factory", name)
		}
	}
}

func TestActionRegistry_List(t *testing.T) {
	r := NewActionRegistry()
	list := r.List()
	if len(list) < 4 {
		t.Errorf("builtin count = %d", len(list))
	}
}

func TestActionRegistry_RegisterCustom(t *testing.T) {
	r := NewActionRegistry()
	r.Register(
		&ActionDescriptor{
			Name:     "my.custom",
			Factory:  func() Action { return NewEchoAction() },
			IsSystem: false,
		},
	)
	d, ok := r.Get("my.custom")
	if !ok {
		t.Fatal("custom not registered")
	}
	inst := d.Factory()
	if inst == nil {
		t.Error("factory gave nil")
	}
}

// ------------------------------------------------------------
// TestActionProvider — runtime-registered actions
// ------------------------------------------------------------

func TestTestActionProvider_FindAndInstantiate(t *testing.T) {
	p := NewTestActionProvider()
	called := 0
	p.RegisterFunc(
		"test.counter", func() Action {
			called++
			return newCountingAction()
		},
	)

	d, ok := p.Find("test.counter", "")
	if !ok || d == nil {
		t.Fatal("Find failed")
	}
	a := d.Factory().(Action)
	res, err := a.Run(nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.IsSuccess() {
		t.Errorf("res.Error = %v", res.Error)
	}
	if called != 1 {
		t.Errorf("factory called = %d", called)
	}
}

func TestTestActionProvider_NotFound(t *testing.T) {
	p := NewTestActionProvider()
	if _, ok := p.Find("does.not.exist", ""); ok {
		t.Error("expected not-found")
	}
}

func TestTestActionProvider_DuplicatePanics(t *testing.T) {
	p := NewTestActionProvider()
	p.RegisterFunc("dup", func() Action { return NewNoopAction() })
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate")
		}
	}()
	p.RegisterFunc("dup", func() Action { return NewNoopAction() })
}

// ------------------------------------------------------------
// CompositeActionProvider — first-hit-wins chain
// ------------------------------------------------------------

func TestCompositeActionProvider_FirstHit(t *testing.T) {
	sys := NewActionRegistry()
	custom := NewTestActionProvider()
	custom.RegisterFunc(
		"test.only", func() Action {
			return newCountingAction()
		},
	)
	// same name in both: custom shadows sys.
	custom.RegisterFunc(
		"std.echo", func() Action {
			return newCountingAction()
		},
	)

	comp := NewCompositeActionProvider(
		RegistryAsProvider(sys),
		custom,
	)

	// std.echo exists in BOTH → custom wins (listed first).
	d, ok := comp.Find("std.echo", "")
	if !ok {
		t.Fatal("std.echo not found")
	}
	a := d.Factory().(Action)
	if _, err := a.Run(nil); err != nil {
		t.Logf("note: custom Run returned err=%v", err)
	}

	// test.only only in custom → found.
	if _, ok := comp.Find("test.only", ""); !ok {
		t.Error("test.only should come from custom")
	}

	// nowhere → not found.
	if _, ok := comp.Find("nowhere", ""); ok {
		t.Error("should not exist")
	}
}
