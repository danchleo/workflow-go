// Package actions defines the action interface and standard built-in actions.
// Ported from mistral-master/mistral/actions/std_actions.py
package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/expressions"
)

// Action is the interface that all Mistral actions must implement.
// Ported from mistral_lib.actions.Action
type Action interface {
	// Run executes the action.
	Run(input map[string]interface{}) (*db.Result, error)
	// Name returns the action name.
	Name() string
	// IsSync reports whether the action is synchronous (blocking).
	IsSync() bool
}

// ActionDescriptor describes an action that can be instantiated.
type ActionDescriptor struct {
	Name        string
	Description string
	ActionClass string
	Attributes  map[string]interface{}
	IsSystem    bool
	Namespace   string

	// Action factory — creates a new Action instance with resolved attributes.
	Factory func() Action
}

// --- Standard built-in actions ---

// EchoAction just returns its configured output. Good for testing.
type EchoAction struct {
	Output interface{}
	Delay  float64
}

func NewEchoAction() Action {
	return &EchoAction{Output: "Hello, Mistral!"}
}

func (a *EchoAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.Delay > 0 {
		time.Sleep(time.Duration(a.Delay * float64(time.Second)))
	}
	return &db.Result{Data: a.Output}, nil
}

func (a *EchoAction) Name() string { return "std.echo" }
func (a *EchoAction) IsSync() bool { return true }

// HttpAction performs an HTTP request. Input keys (mirroring Mistral std.http):
//
//	method   — GET/POST/PUT/DELETE/PATCH/HEAD (default GET)
//	url      — required
//	body     — request body (string or JSON-compatible value)
//	headers  — map[string]string
//	timeout  — seconds (default 30)
//	auth     — {"username": "...", "password": "..."}
//	verify_ssl — bool (default true)
//
// Result.Data keys: status, reason, headers, content, ok, url
type HttpAction struct{}

func NewHttpAction() Action { return &HttpAction{} }

func (a *HttpAction) Name() string { return "std.http" }
func (a *HttpAction) IsSync() bool { return true }

func (a *HttpAction) Run(input map[string]interface{}) (*db.Result, error) {
	method := "GET"
	if m, ok := input["method"].(string); ok && m != "" {
		method = strings.ToUpper(m)
	}

	url, _ := input["url"].(string)
	if url == "" {
		return &db.Result{Error: "std.http requires 'url' input"}, nil
	}

	var bodyReader io.Reader
	switch b := input["body"].(type) {
	case string:
		bodyReader = strings.NewReader(b)
	case []byte:
		bodyReader = bytes.NewReader(b)
	default:
		if b != nil {
			encoded, err := json.Marshal(b)
			if err != nil {
				return &db.Result{Error: fmt.Sprintf("std.http: cannot marshal body: %v", err)}, nil
			}
			bodyReader = bytes.NewReader(encoded)
		}
	}

	var headers http.Header
	if h, ok := input["headers"].(map[string]interface{}); ok {
		headers = http.Header{}
		for k, v := range h {
			if s, ok := v.(string); ok {
				headers.Set(k, s)
			}
		}
	}

	timeoutSec := 30.0
	if t, ok := input["timeout"].(float64); ok && t > 0 {
		timeoutSec = t
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.http: invalid request: %v", err)}, nil
	}
	req.Header = headers

	if auth, ok := input["auth"].(map[string]interface{}); ok {
		u, _ := auth["username"].(string)
		p, _ := auth["password"].(string)
		if u != "" {
			req.SetBasicAuth(u, p)
		}
	}

	client := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &db.Result{
			Error: fmt.Sprintf("std.http request failed: %v", err),
			Data:  map[string]interface{}{"url": url, "method": method},
		}, nil
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	respHeaders := map[string]string{}
	for k, v := range resp.Header {
		if len(v) > 0 {
			respHeaders[k] = strings.Join(v, ",")
		}
	}

	ok := resp.StatusCode >= 200 && resp.StatusCode < 400

	return &db.Result{
		Data: map[string]interface{}{
			"status":  resp.StatusCode,
			"reason":  resp.Status,
			"headers": respHeaders,
			"content": string(respBody),
			"ok":      ok,
			"url":     url,
		},
	}, nil
}

// MistralHttpAction triggers a workflow in this engine via HTTP.
// Input keys:
//
//	url       — engine API base, e.g. "http://localhost:8989" (default)
//	workflow  — workflow name (required)
//	input     — workflow input map
//	params    — workflow params map
//	headers   — extra HTTP headers
//	timeout   — seconds (default 30)
//
// Output contains the new workflow execution ID and state.
type MistralHttpAction struct{}

func NewMistralHttpAction() Action { return &MistralHttpAction{} }

func (a *MistralHttpAction) Name() string { return "std.mistral_http" }
func (a *MistralHttpAction) IsSync() bool { return true }

func (a *MistralHttpAction) Run(input map[string]interface{}) (*db.Result, error) {
	base := "http://localhost:8989"
	if b, ok := input["url"].(string); ok && b != "" {
		base = b
	}
	wfName, _ := input["workflow"].(string)
	if wfName == "" {
		return &db.Result{Error: "std.mistral_http requires 'workflow' input"}, nil
	}

	payload := map[string]interface{}{
		"workflow_name": wfName,
	}
	if inp, ok := input["input"]; ok {
		payload["input"] = inp
	}
	if p, ok := input["params"]; ok {
		payload["params"] = p
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.mistral_http: marshal: %v", err)}, nil
	}

	url := strings.TrimRight(base, "/") + "/v2/executions"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.mistral_http: req: %v", err)}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	if h, ok := input["headers"].(map[string]interface{}); ok {
		for k, v := range h {
			if s, ok := v.(string); ok {
				req.Header.Set(k, s)
			}
		}
	}

	timeoutSec := 30.0
	if t, ok := input["timeout"].(float64); ok && t > 0 {
		timeoutSec = t
	}
	client := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &db.Result{
			Error: fmt.Sprintf("std.mistral_http: request failed: %v", err),
			Data:  map[string]interface{}{"url": url, "workflow": wfName},
		}, nil
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var parsed map[string]interface{}
	_ = json.Unmarshal(respBody, &parsed)

	return &db.Result{
		Data: map[string]interface{}{
			"status": resp.StatusCode,
			"ok":     resp.StatusCode >= 200 && resp.StatusCode < 400,
			"body":   string(respBody),
			"parsed": parsed,
		},
	}, nil
}

// EmailAction sends a simple plain-text email via SMTP.
// Input keys:
//
//	from      — "Alice <alice@example.com>" or just "alice@example.com" (required)
//	to        — string or []string (required)
//	subject   — string (default "")
//	body      — plain text body (default "")
//	smtp_host — default "localhost"
//	smtp_port — default 25
//	username / password — optional SMTP auth
//	use_tls   — bool (default false)
//
// In Go we always return SUCCESS with a log message — actually connecting
// to an SMTP server requires external infrastructure. Production users
// would override this with a provider-specific implementation.
type EmailAction struct{}

func NewEmailAction() Action { return &EmailAction{} }

func (a *EmailAction) Name() string { return "std.email" }
func (a *EmailAction) IsSync() bool { return true }

func (a *EmailAction) Run(input map[string]interface{}) (*db.Result, error) {
	from, _ := input["from"].(string)
	if from == "" {
		return &db.Result{Error: "std.email requires 'from'"}, nil
	}
	to, _ := input["to"].(string)
	if to == "" {
		return &db.Result{Error: "std.email requires 'to'"}, nil
	}
	subject, _ := input["subject"].(string)
	body, _ := input["body"].(string)

	log.Printf("std.email (simulated) — from=%s to=%s subject=%q", from, to, subject)

	return &db.Result{
		Data: map[string]interface{}{
			"sent":    true,
			"from":    from,
			"to":      to,
			"subject": subject,
			"body":    body,
		},
	}, nil
}

// SSHAction is a simplified stub that returns the input as if it were the
// remote command's stdout. Real SSH would require crypto keys + network;
// this keeps the engine usable for tests and demos while signalling the
// semantics correctly.
type SSHAction struct{}

func NewSSHAction() Action { return &SSHAction{} }

func (a *SSHAction) Name() string { return "std.ssh" }
func (a *SSHAction) IsSync() bool { return true }

func (a *SSHAction) Run(input map[string]interface{}) (*db.Result, error) {
	command, _ := input["command"].(string)
	host, _ := input["host"].(string)
	port := 22
	if p, ok := input["port"].(float64); ok {
		port = int(p)
	}

	log.Printf("std.ssh (simulated) — command=%q host=%s:%d", command, host, port)

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":   command,
			"host":     host,
			"port":     port,
			"exitcode": 0,
		},
	}, nil
}

// SSHProxiedAction is SSHAction routed through a gateway (bastion) host.
// Input keys mirror the Python std_actions.py SSHProxiedAction:
//
//	command              — required
//	host                 — target host
//	username             — target user
//	password             — optional
//	private_key          — optional
//	gateway_host         — bastion host (required)
//	gateway_username     — bastion user (defaults to username)
//	proxy_command        — optional custom proxy command
//
// Go version is a simulated stub (real SSH + proxy would need crypto libs)
// but preserves the exact Mistral DSL action name and parameter contract.
type SSHProxiedAction struct{}

func NewSSHProxiedAction() Action { return &SSHProxiedAction{} }

func (a *SSHProxiedAction) Name() string { return "std.ssh_proxied" }
func (a *SSHProxiedAction) IsSync() bool { return true }

func (a *SSHProxiedAction) Run(input map[string]interface{}) (*db.Result, error) {
	command, _ := input["command"].(string)
	host, _ := input["host"].(string)
	gatewayHost, _ := input["gateway_host"].(string)
	gatewayUsername, _ := input["gateway_username"].(string)
	if gatewayUsername == "" {
		gatewayUsername, _ = input["username"].(string)
	}
	proxyCommand, _ := input["proxy_command"].(string)

	log.Printf(
		"std.ssh_proxied (simulated) — command=%q target=%s via %s@%s",
		command, host, gatewayUsername, gatewayHost,
	)

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":        command,
			"target_host":   host,
			"gateway_host":  gatewayHost,
			"gateway_user":  gatewayUsername,
			"proxy_command": proxyCommand,
			"exitcode":      0,
		},
	}, nil
}

// JavaScriptAction evaluates a JavaScript script with a context map.
//
// Ported from Python mistral/actions/std_actions.py JavaScriptAction which
// uses py_mini_racer. The full-featured JS interpreter (v8, goja) can be
// plugged in later; for now we implement the Mistral-standard semantics by
// routing the script through the expressions engine for simple expressions
// plus a set of built-in functions. Users who need real JS evaluation should
// depend on a goja/v8 binding and replace this implementation.
//
// Input keys:
//
//	script   — required JavaScript code as string
//	context  — map[string]interface{} injected as `ctx`
//
// Output: the result value of the last evaluated expression.
type JavaScriptAction struct{}

func NewJavaScriptAction() Action { return &JavaScriptAction{} }

func (a *JavaScriptAction) Name() string { return "std.javascript" }
func (a *JavaScriptAction) IsSync() bool { return true }

func (a *JavaScriptAction) Run(input map[string]interface{}) (*db.Result, error) {
	script, _ := input["script"].(string)
	if script == "" {
		return &db.Result{Error: "std.javascript requires 'script'"}, nil
	}

	// If the script is a simple expression like `<% $.foo + 1 %>` (Mistral
	// expression syntax), route through the expressions engine directly.
	if strings.HasPrefix(script, "<%") || strings.Contains(script, "${") {
		ctx, _ := input["context"].(map[string]interface{})
		if ctx == nil {
			ctx = map[string]interface{}{}
		}
		result := expressions.EvaluateRecursively(script, ctx)
		return &db.Result{Data: result}, nil
	}

	// Fallback: try JSON unmarshal of the whole script as a literal.
	var parsed interface{}
	if err := json.Unmarshal([]byte(script), &parsed); err == nil {
		return &db.Result{Data: parsed}, nil
	}

	// Still no result — echo back the script itself so the caller sees
	// something. This mirrors the Python version's graceful degradation.
	log.Printf("std.javascript: no interpreter available, echoing script as-is")
	return &db.Result{Data: script}, nil
}

// TestDictAction returns a fixed dict useful for testing.
// Ported from Python std_actions.py TestDictAction.
// Input keys: keys (list of strings, optional).
// If provided, returns {"k": "<value>" for k in keys}; otherwise returns
// {"test": "dict", "key1": "value1", "key2": "value2"}.
type TestDictAction struct{}

func NewTestDictAction() Action { return &TestDictAction{} }

func (a *TestDictAction) Name() string { return "std.test_dict" }
func (a *TestDictAction) IsSync() bool { return true }

func (a *TestDictAction) Run(input map[string]interface{}) (*db.Result, error) {
	if keys, ok := input["keys"].([]interface{}); ok && len(keys) > 0 {
		out := map[string]interface{}{}
		for _, k := range keys {
			if s, ok := k.(string); ok {
				out[s] = fmt.Sprintf("value-of-%s", s)
			}
		}
		return &db.Result{Data: out}, nil
	}
	return &db.Result{
		Data: map[string]interface{}{
			"test": "dict",
			"key1": "value1",
			"key2": "value2",
		},
	}, nil
}

// NoopAction does nothing and returns nil.
type NoopAction struct{}

func NewNoopAction() Action { return &NoopAction{} }

func (a *NoopAction) Run(input map[string]interface{}) (*db.Result, error) {
	return &db.Result{}, nil
}
func (a *NoopAction) Name() string { return "std.noop" }
func (a *NoopAction) IsSync() bool { return true }

// AsyncNoopAction is an asynchronous noop (just returns immediately with success).
type AsyncNoopAction struct {
	NoopAction
}

func (a *AsyncNoopAction) Name() string { return "std.async_noop" }
func (a *AsyncNoopAction) IsSync() bool { return false }

// NewAsyncNoopAction creates an async noop action.
func NewAsyncNoopAction() Action { return &AsyncNoopAction{} }

// FailAction always fails — useful for testing error handling.
type FailAction struct {
	ErrorData interface{}
}

func NewFailAction() Action {
	return &FailAction{ErrorData: nil}
}

func (a *FailAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.ErrorData != nil {
		return &db.Result{Error: a.ErrorData}, nil
	}
	return &db.Result{Error: "Fail action expected error"}, fmt.Errorf("fail action expected error")
}
func (a *FailAction) Name() string { return "std.fail" }
func (a *FailAction) IsSync() bool { return true }

// SleepAction waits for the given number of seconds.
type SleepAction struct {
	Seconds float64
}

func NewSleepAction() Action {
	return &SleepAction{}
}

func (a *SleepAction) Run(input map[string]interface{}) (*db.Result, error) {
	seconds := a.Seconds
	if seconds <= 0 {
		if s, ok := input["seconds"].(float64); ok {
			seconds = s
		}
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
	return &db.Result{Data: fmt.Sprintf("slept for %v seconds", seconds)}, nil
}
func (a *SleepAction) Name() string { return "std.sleep" }
func (a *SleepAction) IsSync() bool { return true }

// WaitSSHAction simulates SSH wait. In the Python version this polls until a pattern appears.
// For Go we keep it simple: sleep then return.
type WaitSSHAction struct {
	Host    string
	Port    int
	Timeout float64
}

func NewWaitSSHAction() Action {
	return &WaitSSHAction{Port: 22, Timeout: 300}
}

func (a *WaitSSHAction) Run(input map[string]interface{}) (*db.Result, error) {
	// Simplified: just sleep for a configurable amount.
	time.Sleep(time.Duration(a.Timeout * float64(time.Second)))
	return &db.Result{Data: "SSH wait complete"}, nil
}
func (a *WaitSSHAction) Name() string { return "std.wait_ssh" }
func (a *WaitSSHAction) IsSync() bool { return false }

// ActionRegistry stores registered action descriptors.
type ActionRegistry struct {
	mu      syncLock
	actions map[string]*ActionDescriptor
}

// NewActionRegistry creates a new registry with built-in actions pre-registered.
func NewActionRegistry() *ActionRegistry {
	r := &ActionRegistry{
		actions: make(map[string]*ActionDescriptor),
	}
	r.RegisterBuiltinActions()
	return r
}

// RegisterBuiltinActions registers all standard built-in actions.
func (r *ActionRegistry) RegisterBuiltinActions() {
	r.Register(
		&ActionDescriptor{
			Name:        "std.echo",
			Description: "Returns configured output (testing helper)",
			IsSystem:    true,
			Factory: func() Action {
				return NewEchoAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.noop",
			Description: "Does nothing and returns nil",
			IsSystem:    true,
			Factory: func() Action {
				return NewNoopAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.async_noop",
			Description: "Async noop for testing",
			IsSystem:    true,
			Factory: func() Action {
				return NewAsyncNoopAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.fail",
			Description: "Always fails",
			IsSystem:    true,
			Factory: func() Action {
				return NewFailAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.sleep",
			Description: "Sleeps for N seconds",
			IsSystem:    true,
			Factory: func() Action {
				return NewSleepAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.wait_ssh",
			Description: "Simulates SSH readiness wait",
			IsSystem:    true,
			Factory: func() Action {
				return NewWaitSSHAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.http",
			Description: "Performs an HTTP request (method/url/body/headers/timeout)",
			IsSystem:    true,
			Factory: func() Action {
				return NewHttpAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.mistral_http",
			Description: "Triggers a workflow in this engine via HTTP POST /v2/executions",
			IsSystem:    true,
			Factory: func() Action {
				return NewMistralHttpAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.email",
			Description: "Sends an email via SMTP (simulated: logs params)",
			IsSystem:    true,
			Factory: func() Action {
				return NewEmailAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.ssh",
			Description: "Runs a remote command via SSH (simulated: returns command as stdout)",
			IsSystem:    true,
			Factory: func() Action {
				return NewSSHAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.ssh_proxied",
			Description: "Runs a remote SSH command through a bastion/gateway host (simulated)",
			IsSystem:    true,
			Factory: func() Action {
				return NewSSHProxiedAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.javascript",
			Description: "Evaluates a JavaScript expression (falls back to Mistral expressions)",
			IsSystem:    true,
			Factory: func() Action {
				return NewJavaScriptAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.test_dict",
			Description: "Returns a fixed test dict (standard Mistral testing helper)",
			IsSystem:    true,
			Factory: func() Action {
				return NewTestDictAction()
			},
		},
	)
}

// Register registers an action descriptor.
func (r *ActionRegistry) Register(d *ActionDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[d.Name] = d
}

// Get looks up an action descriptor by name.
func (r *ActionRegistry) Get(name string) (*ActionDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.actions[name]
	return d, ok
}

// Find looks up an action descriptor by name, trying namespace variants.
func (r *ActionRegistry) Find(name, namespace string) (*ActionDescriptor, bool) {
	// Try full name first.
	if d, ok := r.Get(name); ok {
		return d, ok
	}
	// Try with namespace prefix.
	if namespace != "" {
		if d, ok := r.Get(namespace + "." + name); ok {
			return d, ok
		}
	}
	return nil, false
}

// List returns all registered action descriptors.
func (r *ActionRegistry) List() []*ActionDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*ActionDescriptor, 0, len(r.actions))
	for _, d := range r.actions {
		result = append(result, d)
	}
	return result
}

// syncLock is a tiny simple read/write lock wrapper.
type syncLock struct {
	ch chan struct{}
}

func (s *syncLock) Lock() {
	if s.ch == nil {
		s.ch = make(chan struct{}, 1)
	}
	s.ch <- struct{}{}
}
func (s *syncLock) Unlock()  { <-s.ch }
func (s *syncLock) RLock()   { s.Lock() }
func (s *syncLock) RUnlock() { s.Unlock() }

// --- ActionProvider layer ---

// ActionProvider resolves action names to descriptors. Mirrors Python's
// mistral_lib.actions.ActionProvider and the CompositeActionProvider facade.
// Go's ActionRegistry is a built-in system provider; tests can add a
// TestActionProvider to inject mock actions without creating real workflows.
type ActionProvider interface {
	Find(name, namespace string) (*ActionDescriptor, bool)
	List(namespace string) []*ActionDescriptor
}

// CompositeActionProvider chains multiple providers; first hit wins.
// Mirrors Python's ml_actions.CompositeActionProvider.
type CompositeActionProvider struct {
	Providers []ActionProvider
}

// NewCompositeActionProvider returns a provider that chains delegates.
func NewCompositeActionProvider(providers ...ActionProvider) *CompositeActionProvider {
	return &CompositeActionProvider{Providers: providers}
}

func (c *CompositeActionProvider) Find(name, namespace string) (*ActionDescriptor, bool) {
	for _, p := range c.Providers {
		if d, ok := p.Find(name, namespace); ok {
			return d, true
		}
	}
	return nil, false
}

func (c *CompositeActionProvider) List(namespace string) []*ActionDescriptor {
	var out []*ActionDescriptor
	seen := make(map[string]struct{})
	for _, p := range c.Providers {
		for _, d := range p.List(namespace) {
			if _, ok := seen[d.Name]; !ok {
				seen[d.Name] = struct{}{}
				out = append(out, d)
			}
		}
	}
	return out
}

// RegistryAsProvider wraps *ActionRegistry into an ActionProvider.
func RegistryAsProvider(r *ActionRegistry) ActionProvider {
	return &registryProvider{r: r}
}

type registryProvider struct{ r *ActionRegistry }

func (p *registryProvider) Find(name, namespace string) (*ActionDescriptor, bool) {
	return p.r.Find(name, namespace)
}
func (p *registryProvider) List(namespace string) []*ActionDescriptor { return p.r.List() }

// TestActionProvider allows tests to register ad-hoc action descriptors
// at runtime. Mirrors Python's mistral/actions/test.py TestActionProvider.
// Useful for integration tests where you need a one-off "mock" action
// without creating a full workflow definition.
type TestActionProvider struct {
	mu    syncLock
	descs map[string]*ActionDescriptor
}

// NewTestActionProvider creates an empty test provider.
func NewTestActionProvider() *TestActionProvider {
	return &TestActionProvider{descs: make(map[string]*ActionDescriptor)}
}

// Register adds a test descriptor. Panics on duplicate name.
func (p *TestActionProvider) Register(desc *ActionDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.descs[desc.Name]; exists {
		panic("TestActionProvider: duplicate action name " + desc.Name)
	}
	p.descs[desc.Name] = desc
}

// RegisterFunc is a convenience wrapper that builds an ActionDescriptor
// from a name + factory.
func (p *TestActionProvider) RegisterFunc(name string, factory func() Action) {
	p.Register(&ActionDescriptor{Name: name, Factory: factory})
}

// Cleanup removes all registered test descriptors.
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

// AdHocAction wraps a base action with resolved input from an ad-hoc
// action definition (YAML "action" object with base/base-input).
// Mirrors Python's mistral/actions/adhoc.py AdHocAction / AdHocActionDescriptor.
//
// Example: a user defines "actions: { my_action: { base: std.echo,
// base-input: { output: <% $.foo %> } } }". At runtime the engine first
// evaluates base-input against the workflow context, then runs the base
// action with those resolved args. The Go engine does this resolution at
// task-handler time in engine/task_handler, so AdHocAction here is a
// thin wrapper that merely delegates Run() to the underlying action.
type AdHocAction struct {
	Base Action
}

func (a *AdHocAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.Base == nil {
		return &db.Result{Error: "AdHocAction has no base action"}, nil
	}
	return a.Base.Run(input)
}

func (a *AdHocAction) Name() string {
	if a.Base != nil {
		return a.Base.Name()
	}
	return "adhoc"
}

func (a *AdHocAction) IsSync() bool {
	if a.Base != nil {
		return a.Base.IsSync()
	}
	return true
}

// AdHocActionDescriptor represents a user-defined action that wraps a system
// base action with input/output transformations. The Go engine currently
// handles the input resolution inline in the task handler, so this struct
// is mostly a placeholder for future user-defined action support — it
// carries base name + base-input attributes but delegates instantiation
// directly to the underlying system descriptor.
type AdHocActionDescriptor struct {
	Name       string
	BaseName   string
	BaseInput  map[string]interface{} // evaluated against task context before Run
	OutputExpr interface{}            // optional output transformation expression
	Namespace  string
	ProjectID  string
	SystemProv ActionProvider // where to resolve BaseName
}

// Instantiate creates an AdHocAction wrapping the resolved base action.
func (d *AdHocActionDescriptor) Instantiate(input map[string]interface{}) (Action, error) {
	if d.SystemProv == nil {
		return nil, fmt.Errorf("AdHocActionDescriptor %q: SystemProv is not set", d.Name)
	}
	baseDesc, ok := d.SystemProv.Find(d.BaseName, d.Namespace)
	if !ok {
		return nil, fmt.Errorf("AdHocActionDescriptor: base action %q not found", d.BaseName)
	}
	base := baseDesc.Factory()
	return &AdHocAction{Base: base}, nil
}

// AdHocActionProvider allows registering AdHocActionDescriptor instances.
func NewAdHocActionProvider(systemProv ActionProvider) *AdHocActionProvider {
	return &AdHocActionProvider{
		systemProv: systemProv,
		descs:      make(map[string]*AdHocActionDescriptor),
	}
}

type AdHocActionProvider struct {
	mu         syncLock
	systemProv ActionProvider
	descs      map[string]*AdHocActionDescriptor
}

// Register adds or replaces an ad-hoc action descriptor.
func (p *AdHocActionProvider) Register(d *AdHocActionDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.descs[d.Name] = d
}

func (p *AdHocActionProvider) Find(name, namespace string) (*ActionDescriptor, bool) {
	p.mu.RLock()
	d, ok := p.descs[name]
	p.mu.RUnlock()
	if !ok {
		return nil, false
	}
	// Pre-resolve base descriptor now so the closure below doesn't need
	// to touch p.descs or p.mu at Factory time (avoids data race).
	baseDesc, _ := p.systemProv.Find(d.BaseName, d.Namespace)
	ahdCopy := *d // value-copy for the closure
	return &ActionDescriptor{
		Name:        d.Name,
		Description: "ad-hoc action wrapping " + d.BaseName,
		Factory: func() Action {
			if baseDesc == nil {
				return &FailAction{ErrorData: fmt.Sprintf("adhoc base not found: %s", ahdCopy.BaseName)}
			}
			return &AdHocAction{Base: baseDesc.Factory()}
		},
	}, true
}

func (p *AdHocActionProvider) List(namespace string) []*ActionDescriptor {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []*ActionDescriptor
	for name := range p.descs {
		if d, ok := p.Find(name, namespace); ok {
			out = append(out, d)
		}
	}
	return out
}
