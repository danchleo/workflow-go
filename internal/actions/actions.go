// Package actions defines the action interface and standard built-in actions.
// Ported from Python reference/actions/std_actions.py
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
	"github.com/mitchellh/mapstructure"
)

// Action is the interface that all Workflow actions must implement.
// Ported from reference_lib.actions.Action
type Action interface {
	// Run executes the action. input is decoded via mapstructure into a
	// typed request struct per action; see individual Run implementations.
	Run(input map[string]interface{}) (*db.Result, error)
	// Name returns the action name.
	Name() string
	// IsSync reports whether the action is synchronous (blocking).
	IsSync() bool
}

// --- shared mapstructure decoder: WeaklyTypedInput lets float64→int ---

var actionDecoderConfig = &mapstructure.DecoderConfig{
	WeaklyTypedInput: true,
	Result:           nil,
}

func decodeActionInput(input map[string]interface{}, out interface{}) error {
	if input == nil {
		input = map[string]interface{}{}
	}
	cfg := *actionDecoderConfig
	cfg.Result = out
	dec, err := mapstructure.NewDecoder(&cfg)
	if err != nil {
		return err
	}
	return dec.Decode(input)
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
	return &EchoAction{Output: "Hello, Workflow!"}
}

func (a *EchoAction) Run(input map[string]interface{}) (*db.Result, error) {
	if a.Delay > 0 {
		time.Sleep(time.Duration(a.Delay * float64(time.Second)))
	}
	return &db.Result{Data: a.Output}, nil
}

func (a *EchoAction) Name() string { return "std.echo" }
func (a *EchoAction) IsSync() bool { return true }

// --- HttpAction ---

// httpActionInput mirrors Python std.http input keys.
type httpActionInput struct {
	Method    string                 `mapstructure:"method"`
	URL       string                 `mapstructure:"url"`
	Body      interface{}            `mapstructure:"body"`
	Headers   map[string]interface{} `mapstructure:"headers"`
	Timeout   float64                `mapstructure:"timeout"`
	Auth      map[string]interface{} `mapstructure:"auth"`
	VerifySSL bool                   `mapstructure:"verify_ssl"`
}

// HttpAction performs an HTTP request.
type HttpAction struct{}

func NewHttpAction() Action { return &HttpAction{} }

func (a *HttpAction) Name() string { return "std.http" }
func (a *HttpAction) IsSync() bool { return true }

func (a *HttpAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in httpActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.http: decode input: %w", err)
	}

	method := in.Method
	if method == "" {
		method = "GET"
	} else {
		method = strings.ToUpper(method)
	}
	if in.URL == "" {
		return &db.Result{Error: "std.http requires 'url' input"}, nil
	}

	var bodyReader io.Reader
	switch b := in.Body.(type) {
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
	if len(in.Headers) > 0 {
		headers = http.Header{}
		for k, v := range in.Headers {
			if s, ok := v.(string); ok {
				headers.Set(k, s)
			}
		}
	}

	timeoutSec := in.Timeout
	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	req, err := http.NewRequest(method, in.URL, bodyReader)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.http: invalid request: %v", err)}, nil
	}
	req.Header = headers

	if len(in.Auth) > 0 {
		u, _ := in.Auth["username"].(string)
		p, _ := in.Auth["password"].(string)
		if u != "" {
			req.SetBasicAuth(u, p)
		}
	}

	client := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &db.Result{
			Error: fmt.Sprintf("std.http request failed: %v", err),
			Data:  map[string]interface{}{"url": in.URL, "method": method},
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
			"url":     in.URL,
		},
	}, nil
}

// --- WorkflowHTTPAction ---

// wfHTTPActionInput mirrors Python std.workflow_http input keys.
type wfHTTPActionInput struct {
	URL      string                 `mapstructure:"url"`
	Workflow string                 `mapstructure:"workflow"`
	Input    map[string]interface{} `mapstructure:"input"`
	Params   map[string]interface{} `mapstructure:"params"`
	Headers  map[string]interface{} `mapstructure:"headers"`
	Timeout  float64                `mapstructure:"timeout"`
}

// WorkflowHTTPAction triggers a workflow in this engine via HTTP.
type WorkflowHTTPAction struct{}

func NewWorkflowHTTPAction() Action { return &WorkflowHTTPAction{} }

func (a *WorkflowHTTPAction) Name() string { return "std.workflow_http" }
func (a *WorkflowHTTPAction) IsSync() bool { return true }

func (a *WorkflowHTTPAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in wfHTTPActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.workflow_http: decode input: %w", err)
	}

	base := in.URL
	if base == "" {
		base = "http://localhost:8989"
	}
	if in.Workflow == "" {
		return &db.Result{Error: "std.workflow_http requires 'workflow' input"}, nil
	}

	payload := map[string]interface{}{
		"workflow_name": in.Workflow,
	}
	if in.Input != nil {
		payload["input"] = in.Input
	}
	if in.Params != nil {
		payload["params"] = in.Params
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.workflow_http: marshal: %v", err)}, nil
	}

	url := strings.TrimRight(base, "/") + "/v2/executions"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.workflow_http: req: %v", err)}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range in.Headers {
		if s, ok := v.(string); ok {
			req.Header.Set(k, s)
		}
	}

	timeoutSec := in.Timeout
	if timeoutSec <= 0 {
		timeoutSec = 30
	}
	client := &http.Client{Timeout: time.Duration(timeoutSec) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &db.Result{
			Error: fmt.Sprintf("std.workflow_http: request failed: %v", err),
			Data:  map[string]interface{}{"url": url, "workflow": in.Workflow},
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

// --- EmailAction ---

// emailActionInput mirrors Python std.email input keys.
type emailActionInput struct {
	From     string `mapstructure:"from"`
	To       string `mapstructure:"to"`
	Subject  string `mapstructure:"subject"`
	Body     string `mapstructure:"body"`
	SMTPHost string `mapstructure:"smtp_host"`
	SMTPPort int    `mapstructure:"smtp_port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	UseTLS   bool   `mapstructure:"use_tls"`
}

// EmailAction sends a simple plain-text email via SMTP (simulated).
type EmailAction struct{}

func NewEmailAction() Action { return &EmailAction{} }

func (a *EmailAction) Name() string { return "std.email" }
func (a *EmailAction) IsSync() bool { return true }

func (a *EmailAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in emailActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.email: decode input: %w", err)
	}

	if in.From == "" {
		return &db.Result{Error: "std.email requires 'from'"}, nil
	}
	if in.To == "" {
		return &db.Result{Error: "std.email requires 'to'"}, nil
	}

	log.Printf("std.email (simulated) — from=%s to=%s subject=%q", in.From, in.To, in.Subject)

	return &db.Result{
		Data: map[string]interface{}{
			"sent":    true,
			"from":    in.From,
			"to":      in.To,
			"subject": in.Subject,
			"body":    in.Body,
		},
	}, nil
}

// --- SSHAction ---

// sshActionInput mirrors Python std.ssh input keys.
type sshActionInput struct {
	Command string `mapstructure:"command"`
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
}

// SSHAction simulates SSH — returns command as stdout.
type SSHAction struct{}

func NewSSHAction() Action { return &SSHAction{} }

func (a *SSHAction) Name() string { return "std.ssh" }
func (a *SSHAction) IsSync() bool { return true }

func (a *SSHAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in sshActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.ssh: decode input: %w", err)
	}
	if in.Port == 0 {
		in.Port = 22
	}

	log.Printf("std.ssh (simulated) — command=%q host=%s:%d", in.Command, in.Host, in.Port)

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":   in.Command,
			"host":     in.Host,
			"port":     in.Port,
			"exitcode": 0,
		},
	}, nil
}

// --- SSHProxiedAction ---

// sshProxiedActionInput mirrors Python std.ssh_proxied input keys.
type sshProxiedActionInput struct {
	Command      string `mapstructure:"command"`
	Host         string `mapstructure:"host"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	PrivateKey   string `mapstructure:"private_key"`
	GatewayHost  string `mapstructure:"gateway_host"`
	GatewayUser  string `mapstructure:"gateway_username"`
	ProxyCommand string `mapstructure:"proxy_command"`
}

// SSHProxiedAction is SSHAction routed through a bastion host.
type SSHProxiedAction struct{}

func NewSSHProxiedAction() Action { return &SSHProxiedAction{} }

func (a *SSHProxiedAction) Name() string { return "std.ssh_proxied" }
func (a *SSHProxiedAction) IsSync() bool { return true }

func (a *SSHProxiedAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in sshProxiedActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.ssh_proxied: decode input: %w", err)
	}

	gatewayUser := in.GatewayUser
	if gatewayUser == "" {
		gatewayUser = in.Username
	}

	log.Printf(
		"std.ssh_proxied (simulated) — command=%q target=%s via %s@%s",
		in.Command, in.Host, gatewayUser, in.GatewayHost,
	)

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":        in.Command,
			"target_host":   in.Host,
			"gateway_host":  in.GatewayHost,
			"gateway_user":  gatewayUser,
			"proxy_command": in.ProxyCommand,
			"exitcode":      0,
		},
	}, nil
}

// --- JavaScriptAction ---

// jsActionInput mirrors Python std.javascript input keys.
type jsActionInput struct {
	Script  string                 `mapstructure:"script"`
	Context map[string]interface{} `mapstructure:"context"`
}

// JavaScriptAction evaluates a JavaScript script with a context map.
type JavaScriptAction struct{}

func NewJavaScriptAction() Action { return &JavaScriptAction{} }

func (a *JavaScriptAction) Name() string { return "std.javascript" }
func (a *JavaScriptAction) IsSync() bool { return true }

func (a *JavaScriptAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in jsActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.javascript: decode input: %w", err)
	}

	if in.Script == "" {
		return &db.Result{Error: "std.javascript requires 'script'"}, nil
	}

	if in.Context == nil {
		in.Context = map[string]interface{}{}
	}

	if strings.HasPrefix(in.Script, "<%") || strings.Contains(in.Script, "${") {
		result := expressions.EvaluateRecursively(in.Script, in.Context)
		return &db.Result{Data: result}, nil
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(in.Script), &parsed); err == nil {
		return &db.Result{Data: parsed}, nil
	}

	log.Printf("std.javascript: no interpreter available, echoing script as-is")
	return &db.Result{Data: in.Script}, nil
}

// --- TestDictAction ---

// testDictActionInput mirrors Python std.test_dict input keys.
type testDictActionInput struct {
	Keys []string `mapstructure:"keys"`
}

// TestDictAction returns a fixed dict useful for testing.
type TestDictAction struct{}

func NewTestDictAction() Action { return &TestDictAction{} }

func (a *TestDictAction) Name() string { return "std.test_dict" }
func (a *TestDictAction) IsSync() bool { return true }

func (a *TestDictAction) Run(input map[string]interface{}) (*db.Result, error) {
	var in testDictActionInput
	if err := decodeActionInput(input, &in); err != nil {
		return nil, fmt.Errorf("std.test_dict: decode input: %w", err)
	}

	if len(in.Keys) > 0 {
		out := map[string]interface{}{}
		for _, k := range in.Keys {
			out[k] = fmt.Sprintf("value-of-%s", k)
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

// NewNoopAction creates a sync noop action.
func NewNoopAction() Action { return &NoopAction{} }

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

// --- SleepAction ---

// sleepActionInput: struct attr SleepAction.Seconds takes priority;
// input["seconds"] is the fallback (matching original behavior).
type sleepActionInput struct {
	Seconds float64 `mapstructure:"seconds"`
}

// SleepAction waits for the given number of seconds.
type SleepAction struct {
	Seconds float64 // struct attribute (higher priority than input)
}

func NewSleepAction() Action {
	return &SleepAction{}
}

func (a *SleepAction) Run(input map[string]interface{}) (*db.Result, error) {
	seconds := a.Seconds
	if seconds <= 0 {
		var in sleepActionInput
		_ = decodeActionInput(input, &in)
		seconds = in.Seconds
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
	return &db.Result{Data: fmt.Sprintf("slept for %v seconds", seconds)}, nil
}
func (a *SleepAction) Name() string { return "std.sleep" }
func (a *SleepAction) IsSync() bool { return true }

// --- WaitSSHAction ---

// waitSSHActionInput mirrors Python std.wait_ssh input keys.
type waitSSHActionInput struct {
	Host    string  `mapstructure:"host"`
	Port    int     `mapstructure:"port"`
	Timeout float64 `mapstructure:"timeout"`
}

// WaitSSHAction simulates SSH wait — sleeps for a configurable amount.
type WaitSSHAction struct {
	Host    string
	Port    int
	Timeout float64
}

func NewWaitSSHAction() Action {
	return &WaitSSHAction{Port: 22, Timeout: 300}
}

func (a *WaitSSHAction) Run(input map[string]interface{}) (*db.Result, error) {
	// struct attrs take priority, input overrides as fallback.
	timeout := a.Timeout
	if timeout <= 0 {
		var in waitSSHActionInput
		_ = decodeActionInput(input, &in)
		timeout = in.Timeout
	}
	if timeout <= 0 {
		timeout = 300
	}
	time.Sleep(time.Duration(timeout * float64(time.Second)))
	return &db.Result{Data: "SSH wait complete"}, nil
}
func (a *WaitSSHAction) Name() string { return "std.wait_ssh" }
func (a *WaitSSHAction) IsSync() bool { return false }

// --- ActionRegistry ---

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
			Name:        "std.workflow_http",
			Description: "Triggers a workflow in this engine via HTTP POST /v2/executions",
			IsSystem:    true,
			Factory: func() Action {
				return NewWorkflowHTTPAction()
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
			Description: "Evaluates a JavaScript expression (falls back to Workflow expressions)",
			IsSystem:    true,
			Factory: func() Action {
				return NewJavaScriptAction()
			},
		},
	)

	r.Register(
		&ActionDescriptor{
			Name:        "std.test_dict",
			Description: "Returns a fixed test dict (standard Workflow testing helper)",
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
	if d, ok := r.Get(name); ok {
		return d, ok
	}
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
// reference_lib.actions.ActionProvider and the CompositeActionProvider facade.
type ActionProvider interface {
	Find(name, namespace string) (*ActionDescriptor, bool)
	List(namespace string) []*ActionDescriptor
}

// CompositeActionProvider chains multiple providers; first hit wins.
type CompositeActionProvider struct {
	Providers []ActionProvider
}

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

func RegistryAsProvider(r *ActionRegistry) ActionProvider {
	return &registryProvider{r: r}
}

type registryProvider struct{ r *ActionRegistry }

func (p *registryProvider) Find(name, namespace string) (*ActionDescriptor, bool) {
	return p.r.Find(name, namespace)
}
func (p *registryProvider) List(namespace string) []*ActionDescriptor { return p.r.List() }

// TestActionProvider allows tests to register ad-hoc action descriptors
// at runtime. Mirrors Python's reference/actions/test.py TestActionProvider.
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

// AdHocAction wraps a base action with resolved input from an ad-hoc
// action definition. Delegates Run() to the underlying base action.
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
// base action with input/output transformations. Currently handles the
// input resolution inline in the task handler — this struct carries base
// name + base-input attributes and delegates instantiation to the underlying
// system descriptor.
type AdHocActionDescriptor struct {
	Name       string
	BaseName   string
	BaseInput  map[string]interface{}
	OutputExpr interface{}
	Namespace  string
	ProjectID  string
	SystemProv ActionProvider
}

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
	baseDesc, _ := p.systemProv.Find(d.BaseName, d.Namespace)
	ahdCopy := *d
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
