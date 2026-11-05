// Package actions defines the action interface and standard built-in actions.
// Ported from Python reference/actions/std_actions.py
package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/expressions"
	"github.com/dop251/goja"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/crypto/ssh"
)

// Action is the interface that all Workflow actions must implement.
// Ported from reference_lib.actions.Action
// Action 是所有 action 的统一接口。
// 所有标准 action 都嵌入 GenericAction[T] 自动获得完整实现。
type Action interface {
	// Run executes the action. input is decoded via mapstructure into a
	// typed request struct per action; see individual Run implementations.
	Run(input map[string]interface{}) (*db.Result, error)
	// Name returns the action name (e.g. "std.http").
	Name() string
	// IsSync reports whether the action is synchronous (blocking).
	IsSync() bool
	// Description 人类可读的描述（可选）。
	Description() string
	// IsSystem 是否是内置系统 action。
	IsSystem() bool
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

// ActionDescriptor 是 Registry 内部存储的壳。
// 所有字段都可以从 Action 接口实现自动提取，注册时不需要手动填。
// 保留它作为返回值类型是因为 Registry.Get 返回它；外部如果需要 Attributes/ActionClass/Namespace
// 这些不常变化的元信息，可以在 Register 时手动传入覆盖。
type ActionDescriptor struct {
	Name        string
	Description string
	ActionClass string
	Attributes  map[string]interface{}
	IsSystem    bool
	Namespace   string

	// Factory 创建 action 实例。
	Factory func() Action
}

// makeDescriptor 从 Factory 返回的 Action 实例自动填充 Name/Description/IsSystem，
// 已显式设置的字段不会被覆盖。
func makeDescriptor(desc ActionDescriptor) ActionDescriptor {
	if desc.Factory != nil {
		if a := desc.Factory(); a != nil {
			if desc.Name == "" {
				desc.Name = a.Name()
			}
			if desc.Description == "" {
				desc.Description = a.Description()
			}
			if !desc.IsSystem {
				desc.IsSystem = a.IsSystem()
			}
		}
	}
	return desc
}

// --- Standard built-in actions ---

// EchoAction just returns its configured output. Good for testing.
// 嵌入 GenericAction 但有 config attr，泛型输入是 echoActionInput（目前只有 output 字段）。
type echoActionInput struct {
	Output interface{} `mapstructure:"output"`
}

type EchoAction struct {
	Output interface{}
	Delay  float64
	GenericAction[echoActionInput]
}

func NewEchoAction() Action {
	a := &EchoAction{Output: "Hello, Workflow!"}
	a.GenericAction = GenericAction[echoActionInput]{NameStr: "std.echo", Sync: true, DescStr: "Returns configured output (testing helper)", System: true, DoFn: a.echoImpl}
	return a
}

// Do 实现具体逻辑；config attr Output 优先于 input。
func (a *EchoAction) echoImpl(in *echoActionInput) (*db.Result, error) {
	if a.Delay > 0 {
		time.Sleep(time.Duration(a.Delay * float64(time.Second)))
	}
	out := a.Output
	if out == nil && in != nil {
		out = in.Output
	}
	return &db.Result{Data: out}, nil
}

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
// 嵌入 GenericAction[httpActionInput]，自动获得 decode + Action 接口实现。
type HttpAction struct {
	GenericAction[httpActionInput]
}

func NewHttpAction() Action {
	a := &HttpAction{}
	a.GenericAction = GenericAction[httpActionInput]{NameStr: "std.http", Sync: true, DescStr: "Performs an HTTP request (method/url/body/headers/timeout)", System: true, DoFn: a.httpImpl}
	return a
}

// Do 是泛型基类要求实现的方法，直接拿 typed input。
func (a *HttpAction) httpImpl(in *httpActionInput) (*db.Result, error) {
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
type WorkflowHTTPAction struct {
	GenericAction[wfHTTPActionInput]
}

func NewWorkflowHTTPAction() Action {
	a := &WorkflowHTTPAction{}
	a.GenericAction = GenericAction[wfHTTPActionInput]{NameStr: "std.workflow_http", Sync: true, DescStr: "Triggers a workflow via HTTP POST /v2/executions", System: true, DoFn: a.wfHttpImpl}
	return a
}

func (a *WorkflowHTTPAction) wfHttpImpl(in *wfHTTPActionInput) (*db.Result, error) {
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

// EmailAction sends a simple plain-text email via SMTP.
type EmailAction struct {
	GenericAction[emailActionInput]
}

func NewEmailAction() Action {
	a := &EmailAction{}
	a.GenericAction = GenericAction[emailActionInput]{NameStr: "std.email", Sync: true, DescStr: "Sends an email via SMTP", System: true, DoFn: a.emailImpl}
	return a
}

func (a *EmailAction) emailImpl(in *emailActionInput) (*db.Result, error) {
	if in.From == "" {
		return &db.Result{Error: "std.email requires 'from'"}, nil
	}
	if in.To == "" {
		return &db.Result{Error: "std.email requires 'to'"}, nil
	}

	host := in.SMTPHost
	if host == "" {
		return &db.Result{Error: "std.email requires 'smtp_host'"}, nil
	}
	port := in.SMTPPort
	if port == 0 {
		if in.UseTLS {
			port = 465
		} else {
			port = 25
		}
	}

	addr := fmt.Sprintf("%s:%d", host, port)

	// 构造邮件头 + body
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		in.From, in.To, in.Subject, in.Body)

	var err error
	if in.UseTLS {
		// STARTTLS 或直连 TLS — 用 net/smtp 包
		auth := smtp.PlainAuth("", in.Username, in.Password, host)
		err = smtp.SendMail(addr, auth, in.From, []string{in.To}, []byte(msg))
	} else if in.Username != "" {
		// 有用户名但没标 TLS → 也走 PlainAuth（smtp.SendMail 会自动 STARTTLS）
		auth := smtp.PlainAuth("", in.Username, in.Password, host)
		err = smtp.SendMail(addr, auth, in.From, []string{in.To}, []byte(msg))
	} else {
		// 匿名
		err = smtp.SendMail(addr, nil, in.From, []string{in.To}, []byte(msg))
	}

	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.email send: %v", err)}, nil
	}

	return &db.Result{
		Data: map[string]interface{}{
			"sent":    true,
			"from":    in.From,
			"to":      in.To,
			"subject": in.Subject,
		},
	}, nil
}

// --- SSHAction ---

// sshActionInput mirrors Python std.ssh input keys.
type sshActionInput struct {
	Command    string  `mapstructure:"command"`
	Host       string  `mapstructure:"host"`
	Port       int     `mapstructure:"port"`
	Username   string  `mapstructure:"username"`
	Password   string  `mapstructure:"password"`
	PrivateKey string  `mapstructure:"private_key"`
	Timeout    float64 `mapstructure:"timeout"` // seconds
}

// SSHAction runs a command on a remote host via SSH.
type SSHAction struct {
	GenericAction[sshActionInput]
}

func NewSSHAction() Action {
	a := &SSHAction{}
	a.GenericAction = GenericAction[sshActionInput]{NameStr: "std.ssh", Sync: true, DescStr: "Runs a remote command via SSH", System: true, DoFn: a.sshImpl}
	return a
}

// buildSSHClientConfig 构造 SSH client config，支持 password/private key/both。
func buildSSHClientConfig(username, password, privateKey string) (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	if privateKey != "" {
		signer, err := ssh.ParsePrivateKey([]byte(privateKey))
		if err != nil {
			// 可能是带 passphrase 的 key — 尝试空 passphrase
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(privateKey), nil)
			if err != nil {
				return nil, fmt.Errorf("parse private key: %w", err)
			}
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if password != "" {
		authMethods = append(authMethods, ssh.Password(password))
	}
	if len(authMethods) == 0 {
		return nil, fmt.Errorf("std.ssh: requires 'password' or 'private_key'")
	}

	timeout := 30 * time.Second
	return &ssh.ClientConfig{
		User:            username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}, nil
}

func (a *SSHAction) sshImpl(in *sshActionInput) (*db.Result, error) {
	if in.Host == "" {
		return &db.Result{Error: "std.ssh requires 'host'"}, nil
	}
	if in.Command == "" {
		return &db.Result{Error: "std.ssh requires 'command'"}, nil
	}
	if in.Port == 0 {
		in.Port = 22
	}
	if in.Username == "" {
		return &db.Result{Error: "std.ssh requires 'username'"}, nil
	}

	cfg, err := buildSSHClientConfig(in.Username, in.Password, in.PrivateKey)
	if err != nil {
		return &db.Result{Error: err.Error()}, nil
	}
	if in.Timeout > 0 {
		cfg.Timeout = time.Duration(in.Timeout * float64(time.Second))
	}

	addr := net.JoinHostPort(in.Host, fmt.Sprintf("%d", in.Port))
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh dial %s: %v", addr, err)}, nil
	}
	defer client.Close()

	sess, err := client.NewSession()
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh session: %v", err)}, nil
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	exitCode := 0
	if err := sess.Run(in.Command); err != nil {
		if exitErr, ok := err.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			return &db.Result{Error: fmt.Sprintf("std.ssh run: %v", err)}, nil
		}
	}

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":   stdout.String(),
			"stderr":   stderr.String(),
			"host":     in.Host,
			"port":     in.Port,
			"exitcode": exitCode,
		},
	}, nil
}

// --- SSHProxiedAction ---

// sshProxiedActionInput mirrors Python std.ssh_proxied input keys.
type sshProxiedActionInput struct {
	Command      string  `mapstructure:"command"`
	Host         string  `mapstructure:"host"`
	Username     string  `mapstructure:"username"`
	Password     string  `mapstructure:"password"`
	PrivateKey   string  `mapstructure:"private_key"`
	GatewayHost  string  `mapstructure:"gateway_host"`
	GatewayUser  string  `mapstructure:"gateway_username"`
	ProxyCommand string  `mapstructure:"proxy_command"`
	Timeout      float64 `mapstructure:"timeout"`
}

// SSHProxiedAction routes SSH through a bastion/gateway host.
func NewSSHProxiedAction() Action {
	a := &SSHProxiedAction{}
	a.GenericAction = GenericAction[sshProxiedActionInput]{NameStr: "std.ssh_proxied", Sync: true, DescStr: "SSH through bastion/gateway host", System: true, DoFn: a.sshProxiedImpl}
	return a
}

type SSHProxiedAction struct {
	GenericAction[sshProxiedActionInput]
}

func (a *SSHProxiedAction) sshProxiedImpl(in *sshProxiedActionInput) (*db.Result, error) {
	if in.GatewayHost == "" {
		return &db.Result{Error: "std.ssh_proxied requires 'gateway_host'"}, nil
	}
	if in.Host == "" {
		return &db.Result{Error: "std.ssh_proxied requires 'host'"}, nil
	}
	if in.Command == "" {
		return &db.Result{Error: "std.ssh_proxied requires 'command'"}, nil
	}

	gatewayUser := in.GatewayUser
	if gatewayUser == "" {
		gatewayUser = in.Username
	}

	// 1. 连 bastion
	gwCfg, err := buildSSHClientConfig(gatewayUser, in.Password, in.PrivateKey)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("gateway auth: %v", err)}, nil
	}
	if in.Timeout > 0 {
		gwCfg.Timeout = time.Duration(in.Timeout * float64(time.Second))
	}

	gwClient, err := ssh.Dial("tcp", net.JoinHostPort(in.GatewayHost, "22"), gwCfg)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh_proxied gateway %s: %v", in.GatewayHost, err)}, nil
	}
	defer gwClient.Close()

	// 2. 通过 bastion 做 TCP 转发到目标机
	targetCfg, err := buildSSHClientConfig(in.Username, in.Password, in.PrivateKey)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("target auth: %v", err)}, nil
	}
	if in.Timeout > 0 {
		targetCfg.Timeout = time.Duration(in.Timeout * float64(in.Timeout))
	}

	// 用 bastion 的 SSH 连接做代理拨号
	conn, err := gwClient.Dial("tcp", net.JoinHostPort(in.Host, "22"))
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh_proxied dial target via gateway: %v", err)}, nil
	}

	// 3. 在代理连接上做 SSH handshake → 目标机
	ncc, chans, reqs, err := ssh.NewClientConn(conn, in.Host, targetCfg)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh_proxied target handshake: %v", err)}, nil
	}
	targetClient := ssh.NewClient(ncc, chans, reqs)
	defer targetClient.Close()

	sess, err := targetClient.NewSession()
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.ssh_proxied session: %v", err)}, nil
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	exitCode := 0
	if err := sess.Run(in.Command); err != nil {
		if exitErr, ok := err.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			return &db.Result{Error: fmt.Sprintf("std.ssh_proxied run: %v", err)}, nil
		}
	}

	return &db.Result{
		Data: map[string]interface{}{
			"stdout":       stdout.String(),
			"stderr":       stderr.String(),
			"target_host":  in.Host,
			"gateway_host": in.GatewayHost,
			"gateway_user": gatewayUser,
			"exitcode":     exitCode,
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
type JavaScriptAction struct {
	GenericAction[jsActionInput]
}

func NewJavaScriptAction() Action {
	a := &JavaScriptAction{}
	a.GenericAction = GenericAction[jsActionInput]{NameStr: "std.javascript", Sync: true, DescStr: "Evaluates JavaScript expression", System: true, DoFn: a.jsImpl}
	return a
}

func (a *JavaScriptAction) jsImpl(in *jsActionInput) (*db.Result, error) {
	if in.Script == "" {
		return &db.Result{Error: "std.javascript requires 'script'"}, nil
	}

	if in.Context == nil {
		in.Context = map[string]interface{}{}
	}

	// YAQL/Jinja 模板 → 走 expressions 引擎
	if strings.HasPrefix(in.Script, "<%") || strings.Contains(in.Script, "${") {
		result := expressions.EvaluateRecursively(in.Script, in.Context)
		return &db.Result{Data: result}, nil
	}

	// 纯 JSON → 直接 parse
	var parsed interface{}
	if err := json.Unmarshal([]byte(in.Script), &parsed); err == nil {
		return &db.Result{Data: parsed}, nil
	}

	// 真正的 JS 脚本 → goja 引擎
	vm := goja.New()
	// 把 context 里的变量注入 JS 全局
	for k, v := range in.Context {
		if err := vm.Set(k, v); err != nil {
			return &db.Result{Error: fmt.Sprintf("std.javascript: set var %s: %v", k, err)}, nil
		}
	}

	val, err := vm.RunString(in.Script)
	if err != nil {
		return &db.Result{Error: fmt.Sprintf("std.javascript: %v", err)}, nil
	}

	// 把 goja 返回值转回 Go interface{}
	var result interface{}
	if err := vm.ExportTo(val, &result); err != nil {
		// ExportTo 可能因为类型不兼容失败，fallback 到 val.String()
		result = val.String()
	}
	return &db.Result{Data: result}, nil
}

// --- TestDictAction ---

// testDictActionInput mirrors Python std.test_dict input keys.
type testDictActionInput struct {
	Keys []string `mapstructure:"keys"`
}

// TestDictAction returns a fixed dict useful for testing.
type TestDictAction struct {
	GenericAction[testDictActionInput]
}

func NewTestDictAction() Action {
	a := &TestDictAction{}
	a.GenericAction = GenericAction[testDictActionInput]{NameStr: "std.test_dict", Sync: true, DescStr: "Returns a fixed test dict", System: true, DoFn: a.testDictImpl}
	return a
}

func (a *TestDictAction) testDictImpl(in *testDictActionInput) (*db.Result, error) {
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
type noopInput struct{}

type NoopAction struct {
	GenericAction[noopInput]
}

func NewNoopAction() Action {
	a := &NoopAction{}
	a.GenericAction = GenericAction[noopInput]{NameStr: "std.noop", Sync: true, DescStr: "Does nothing and returns nil", System: true, DoFn: a.noopImpl}
	return a
}

func (a *NoopAction) noopImpl(*noopInput) (*db.Result, error) {
	return &db.Result{}, nil
}

// AsyncNoopAction is an asynchronous noop (just returns immediately with success).
type AsyncNoopAction struct {
	GenericAction[noopInput]
}

func NewAsyncNoopAction() Action {
	a := &AsyncNoopAction{}
	a.GenericAction = GenericAction[noopInput]{NameStr: "std.async_noop", Sync: false, DescStr: "Async noop for testing", System: true, DoFn: a.asyncNoopImpl}
	return a
}

func (a *AsyncNoopAction) asyncNoopImpl(*noopInput) (*db.Result, error) {
	return &db.Result{}, nil
}

// FailAction always fails — useful for testing error handling.
type failInput struct{}

type FailAction struct {
	ErrorData interface{}
	GenericAction[failInput]
}

func NewFailAction() Action {
	a := &FailAction{}
	a.GenericAction = GenericAction[failInput]{NameStr: "std.fail", Sync: true, DescStr: "Always fails (testing helper)", System: true, DoFn: a.failImpl}
	return a
}

func (a *FailAction) failImpl(*failInput) (*db.Result, error) {
	if a.ErrorData != nil {
		return &db.Result{Error: a.ErrorData}, nil
	}
	return &db.Result{Error: "Fail action expected error"}, fmt.Errorf("fail action expected error")
}

// --- SleepAction ---

// sleepActionInput mirrors Python std.sleep input keys.
type sleepActionInput struct {
	Seconds float64 `mapstructure:"seconds"`
}

// SleepAction waits for the given number of seconds.
// 嵌入 GenericAction[sleepActionInput]；Seconds 是 config attr（优先级高于 input）。
type SleepAction struct {
	Seconds float64 // struct attribute (higher priority than input)
	GenericAction[sleepActionInput]
}

func NewSleepAction() Action {
	a := &SleepAction{}
	a.GenericAction = GenericAction[sleepActionInput]{NameStr: "std.sleep", Sync: true, DescStr: "Sleeps for N seconds", System: true, DoFn: a.sleepImpl}
	return a
}

// Do 实现具体逻辑；struct attr Seconds 优先级高于 input。
func (a *SleepAction) sleepImpl(in *sleepActionInput) (*db.Result, error) {
	seconds := a.Seconds
	if seconds <= 0 {
		if in != nil {
			seconds = in.Seconds
		}
	}
	if seconds <= 0 {
		seconds = 0
	}
	time.Sleep(time.Duration(seconds * float64(time.Second)))
	return &db.Result{Data: fmt.Sprintf("slept for %v seconds", seconds)}, nil
}

// --- WaitSSHAction ---

// waitSSHActionInput mirrors Python std.wait_ssh input keys.
type waitSSHActionInput struct {
	Host    string  `mapstructure:"host"`
	Port    int     `mapstructure:"port"`
	Timeout float64 `mapstructure:"timeout"`
}

// WaitSSHAction 等待 SSH 服务就绪 — 端口探测 + SSH banner 验证。
type WaitSSHAction struct {
	Host    string
	Port    int
	Timeout float64
	GenericAction[waitSSHActionInput]
}

func NewWaitSSHAction() Action {
	a := &WaitSSHAction{Port: 22, Timeout: 300}
	a.GenericAction = GenericAction[waitSSHActionInput]{NameStr: "std.wait_ssh", Sync: false, DescStr: "Waits for SSH port + banner", System: true, DoFn: a.waitSSHImpl}
	return a
}

// waitSSHImpl 做真正的 SSH 就绪检测：
// 1. 轮询 TCP 端口能连上
// 2. 连上后读 banner 确认是 SSH 服务
func (a *WaitSSHAction) waitSSHImpl(in *waitSSHActionInput) (*db.Result, error) {
	host := a.Host
	if host == "" && in != nil {
		host = in.Host
	}
	if host == "" {
		return &db.Result{Error: "std.wait_ssh requires 'host'"}, nil
	}
	port := a.Port
	if port == 0 && in != nil {
		port = in.Port
	}
	if port == 0 {
		port = 22
	}
	timeout := a.Timeout
	if timeout <= 0 {
		if in != nil {
			timeout = in.Timeout
		}
	}
	if timeout <= 0 {
		timeout = 300
	}

	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	start := time.Now()
	interval := 2 * time.Second

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	var lastErr error

	for time.Now().Before(deadline) {
		// TCP 连接探测 + SSH banner 读取
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			lastErr = err
		} else {
			// 连上了 → 读 SSH banner（通常是 "SSH-2.0-OpenSSH_..."）
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			buf := make([]byte, 64)
			n, _ := conn.Read(buf)
			conn.Close()
			if n > 0 && strings.HasPrefix(string(buf[:n]), "SSH-") {
				return &db.Result{
					Data: map[string]interface{}{
						"ready":   true,
						"host":    host,
						"port":    port,
						"banner":  string(buf[:n]),
						"elapsed": time.Since(start).Seconds(),
					},
				}, nil
			}
			// 端口开了但不是 SSH（或 banner 还没出）— 继续等
			lastErr = fmt.Errorf("port open but no SSH banner")
		}

		time.Sleep(interval)
	}

	return &db.Result{
		Error: fmt.Sprintf("std.wait_ssh timeout after %.0fs: %v", timeout, lastErr),
		Data:  map[string]interface{}{"host": host, "port": port, "ready": false},
	}, nil
}

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
// Name/Description/IsSystem 全部从 action 自身提取，一行一个。
func (r *ActionRegistry) RegisterBuiltinActions() {
	r.RegisterAction(NewEchoAction)
	r.RegisterAction(NewNoopAction)
	r.RegisterAction(NewAsyncNoopAction)
	r.RegisterAction(NewFailAction)
	r.RegisterAction(NewSleepAction)
	r.RegisterAction(NewWaitSSHAction)
	r.RegisterAction(NewHttpAction)
	r.RegisterAction(NewWorkflowHTTPAction)
	r.RegisterAction(NewEmailAction)
	r.RegisterAction(NewSSHAction)
	r.RegisterAction(NewSSHProxiedAction)
	r.RegisterAction(NewJavaScriptAction)
	r.RegisterAction(NewTestDictAction)
}

// Register 注册一个 ActionDescriptor。Name/Description/IsSystem 为空时
// 会自动从 Factory 返回的 Action 实例提取。
func (r *ActionRegistry) Register(d *ActionDescriptor) {
	filled := makeDescriptor(*d)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[filled.Name] = &filled
}

// RegisterAction 是最便捷的注册方式：直接传 Factory，
// Name/Description/IsSystem 全部从 action 自身提取。
//
// 用法：r.RegisterAction(NewEchoAction)
func (r *ActionRegistry) RegisterAction(factory func() Action) {
	r.Register(&ActionDescriptor{Factory: factory})
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

// ============================================================================
// GenericAction[T] 泛型 helper —— 编译期类型安全 + 零 reflect 开销
// ============================================================================
//
// Go 方法提升没有"虚函数"语义：GenericAction[T].Run 里 g.Do() 调的是
// GenericAction[T].Do，不会分发到 *EchoAction.Do。所以改用函数注入模式，
// DoFn 在 NewXxxAction 时就绑定了子类方法，不存在分发问题。
//
// 用法：
//
//	type MyAction struct { GenericAction[myInput] }
//
//	func NewMyAction() Action {
//	    a := &MyAction{}
//	    a.GenericAction = GenericAction[myInput]{
//	        NameStr: "std.xxx", Sync: true,
//	        DescStr: "描述", System: true,
//	        DoFn:    a.doImpl,
//	    }
//	    return a
//	}
//
//	func (a *MyAction) doImpl(in *myInput) (*db.Result, error) { ... }

// GenericAction 泛型基类，通过 DoFn 注入子类实现（避免方法提升分发问题）。
// T 是输入 struct 类型（字段带 mapstructure tag）。
// 同时实现 Action 接口的 Name/IsSync/Description/IsSystem，
// 子类嵌入后自动获得完整 Action 接口满足。
type GenericAction[T any] struct {
	NameStr string
	Sync    bool
	DescStr string
	System  bool                         // 是否内置系统 action
	DoFn    func(*T) (*db.Result, error) // 绑定子类实现；nil 会报 "DoFn not set"
}

// Run 实现 Action 接口：decode input map → typed *T → 调 DoFn。
func (g *GenericAction[T]) Run(input map[string]interface{}) (*db.Result, error) {
	var typed T
	if input == nil {
		input = map[string]interface{}{}
	}
	if err := decodeActionInput(input, &typed); err != nil {
		return nil, fmt.Errorf("%s: decode input: %w", g.NameStr, err)
	}
	if g.DoFn == nil {
		return nil, fmt.Errorf("%s: DoFn not set", g.NameStr)
	}
	return g.DoFn(&typed)
}

func (g *GenericAction[T]) Name() string        { return g.NameStr }
func (g *GenericAction[T]) IsSync() bool        { return g.Sync }
func (g *GenericAction[T]) Description() string { return g.DescStr }
func (g *GenericAction[T]) IsSystem() bool      { return g.System }

// ============================================================================
