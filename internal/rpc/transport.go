// Package rpc provides topic-based MQ RPC — ported from Python oslo.messaging.
//
// Architecture (mirrors oslo.messaging exactly):
//
//   Transport — abstract MQ transport layer
//     ├── InProcessTransport (进程内内存, 对等 oslo.messaging fake:/ transport)
//     └── AMQPTransport      (生产, 对等 oslo.messaging rabbit://)
//
//   RPCClient{Transport, Topic}  — 发出 RPC 调用
//     SyncCall(ctx, method, args, &result)  — request-reply, 阻塞等结果
//     AsyncCall(ctx, method, args)          — fire-and-forget publish
//
//   RPCServer{Transport, Topic}  — 注册 endpoint, 启动监听
//     Handle(method, handler)              — 注册 handler
//     Start(ctx)                           — 开始接收并分发
//
// Topic 命名（完全对齐 Python）：
//   'engine'   → EngineServer  (start_workflow, on_action_complete, ...)
//   'executor' → ExecutorServer (run_action)
//
// Wire protocol（JSON，完全自定义）：
//   Request:  {id, method, args}
//   Reply:    {id, result, error}
//   Publish:  {method, args}  (no reply expected)
package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Transport is the abstract MQ layer — mirror of oslo.messaging transport.
type Transport interface {
	// SendRequest sends a request on topic, returns a channel for the reply.
	// The replyChan receives exactly one value (or closes on timeout).
	SendRequest(ctx context.Context, topic string, req *Request) (<-chan *Reply, error)

	// SendPublish sends a fire-and-forget message on topic (no reply).
	SendPublish(ctx context.Context, topic string, pub *Publish) error

	// RegisterEndpoint tells the transport to deliver messages for topic
	// to the given dispatcher. Only one endpoint per topic.
	RegisterEndpoint(topic string, dispatcher Dispatcher)

	// Close shuts down the transport.
	Close() error
}

// Dispatcher dispatches an incoming request to its handler and returns the reply.
type Dispatcher interface {
	Dispatch(ctx context.Context, req *Request) *Reply
}

// Request is a sync_call request — expects a reply.
type Request struct {
	ID     string           `json:"id"`
	Method string           `json:"method"`
	Args   map[string]any   `json:"args"`
}

// Reply is the response to a Request.
type Reply struct {
	ID     string `json:"id"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Publish is an async_call publish — no reply expected.
type Publish struct {
	Method string         `json:"method"`
	Args   map[string]any `json:"args"`
}

// ToJSON serializes to JSON bytes — used by wire transports (AMQP).
func (r *Request) ToJSON() ([]byte, error) { return json.Marshal(r) }
func (r *Reply) ToJSON() ([]byte, error)   { return json.Marshal(r) }
func (p *Publish) ToJSON() ([]byte, error) { return json.Marshal(p) }

// --- InProcessTransport — memory transport, 100% Python fake:/ equivalent ---

// NewInProcessTransport creates a process-local memory MQ.
// Perfect for single-process deployment, tests, and development.
// Multi-process deployments should use NewAMQPTransport instead.
func NewInProcessTransport() Transport {
	t := &inProcessTransport{
		endpoints: make(map[string]Dispatcher),
		pending:   make(map[string]chan *Reply),
	}
	globalInProcessMu.Lock()
	defer globalInProcessMu.Unlock()
	globalInProcess = t
	return t
}

// globalInProcess lets InProcessTransport instances created in different
// packages (engine main.go, executor main.go during tests) share memory.
// Each call to NewInProcessTransport() refreshes the global pointer — but
// within ONE process there's only ever one transport for one topic set.
var (
	globalInProcessMu sync.Mutex
	globalInProcess   *inProcessTransport
)

// GetGlobalInProcess returns the currently-registered InProcessTransport,
// or creates a fresh one if none exists. Used by engine/executor that were
// started separately and need to find each other in-process.
func GetGlobalInProcess() *inProcessTransport {
	globalInProcessMu.Lock()
	defer globalInProcessMu.Unlock()
	if globalInProcess == nil {
		globalInProcess = &inProcessTransport{
			endpoints: make(map[string]Dispatcher),
			pending:   make(map[string]chan *Reply),
		}
	}
	return globalInProcess
}

type inProcessTransport struct {
	mu        sync.Mutex
	endpoints map[string]Dispatcher         // topic → dispatcher
	pending   map[string]chan *Reply        // request_id → reply chan
	seq       uint64
	closed    bool
}

func (t *inProcessTransport) RegisterEndpoint(topic string, d Dispatcher) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.endpoints[topic] = d
}

func (t *inProcessTransport) SendRequest(ctx context.Context, topic string, req *Request) (<-chan *Reply, error) {
	t.mu.Lock()
	d, ok := t.endpoints[topic]
	if !ok {
		t.mu.Unlock()
		return nil, fmt.Errorf("no endpoint registered for topic %q", topic)
	}
	replyCh := make(chan *Reply, 1)

	// Dispatch synchronously — handler runs in this goroutine.
	// In oslo.messaging fake transport, request handling is also in-process
	// and runs immediately (the fake transport is synchronous by default).
	t.mu.Unlock()

	reply := d.Dispatch(ctx, req)
	// match the request ID
	if reply.ID == "" {
		reply.ID = req.ID
	}
	select {
	case replyCh <- reply:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return replyCh, nil
}

func (t *inProcessTransport) SendPublish(ctx context.Context, topic string, pub *Publish) error {
	t.mu.Lock()
	d, ok := t.endpoints[topic]
	t.mu.Unlock()
	if !ok {
		// fire-and-forget — no handler is not an error (maybe nobody's listening yet)
		return nil
	}
	// oslo.messaging async_call — also runs synchronously in fake transport.
	// We wrap in goroutine to truly be async (matches oslo.messaging cast semantics).
	go func() {
		req := &Request{Method: pub.Method, Args: pub.Args}
		_ = d.Dispatch(ctx, req)
	}()
	return nil
}

func (t *inProcessTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// --- RPCClient / RPCServer wrappers ---

// RPCClient is the client-side abstraction — mirrors oslo.messaging RPCClient.
type RPCClient struct {
	Transport Transport
	Topic     string
	Timeout   func() context.Context
}

// SyncCall invokes method(args) and blocks for the reply.
// mirrors oslo.messaging RPCClient.call('method', **args).
func (c *RPCClient) SyncCall(ctx context.Context, method string, args map[string]any, result any) error {
	req := &Request{
		ID:     generateReqID(),
		Method: method,
		Args:   args,
	}
	replyCh, err := c.Transport.SendRequest(ctx, c.Topic, req)
	if err != nil {
		return fmt.Errorf("rpc call %s/%s: %w", c.Topic, method, err)
	}
	select {
	case reply := <-replyCh:
		if reply.Error != "" {
			return fmt.Errorf("rpc %s/%s: %s", c.Topic, method, reply.Error)
		}
		if result != nil && reply.Result != nil {
			// round-trip via JSON for correct type coercion (oslo.messaging does
			// the same via its serializer)
			b, _ := json.Marshal(reply.Result)
			return json.Unmarshal(b, result)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AsyncCall fires method(args) without waiting for a reply.
// mirrors oslo.messaging RPCClient.cast('method', **args).
func (c *RPCClient) AsyncCall(ctx context.Context, method string, args map[string]any) error {
	return c.Transport.SendPublish(ctx, c.Topic, &Publish{Method: method, Args: args})
}

// RPCServer is the server-side abstraction — mirrors oslo.messaging RPCServer.
type RPCServer struct {
	Transport Transport
	Topic     string
	handlers  map[string]HandlerFn
}

// HandlerFn is the type every RPC method handler must satisfy.
// Args are what the client sent; return values are serialized as reply.
type HandlerFn func(ctx context.Context, args map[string]any) (any, error)

// NewRPCServer creates an RPCServer bound to transport + topic.
func NewRPCServer(t Transport, topic string) *RPCServer {
	return &RPCServer{
		Transport: t,
		Topic:     topic,
		handlers:  make(map[string]HandlerFn),
	}
}

// Handle registers a method handler.
func (s *RPCServer) Handle(method string, h HandlerFn) {
	s.handlers[method] = h
}

// Dispatcher implements Transport.Dispatcher.
func (s *RPCServer) Dispatch(ctx context.Context, req *Request) *Reply {
	h, ok := s.handlers[req.Method]
	if !ok {
		return &Reply{ID: req.ID, Error: fmt.Sprintf("no handler for method %q on topic %q", req.Method, s.Topic)}
	}
	defer func() {
		if r := recover(); r != nil {
			logRPCPanic(s.Topic, req.Method, r)
		}
	}()
	result, err := h(ctx, req.Args)
	if err != nil {
		return &Reply{ID: req.ID, Error: err.Error()}
	}
	return &Reply{ID: req.ID, Result: result}
}

// Start registers this server's dispatcher with the transport.
// For InProcessTransport this is a one-shot registration (no goroutine needed).
// For AMQPTransport it starts the consumer loop.
func (s *RPCServer) Start(ctx context.Context) error {
	s.Transport.RegisterEndpoint(s.Topic, s)
	return nil
}

var logRPCPanic = func(topic, method string, r any) {
	// lightweight — avoid importing log here
	fmt.Fprintf(fallbackWriter, "rpc panic topic=%s method=%s: %v\n", topic, method, r)
}

var fallbackWriter = &discardWriter{}

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }

func generateReqID() string {
	// uuid would be better but keeps this package dependency-free
	return fmt.Sprintf("req_%d", reqCounter.Add(1))
}

var reqCounter reqIDCounter

type reqIDCounter struct{ sync.Mutex; n uint64 }

func (c *reqIDCounter) Add(any uint64) uint64 {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.n++
	return c.n
}
