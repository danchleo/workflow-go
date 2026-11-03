// Command executor is a standalone action-executor process.
//
// Mirrors Python reference's reference/executors/executor_server.py exactly.
//
// Architecture (topic-based MQ RPC — no HTTP):
//
//	Python                              Go
//	═══════                             ═══
//	rpc.get_rpc_transport()             rpc.NewAMQPTransport(--mq-url)
//	rpc_server.register_endpoint        executorrpc.NewServer(transport, registry)
//	  (ExecutorServer)                  topic='executor', 1 method: run_action
//	rpc_server.start()                  executorServer.Start(ctx)
//
// Engine → Executor call (engine进程):
//
//	self._executor_client.run_action(async_=True)
//	→ ExecutorRPCClient.RunAction → AsyncCall('executor', 'run_action')
//
// Executor → Engine callback (本进程, 通过 EngineRPCClient):
//
//	DefaultExecutor 持有 EngineRPCClient
//	→ EngineRPCClient.OnActionComplete → AsyncCall('engine', 'on_action_complete')
//
// Transport options (同 cmd/engine):
//
//	--mq-url ""  (default) → InProcessTransport (单进程测试)
//	--mq-url amqp://guest:guest@host:5672 → AMQPTransport (跨进程)
//
// Usage (配合 RabbitMQ broker):
//
//	# Start RabbitMQ first: docker run -p 6379:6379 RabbitMQ:7-alpine
//	go run ./cmd/engine   --db engine.db --mq-url amqp://guest:guest@localhost:5672
//	go run ./cmd/executor --mq-url amqp://guest:guest@localhost:5672
//	go run ./cmd/workflowd --server api --db engine.db --mq-url amqp://guest:guest@localhost:5672
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/rpc"
	executorrpc "github.com/danchleo/workflow-go/pkg/executorrpc"
)

func main() {
	mqURL := flag.String("mq-url", "",
		"MQ broker URL — empty=process-local (test), amqp://guest:guest@host:5672=RabbitMQ (prod)")
	flag.Parse()

	// --- MQ Transport ---
	var transport rpc.Transport
	if *mqURL == "" {
		transport = rpc.NewInProcessTransport()
		log.Println("mq transport: in-process memory")
	} else {
		t, err := rpc.NewAMQPTransport(*mqURL)
		if err != nil {
			log.Fatalf("RabbitMQ transport %s: %v", *mqURL, err)
		}
		transport = t
		log.Printf("mq transport: RabbitMQ %s", *mqURL)
	}
	defer transport.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Action Registry ---
	registry := actions.NewActionRegistry()

	// --- Executor MQ RPC server (topic 'executor') ---
	// Python: rpc_server.register_endpoint(ExecutorServer)
	execServer := executorrpc.NewServer(transport, registry)
	if err := execServer.Start(ctx); err != nil {
		log.Fatalf("executor mq start: %v", err)
	}
	log.Println("executor mq server started — topic 'executor' (run_action)")

	// --- DefaultExecutor wired with EngineRPCClient (for callbacks) ---
	// Python: DefaultExecutor(rpc.get_engine_client())
	engineClient := rpc.NewEngineRPCClient(transport)
	executors.SetDefaultExecutor(executors.NewDefaultExecutor(engineClient))
	log.Println("default executor → engine via MQ topic 'engine'")

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	cancel()
	transport.Close()
	log.Println("executor stopped")
}
