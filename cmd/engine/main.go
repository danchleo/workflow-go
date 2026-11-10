// Command engine is a standalone engine process.
//
// Mirrors Python reference's reference/engine/engine_server.py exactly.
//
// Architecture (topic-based MQ RPC — no HTTP for engine/executor calls):
//
//	Python                              Go
//	═══════                             ═══
//	rpc.get_rpc_transport()             rpc.NewAMQPTransport(--mq-url)
//	rpc_server.register_endpoint        enginerpc.NewServer(transport, engine)
//	  (EngineServer)                     topic='engine', 8 methods
//	rpc_server.start()                   enginerpcServer.Start(ctx)
//	DefaultExecutor(rpc.EngineClient)   executors.SetDefaultExecutor(NewDefaultExecutor(EngineRPCClient))
//	DefaultExecutor._executor_client    ExecutorRPCClient.RunAction → AsyncCall('executor', 'run_action')
//	  .run_action(async_=True)
//
// Transport options:
//
//	--mq-url ""  (default) → InProcessTransport (单进程测试, 对齐 Python fake:/)
//	--mq-url amqp://guest:guest@host:5672 → AMQPTransport (跨进程, 对齐 Python rabbit://)
//
// Usage:
//
//	# 单进程（所有组件）
//	workflowd --server api,engine,executor --db ":memory:"
//
//	# 跨进程（需要 RabbitMQ broker）
//	engine    --db engine.db    --mq-url amqp://guest:guest@localhost:5672
//	executor  --mq-url amqp://guest:guest@localhost:5672
//	api       --db engine.db    --mq-url amqp://guest:guest@localhost:5672
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/rpc"
	"github.com/danchleo/workflow-go/internal/scheduler"
	enginerpc "github.com/danchleo/workflow-go/pkg/enginerpc"
)

func main() {
	dbURL := flag.String("db", ":memory:", "DB URL — ':memory:' or SQLite path")
	mqURL := flag.String("mq-url", "",
		"MQ broker URL — empty=process-local (test), amqp://guest:guest@host:5672=RabbitMQ (prod)")
	schedPoll := flag.Duration("sched-poll", 500*time.Millisecond, "Scheduler poll interval")
	flag.Parse()

	// --- Store ---
	var store db.Store
	var err error
	if *dbURL == ":memory:" {
		store = db.MustNewSQLStore(":memory:")
	} else {
		store, err = db.NewSQLStore(*dbURL)
		if err != nil {
			log.Fatalf("db %s: %v", *dbURL, err)
		}
	}

	// --- MQ Transport ---
	var transport rpc.Transport
	if *mqURL == "" {
		transport = rpc.NewInProcessTransport()
		log.Println("mq transport: in-process memory (--mq-url for real broker)")
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

	// --- Engine + Scheduler ---
	eng := engine.NewDefaultEngine(store).WithAutoDrain()
	sched := scheduler.NewDefaultScheduler(store, *schedPoll, 0, 30*time.Second, 10*time.Second, 10)
	sched.Start()
	eng.WithScheduler(sched)
	defer sched.Stop()

	// --- Engine MQ RPC server (topic 'engine') ---
	// 对齐 Python EngineServer.register_endpoint(self).
	engServer := enginerpc.NewServer(transport, eng)
	if err := engServer.Start(ctx); err != nil {
		log.Fatalf("engine mq start: %v", err)
	}
	log.Println("engine mq server started — topic 'engine' (8 methods)")

	// --- DefaultExecutor with EngineRPCClient over MQ ---
	// Python: DefaultExecutor(rpc.get_engine_client()).
	engineClient := rpc.NewEngineRPCClient(transport)
	executors.SetDefaultExecutor(executors.NewDefaultExecutor(engineClient))
	log.Println("default executor wired → engine via MQ topic 'engine'")

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	cancel()
	sched.Stop()
	transport.Close()
	log.Println("engine stopped")
}
