// Command workflowd is a single binary that can run any combination of
// workflow-go's components in one process — useful for development, demos,
// and Docker single-container deployments.
//
// Mirrors Python reference/cmd/launch.py exactly:
//
//	Python                          Go
//	═════════                       ═══
//	launch_engine()                 workflowd --server engine
//	  rpc.get_rpc_transport()         rpc.NewInProcessTransport()
//	  EngClient = topic='engine'      NewEngineRPCClient(transport)
//	  rpc_server.register_endpoint   enginerpc.NewServer(transport, engine)
//	  rpc_server.start()              enginerpcServer.Start(ctx)
//
//	launch_executor()               workflowd --server executor
//	  (same transport)                (same transport)
//	  ExecClient = topic='executor'   NewExecutorRPCClient(transport)
//	  rpc_server.register_endpoint    executorrpc.NewServer(transport, registry)
//	  rpc_server.start()              executorrpcServer.Start(ctx)
//
// Architecture:
//
//	All cross-process calls go through topic-based MQ RPC (not HTTP).
//
//	engine ──AsyncCall('executor', 'run_action')──▶ executor
//	executor ──AsyncCall('engine', 'on_action_complete')──▶ engine
//
// Transport:
//   - InProcessTransport (进程内内存 MQ, 对齐 Python fake:/ transport)
//     默认。单进程部署 + 多进程单节点测试。
//   - AMQPTransport (生产, 对齐 Python rabbit://)
//     未来实现。跨节点多进程部署。架构完全准备好了。
//
// Usage:
//
//	# Everything (single-process, InProcessTransport — simplest)
//	workflowd --server all
//
//	# Engine + Executor in one process, sharing DB (multi-process simulation)
//	workflowd --server api,engine,executor --db /tmp/wf.db
//
//	# Engine process, talking to separate Executor process (will need AMQP transport)
//	workflowd --server engine
//	workflowd --server executor  (separate terminal)
//	# Both share same InProcessTransport if same process, otherwise need AMQP
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/danchleo/workflow-go/internal/actions"
	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/rpc"
	"github.com/danchleo/workflow-go/internal/scheduler"
	"github.com/danchleo/workflow-go/pkg/api"
	enginerpc "github.com/danchleo/workflow-go/pkg/enginerpc"
	executorrpc "github.com/danchleo/workflow-go/pkg/executorrpc"
)

// config holds flag values parsed once at startup.
type config struct {
	servers []string // api | engine | executor
	dbURL   string
	mqURL   string // empty=InProcessTransport, "amqp://..."=AMQPTransport
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	var (
		serverFlag = flag.String("server", "all",
			"Comma-separated list of components: api,engine,executor,all")
		dbURL = flag.String("db", ":memory:",
			"DB URL — ':memory:' or SQLite path (shared across api+engine)")
		mqURL = flag.String("mq-url", "",
			"MQ broker — empty=process-local (test), amqp://guest:guest@host:5672=RabbitMQ (prod)")
	)
	flag.Parse()

	var servers []string
	for _, s := range strings.Split(*serverFlag, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if s == "all" {
			servers = []string{"api", "engine", "executor"}
			break
		}
		switch s {
		case "api", "engine", "executor":
			servers = append(servers, s)
		default:
			log.Fatalf("unknown --server component %q (want api, engine, executor, all)", s)
		}
	}
	if len(servers) == 0 {
		log.Fatal("--server must specify at least one component")
	}

	return config{servers: servers, dbURL: *dbURL, mqURL: *mqURL}
}

func run(cfg config) error {
	fmt.Println(banner(cfg))

	// --- Shared store ---
	var store db.Store
	var err error
	if cfg.dbURL == ":memory:" {
		store = db.NewInMemoryStore()
	} else {
		store, err = db.NewSQLStore(cfg.dbURL)
		if err != nil {
			return fmt.Errorf("db %s: %w", cfg.dbURL, err)
		}
	}

	// --- MQ Transport ---
	// 默认 InProcessTransport（进程内内存 MQ，对齐 Python oslo.messaging fake:/）。
	// 生产多节点部署：--mq-url amqp://guest:guest@host:5672 → AMQPTransport（RabbitMQ）。
	var transport rpc.Transport
	if cfg.mqURL == "" {
		transport = rpc.NewInProcessTransport()
		log.Printf("mq transport: in-process memory (--mq-url for RabbitMQ)")
	} else {
		t, err := rpc.NewAMQPTransport(cfg.mqURL)
		if err != nil {
			return fmt.Errorf("amqp %s: %w", cfg.mqURL, err)
		}
		transport = t
		log.Printf("mq transport: amqp %s", cfg.mqURL)
	}
	defer transport.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// --- Engine ---
	var eng *engine.DefaultEngine
	var engServer *rpc.RPCServer
	if has(cfg.servers, "engine") || has(cfg.servers, "api") {
		eng = engine.NewDefaultEngine(store)
		if has(cfg.servers, "engine") {
			sched := scheduler.NewDefaultScheduler(store, 500*time.Millisecond, 0, 30*time.Second, 10*time.Second, 10)
			sched.Start()
			eng.WithScheduler(sched)
			defer sched.Stop()
		}

		// Engine MQ RPC server — topic 'engine', 8 methods.
		// 对齐 Python EngineServer (engine_server.py)。
		engServer = enginerpc.NewServer(transport, eng)
		if err := engServer.Start(ctx); err != nil {
			return fmt.Errorf("engine mq start: %w", err)
		}
		log.Printf("mq server: engine (topic 'engine')")
	}

	// --- Executor ---
	var executorServer *rpc.RPCServer
	registry := actions.NewActionRegistry()

	if has(cfg.servers, "executor") {
		// Executor MQ RPC server — topic 'executor', 1 method: run_action.
		// 对齐 Python ExecutorServer (executor_server.py)。
		executorServer = executorrpc.NewServer(transport, registry)
		if err := executorServer.Start(ctx); err != nil {
			return fmt.Errorf("executor mq start: %w", err)
		}
		log.Printf("mq server: executor (topic 'executor')")
	}

	// --- Wire DefaultExecutor with EngineClient via MQ ---
	// Python: DefaultExecutor(rpc.get_engine_client()) — client from transport.
	// Go:     DefaultExecutor(NewEngineRPCClient(transport)) — same.
	if engServer != nil {
		engineClient := rpc.NewEngineRPCClient(transport)
		executors.SetDefaultExecutor(executors.NewDefaultExecutor(engineClient))
		log.Printf("executor client wired: engine via MQ topic 'engine'")
	}

	// --- HTTP server: API REST surface ---
	if has(cfg.servers, "api") {
		wg.Add(1)
		apiEng := eng
		if apiEng == nil {
			apiEng = engine.NewDefaultEngine(store)
		}
		apiSrv := api.NewServer(store, apiEng)
		srv := &http.Server{Addr: ":8080", Handler: apiSrv.Router()}
		httpServers = append(httpServers, srv)
		go func() {
			defer wg.Done()
			log.Printf("api listening on :8080")
			srv.ListenAndServe()
		}()
	}

	// --- Wait for shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	cancel()

	for _, srv := range httpServers {
		srv.Shutdown(context.Background())
	}
	wg.Wait()
	log.Println("all components stopped")
	return nil
}

var httpServers []*http.Server

func has(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}

func banner(cfg config) string {
	sb := strings.Join(cfg.servers, " + ")
	return fmt.Sprintf(
		"\nworkflowd starting [%s]\n  db:      %s\n  transport: mq (in-process)\n",
		sb, cfg.dbURL,
	)
}
