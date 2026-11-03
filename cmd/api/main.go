// Command api is a standalone API REST gateway process.
//
// Mirrors Python Mistral's api_service.WSGIService('mistral_api') exactly.
//
// Architecture:
//
//   Python                              Go
//   ═══════                             ═══
//   mistral.cmd.launch --server api      cmd/api (this binary)
//   WSGIService('mistral_api')           api.NewServer(store, eng) → :8080
//   (纯 HTTP REST 网关)                  (纯 HTTP REST 网关)
//
//   API 进程本身不执行任何工作流逻辑 —— 它只是一个 REST 表面，内部
//   通过 EngineClient（topic='engine' MQ RPC）调用 engine 进程。
//   这和 Python 的 API 完全一样：WSGI handler → EngineClient.start_workflow()
//   → MQ RPC 到 engine 进程处理。
//
// Transport options (和 engine/executor 完全一致):
//   --mq-url ""                               → InProcessTransport (测试)
//   --mq-url amqp://guest:guest@host:5672     → AMQPTransport (生产, RabbitMQ)
//
// Usage (和 engine, executor 三个独立进程):
//
//   # 跨进程部署（完全对齐 Python 三进程架构）
//   go run ./cmd/engine   --db workflow_go --mq-url amqp://guest:guest@host:5672
//   go run ./cmd/executor --mq-url amqp://guest:guest@host:5672
//   go run ./cmd/api      --db workflow_go --mq-url amqp://guest:guest@host:5672
//
//   API → MQ topic='engine' → EngineServer.start_workflow()
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/rpc"
	"github.com/danchleo/workflow-go/pkg/api"
)

func main() {
	addr := flag.String("addr", ":8080", "API REST listen address")
	dbURL := flag.String("db", ":memory:",
		"DB URL — ':memory:', SQLite path, or MySQL DSN")
	mqURL := flag.String("mq-url", "",
		"MQ broker — empty=process-local (test), amqp://guest:guest@host:5672=RabbitMQ (prod)")
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

	// --- MQ Transport (API 也需要 —— 它内部用 EngineClient 调 engine) ---
	var transport rpc.Transport
	if *mqURL == "" {
		transport = rpc.NewInProcessTransport()
		log.Println("mq transport: in-process memory")
	} else {
		t, err := rpc.NewAMQPTransport(*mqURL)
		if err != nil {
			log.Fatalf("amqp %s: %v", *mqURL, err)
		}
		transport = t
		log.Printf("mq transport: amqp %s", *mqURL)
	}
	defer transport.Close()

	// --- Engine (API 进程持有一个本地 engine 实例作为 fallback;
	//     真实部署下 engine 在独立进程, EngineClient 通过 MQ RPC 调) ---
	eng := engine.NewDefaultEngine(store)

	// --- HTTP REST server (API 服务核心) ---
	apiSrv := api.NewServer(store, eng)
	httpSrv := &http.Server{
		Addr:         *addr,
		Handler:      apiSrv.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("api listening on %s (Python mistral-api)", *addr)
		errCh <- httpSrv.ListenAndServe()
	}()

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("api server error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down...", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	transport.Close()
	log.Println("api stopped")
}
