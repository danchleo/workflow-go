// Command engine is a standalone engine process.
//
// Mirrors Python mistral's mistral/engine/engine_server.py:
// This process owns the DefaultEngine + scheduler + DB store, listens on
// an HTTP port for engine RPC calls (StartWorkflow, OnActionComplete, etc.)
// from API processes or a caller that uses HttpEngineClient.
//
// In single-process deployments (cmd/test) the engine runs in-process with
// LocalExecutor and no RPC layer. For multi-process deployments you launch:
//
//  1. An executor process (cmd/executor) — runs actions locally
//  2. This engine process — points at the executor via RemoteExecutor
//  3. An API process (cmd/workflowd --api) or cmd/test → talks to engine
//     via HttpEngineClient over HTTP
//
// Usage:
//
//	go run ./cmd/engine --addr ":8951" --db ":memory:" \
//	    --executor remote --executor-url http://executor:8952
//
//	go run ./cmd/engine --addr ":8951" --db "sqlite://engine.db"
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
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/scheduler"
	"github.com/danchleo/workflow-go/pkg/api"
)

func main() {
	addr := flag.String("addr", ":8951", "Engine RPC listen address")
	dbURL := flag.String("db", ":memory:", "DB URL — ':memory:' or SQLite path")
	executorType := flag.String("executor", "local", "Executor type: 'local' or 'remote'")
	executorURL := flag.String("executor-url", "", "Remote executor base URL (required when --executor=remote)")
	schedPoll := flag.Duration("sched-poll", 500*time.Millisecond, "Scheduler poll interval")
	flag.Parse()

	// --- Store ---
	var store db.Store
	var err error
	if *dbURL == ":memory:" {
		store = db.NewInMemoryStore()
	} else {
		store, err = db.NewSQLStore(*dbURL)
		if err != nil {
			log.Fatalf("failed to open DB %s: %v", *dbURL, err)
		}
	}

	// --- Engine + Scheduler (先创建 engine，DefaultExecutor 需要 engine client) ---
	eng := engine.NewDefaultEngine(store)
	sched := scheduler.NewDefaultScheduler(
		store,
		*schedPoll,     // pollInterval
		0,              // randomDelay
		30*time.Second, // captureTimeout
		10*time.Second, // pickupJobAfter
		10,             // batchSize
	)
	sched.Start()
	eng.WithScheduler(sched)

	// --- Executor (必须在 engine 有了之后) ---
	switch *executorType {
	case "local":
		// DefaultExecutor 持有 engine client，RunAction 内部回调 engine
		executors.SetDefaultExecutor(executors.NewDefaultExecutor(
			engine.NewLocalEngineClient(eng),
		))
	case "remote":
		// RemoteExecutor 只做 HTTP transport，不负责回调
		if *executorURL == "" {
			log.Fatal("--executor-url required when --executor=remote")
		}
		executors.SetDefaultExecutor(executors.NewRemoteExecutor(*executorURL))
	default:
		log.Fatalf("unknown --executor type %q (want 'local' or 'remote')", *executorType)
	}

	// --- HTTP server (engine RPC only) ---
	apiSrv := api.NewServer(store, eng)
	httpSrv := &http.Server{
		Addr:         *addr,
		Handler:      apiSrv.RouterEngineOnly(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("engine server listening on %s (executor=%s)", *addr, *executorType)
		errCh <- httpSrv.ListenAndServe()
	}()

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("engine server error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down...", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	sched.Stop()
	log.Println("engine stopped")
}
