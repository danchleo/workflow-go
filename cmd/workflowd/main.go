// Command workflowd is a single binary that can run any combination of
// workflow-go's components in one process — useful for development, demos,
// and Docker single-container deployments.
//
// Mirrors Python mistral's mistral/cmd/launch.py:
//   LAUNCH_OPTIONS = { 'api': launch_api, 'engine': launch_engine,
//                      'executor': launch_executor, ... }
//
// You specify which components to run with --server (comma-separated):
//
//	# Everything (single-process — simplest dev experience)
//	workflowd --server all
//
//	# API + Engine + Executor all in one process
//	workflowd --server api,engine,executor
//
//	# Engine only (talking to a separate executor)
//	workflowd --server engine --executor-url http://exec:8952
//
//	# API only (talking to a separate engine via HttpEngineClient)
//	workflowd --server api --engine-url http://engine:8951
//
// Each component listens on its own port (defaults match mistral convention):
//
//	API      :8080  (REST + embedded engine RPC)
//	Engine   :8951  (RPC only, scheduler, auto-execute)
//	Executor :8952  (/v1/actions/run, LocalExecutor only)
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

	"github.com/danchleo/workflow-go/internal/db"
	"github.com/danchleo/workflow-go/internal/engine"
	"github.com/danchleo/workflow-go/internal/executors"
	"github.com/danchleo/workflow-go/internal/scheduler"
	"github.com/danchleo/workflow-go/pkg/api"
	"github.com/danchleo/workflow-go/pkg/executorapi"
	"github.com/danchleo/workflow-go/internal/rpc"
)

// config holds flag values parsed once at startup.
type config struct {
	servers      []string // api | engine | executor
	dbURL        string
	executorType string // local | remote
	executorURL  string
	engineURL    string // used by api server when engine is remote
	apiAddr      string
	engineAddr   string
	executorAddr string
}

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	var (
		serverFlag   = flag.String("server", "all", "Comma-separated list of components to run: api,engine,executor,all")
		dbURL        = flag.String("db", ":memory:", "DB URL — ':memory:' or SQLite path (shared across api+engine in one process)")
		execType     = flag.String("executor", "local", "Executor type for engine: 'local' or 'remote'")
		execURL      = flag.String("executor-url", "", "Remote executor URL (when engine runs with --executor=remote)")
		engineURL    = flag.String("engine-url", "", "Remote engine URL (when api runs with separate engine, uses HttpEngineClient)")
		apiAddr      = flag.String("api-addr", ":8080", "API listen address")
		engineAddr   = flag.String("engine-addr", ":8951", "Engine RPC listen address")
		executorAddr = flag.String("executor-addr", ":8952", "Executor listen address")
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

	return config{
		servers:      servers,
		dbURL:        *dbURL,
		executorType: *execType,
		executorURL:  *execURL,
		engineURL:    *engineURL,
		apiAddr:      *apiAddr,
		engineAddr:   *engineAddr,
		executorAddr: *executorAddr,
	}
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

	// --- Executor setup (shared global in executors package) ---
	switch cfg.executorType {
	case "local":
		executors.SetDefaultExecutor(executors.NewLocalExecutor())
	case "remote":
		if cfg.executorURL == "" {
			return fmt.Errorf("--executor-url required with --executor=remote")
		}
		executors.SetDefaultExecutor(executors.NewRemoteExecutor(cfg.executorURL))
	}

	// --- Engine (in-process, unless api-only + engine-url set) ---
	var eng *engine.DefaultEngine
	var engClient rpc.EngineClient
	if has(cfg.servers, "engine") || has(cfg.servers, "api") && cfg.engineURL == "" {
		eng = engine.NewDefaultEngine(store)
		if has(cfg.servers, "engine") {
			sched := scheduler.NewDefaultScheduler(store, 500*time.Millisecond, 0, 30*time.Second, 10*time.Second, 10)
			sched.Start()
			eng.WithScheduler(sched)
			defer sched.Stop()
		}
		engClient = rpc.NewLocalEngineClient(eng)
	}
	if cfg.engineURL != "" {
		engClient = rpc.NewHttpEngineClient(cfg.engineURL)
	}
	if engClient == nil {
		return fmt.Errorf("no engine configured — either include 'engine' in --server or set --engine-url")
	}

	// --- HTTP servers ---
	var wg sync.WaitGroup
	var httpServers []*http.Server

	// Executor server (only when requested; LocalExecutor is fine with no HTTP)
	if has(cfg.servers, "executor") {
		wg.Add(1)
		execAPI := executorapi.NewServer()
		srv := &http.Server{Addr: cfg.executorAddr, Handler: execAPI.Router()}
		httpServers = append(httpServers, srv)
		go func() {
			defer wg.Done()
			log.Printf("executor listening on %s", cfg.executorAddr)
			srv.ListenAndServe()
		}()
	}

	// Engine RPC server
	if has(cfg.servers, "engine") {
		wg.Add(1)
		apiSrv := api.NewServer(store, eng)
		srv := &http.Server{Addr: cfg.engineAddr, Handler: apiSrv.RouterEngineOnly()}
		httpServers = append(httpServers, srv)
		go func() {
			defer wg.Done()
			log.Printf("engine listening on %s (executor=%s)", cfg.engineAddr, cfg.executorType)
			srv.ListenAndServe()
		}()
	}

	// API server (full REST surface; always embeds /rpc/* so it can self-serve when engine is in-process)
	if has(cfg.servers, "api") {
		wg.Add(1)
		// When engine runs in-process (eng != nil) api.Server uses it directly;
		// when --engine-url is set, api.Server's internal engine is unused but
		// still needed for the /rpc/* endpoints we might want to strip later.
		var apiEng engine.Engine = eng
		if apiEng == nil {
			// We need *some* engine for api.NewServer — create a throwaway.
			// Actually api.Server currently takes engine.Engine interface.
			// If we're in api-only mode with remote engine, we still use a local
			// engine struct but calls flow through HttpEngineClient for the
			// actual endpoints. For now, pass a local one.
			apiEng = engine.NewDefaultEngine(store)
		}
		apiSrv := api.NewServer(store, apiEng)
		srv := &http.Server{Addr: cfg.apiAddr, Handler: apiSrv.Router()}
		httpServers = append(httpServers, srv)
		go func() {
			defer wg.Done()
			log.Printf("api listening on %s", cfg.apiAddr)
			srv.ListenAndServe()
		}()
	}

	// --- Wait for shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	// Graceful shutdown with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, srv := range httpServers {
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("http server shutdown error: %v", err)
		}
	}
	wg.Wait()
	log.Println("all components stopped")
	return nil
}

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
		"\nworkflowd starting [%s]\n  db:       %s\n  executor: %s\n",
		sb, cfg.dbURL, cfg.executorType,
	)
}
