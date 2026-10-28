// Command executor is a standalone action-executor process.
//
// Mirrors Python mistral's mistral/executors/executor_server.py:
// This process owns the LocalExecutor and exposes it over HTTP at
// /v1/actions/run. An engine process with --executor=remote points its
// RemoteExecutor at this server's base URL.
//
// The executor does NOT own a DB store or engine — it's purely a compute
// worker. It receives runActionRequest JSON, runs the action, and returns
// runActionResponse JSON.
//
// Usage:
//
//	go run ./cmd/executor --addr ":8952"
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/danchleo/workflow-go/pkg/executorapi"
)

func main() {
	addr := flag.String("addr", ":8952", "Executor HTTP listen address")
	flag.Parse()

	srv := executorapi.NewServer()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(*addr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("executor server error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, shutting down executor...", sig)
		// executorapi.Server doesn't expose Shutdown yet; just exit.
	}
}
