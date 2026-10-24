// Command mistral is the main entry point for the Mistral Go workflow engine.
//
// Run without arguments to start the REST API server.
// Run with `--demo` to execute an example workflow and print results.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mistral/workflow/internal/db"
	"github.com/mistral/workflow/internal/engine"
	"github.com/mistral/workflow/internal/lang/parser"
	v2 "github.com/mistral/workflow/internal/lang/v2"
	"github.com/mistral/workflow/internal/services"
	"github.com/mistral/workflow/internal/states"
	"github.com/mistral/workflow/pkg/api"
)

func main() {
	demo := flag.Bool("demo", false, "Run an example workflow and print results")
	addr := flag.String("addr", ":8080", "API server address")
	reverse := flag.Bool("reverse", false, "Use a reverse workflow demo instead of direct")
	flag.Parse()

	store := db.NewInMemoryStore()
	eng := engine.NewDefaultEngine(store)

	if *demo {
		if *reverse {
			runDemoReverse(store, eng)
		} else {
			runDemoDirect(store, eng)
		}
		return
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	srv := api.NewServer(store, eng)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(*addr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatal(err)
		}
	case sig := <-sigCh:
		log.Printf("Received signal %s, shutting down...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Stop(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}
}

func runDemoDirect(store db.Store, eng *engine.DefaultEngine) {
	fmt.Println("=== Mistral Go Engine — Direct Workflow Demo ===")
	fmt.Println()

	wfYAML := api.NewDemoWorkflowDef()

	// Step 1: Create workflow definition.
	wfSvc := services.NewWorkflowService(store)
	wfDef, err := wfSvc.CreateWorkflow("demo_workflow", wfYAML, "demo-project", "")
	if err != nil {
		log.Fatalf("creating workflow: %v", err)
	}
	fmt.Printf("[1] Created workflow definition: id=%s\n", wfDef.ID)

	// Step 2: Validate spec.
	wfSpec, err := v2.NewWorkflowSpec(wfDef.Name, wfDef.Spec)
	if err != nil {
		log.Fatalf("parsing workflow spec: %v", err)
	}
	fmt.Printf("[2] Validated workflow spec: name=%s, type=%s, tasks=%d\n",
		wfSpec.GetName(), wfSpec.GetType(), len(wfSpec.GetTasks()))
	for name, t := range wfSpec.GetTasks() {
		fmt.Printf("    - task %q: action=%s\n", name, t.GetActionName())
	}

	// Step 3: Start workflow execution.
	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}
	input := map[string]interface{}{
		"greeting": "Hi",
		"name":     "Mistral",
	}

	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", input, "demo execution", params)
	if err != nil {
		log.Fatalf("starting workflow: %v", err)
	}
	fmt.Printf("[3] Workflow started: id=%s, state=%s\n", wfEx.ID, wfEx.State)

	// Step 4: Simulate task completion by running action handler.
	// Loop until all tasks complete.
	const maxIterations = 20
	for i := 0; i < maxIterations; i++ {
		tasks, _ := store.TaskExecutions().List(wfEx.ID, "")

		// Check if all tasks completed.
		allDone := true
		var pendingTask *db.TaskExecution
		for _, t := range tasks {
			if !states.IsCompleted(t.State) {
				allDone = false
				if pendingTask == nil {
					pendingTask = t
				}
			}
		}

		if allDone {
			break
		}

		t := pendingTask
		fmt.Printf("[4.%d] Running task: name=%s, id=%s, state=%s\n", i+1, t.Name, t.ID, t.State)

		// Simulate action run and completion.
		result := &db.Result{Data: fmt.Sprintf("'%s' completed", t.Name)}
		if err := eng.OnActionComplete(context.Background(), t.ID, result, false); err != nil {
			log.Printf("    OnActionComplete error: %v", err)
		}

		time.Sleep(5 * time.Millisecond)
	}

	// Step 5: Check final workflow state.
	time.Sleep(20 * time.Millisecond)
	wfEx, _ = store.WorkflowExecutions().Get(wfEx.ID)
	fmt.Printf("[5] Final workflow state: %s\n", wfEx.State)
	if len(wfEx.Output) > 0 {
		out, _ := json.MarshalIndent(wfEx.Output, "    ", "  ")
		fmt.Printf("    Output: %s\n", out)
	}

	if wfEx.State == states.SUCCESS {
		fmt.Println()
		fmt.Println("✅ Demo workflow completed successfully!")
		os.Exit(0)
	} else {
		fmt.Printf("❌ Demo workflow ended with state %s\n", wfEx.State)
		fmt.Printf("   state_info: %v\n", wfEx.StateInfo)
		os.Exit(1)
	}
}

func runDemoReverse(store db.Store, eng *engine.DefaultEngine) {
	fmt.Println("=== Mistral Go Engine — Reverse Workflow Demo ===")
	fmt.Println()

	wfYAML := api.NewReverseDemoWorkflowDef()

	wfSvc := services.NewWorkflowService(store)
	wfDef, err := wfSvc.CreateWorkflow("demo_reverse", wfYAML, "demo-project", "")
	if err != nil {
		log.Fatalf("creating workflow: %v", err)
	}
	fmt.Printf("[1] Created workflow definition: id=%s\n", wfDef.ID)

	params := map[string]interface{}{
		"env":       make(map[string]interface{}),
		"namespace": "",
	}

	wfEx, err := eng.StartWorkflow(context.Background(), wfDef.ID, "", "", nil, "reverse demo", params)
	if err != nil {
		log.Fatalf("starting workflow: %v", err)
	}
	fmt.Printf("[2] Workflow started: id=%s, state=%s\n", wfEx.ID, wfEx.State)

	// Run all tasks.
	tasks, _ := store.TaskExecutions().List(wfEx.ID, "")
	for _, t := range tasks {
		fmt.Printf("[3] Running task: %s (state=%s)\n", t.Name, t.State)
		result := &db.Result{Data: t.Name + " done"}
		if err := eng.OnActionComplete(context.Background(), t.ID, result, false); err != nil {
			log.Printf("    error: %v", err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	wfEx, _ = store.WorkflowExecutions().Get(wfEx.ID)
	fmt.Printf("[4] Final workflow state: %s\n", wfEx.State)

	if wfEx.State == states.SUCCESS {
		fmt.Println()
		fmt.Println("✅ Reverse demo workflow completed successfully!")
	}
}

// Silence unused import warning.
var _ = parser.ParseYAML
