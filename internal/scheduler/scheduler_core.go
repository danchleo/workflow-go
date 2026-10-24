// Package scheduler provides a general-purpose delayed-job scheduler that
// mirrors Python mistral.scheduler's DefaultScheduler architecture:
//
//  1. On Schedule() → persist to DB FIRST (ScheduledJob), then push to
//     in-memory heap for fast local dispatch.
//  2. A second goroutine (jobStoreChecker) polls DB for due uncaptured jobs
//     — this is the crash-recovery path. After a process crash, heap jobs
//     are lost but DB jobs survive; on restart the checker picks them up.
//  3. captured_at optimistic lock prevents duplicate execution across
//     multiple scheduler instances (UPDATE ... WHERE captured_at = <old>).
//  4. Stuck captures (captured_at older than captureTimeout) are eligible
//     for re-capture — handles the case where a scheduler crashed mid-job.
//
// Unlike Python's string-reflection approach, Go uses a JobType + handler
// registry (closure-based, not reflection). The registry is populated by the
// engine at init time with handlers for: fail_if_incomplete, continue_task,
// complete_task, advance_workflow, etc.
package scheduler

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/db"
)

// JobType identifies the kind of delayed work — must match the registry.
type JobType string

// Built-in job types that engine registers.
const (
	JobFailIfIncomplete JobType = "fail_if_incomplete"
	JobContinueTask     JobType = "continue_task" // retry delayed → re-enter StartTask
	JobCompleteTask     JobType = "complete_task" // wait-after delayed → set final state + ContinueWorkflow
)

// JobHandlerRegistry maps JobType → handler function.
// Handlers are registered by engine on startup; scheduler stays generic.
var (
	jobHandlersMu sync.RWMutex
	jobHandlers   = map[JobType]func(args map[string]interface{}){}
)

// RegisterJobHandler registers a handler for a JobType. Called by engine init.
func RegisterJobHandler(t JobType, fn func(args map[string]interface{})) {
	jobHandlersMu.Lock()
	defer jobHandlersMu.Unlock()
	jobHandlers[t] = fn
}

// jobHeapElem is an in-memory heap node (separate from the DB record).
type jobHeapElem struct {
	executeAt time.Time
	seq       int // tiebreaker for same execution time
	dbJobID   string
}

type jobHeap []*jobHeapElem

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].executeAt.Before(h[j].executeAt) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*jobHeapElem))
}
func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Scheduler accepts delayed jobs and fires them at the correct time.
// All implementations use the shared DB for persistency.
type Scheduler interface {
	// Schedule persists a job to DB and (if in-memory) pushes to heap.
	Schedule(jobType JobType, args map[string]interface{}, delay time.Duration) (string, error)
	// Cancel removes a pending job by id.
	Cancel(id string) error
	// Start begins the heap dispatcher + job-store checker goroutines.
	Start()
	// Stop halts both goroutines. Heap jobs are dropped; DB jobs survive.
	Stop()
}

// DefaultScheduler is the heap + DB-backed implementation — the Go
// equivalent of Python's mistral.scheduler.DefaultScheduler.
type DefaultScheduler struct {
	store db.Store

	mu   sync.Mutex
	heap jobHeap
	seq  int
	wake chan struct{}

	dispatcherStop chan struct{}
	checkerStop    chan struct{}
	wg             sync.WaitGroup
	once           sync.Once
	captureTimeout time.Duration // how long captured_at is valid before re-capture
	pollInterval   time.Duration // how often jobStoreChecker polls DB
	started        bool
}

// NewDefaultScheduler creates a scheduler backed by the given store.
// Parameters:
//
//	pollInterval — DB poll cadence for crash-recovery jobs (default 500ms if 0)
//	captureTimeout — how long a captured job is still "owned" before
//	  another scheduler can re-capture (default 30s if 0).
func NewDefaultScheduler(store db.Store, pollInterval, captureTimeout time.Duration) *DefaultScheduler {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if captureTimeout <= 0 {
		captureTimeout = 30 * time.Second
	}
	return &DefaultScheduler{
		store:          store,
		wake:           make(chan struct{}, 1),
		captureTimeout: captureTimeout,
		pollInterval:   pollInterval,
	}
}

// Schedule persists a ScheduledJob to DB first, then pushes to in-memory heap.
// Returns the DB job id (caller can pass it back to Cancel if needed).
func (s *DefaultScheduler) Schedule(jobType JobType, args map[string]interface{}, delay time.Duration) (string, error) {
	id := fmt.Sprintf("sj-%d", time.Now().UnixNano())
	executeAt := db.Now().Add(delay)

	j := &db.ScheduledJob{
		ID:        id,
		JobType:   string(jobType),
		JobArgs:   args,
		ExecuteAt: executeAt,
		CreatedAt: db.Now(),
	}
	if err := s.store.ScheduledJobs().Create(j); err != nil {
		return "", fmt.Errorf("persist scheduled job: %w", err)
	}

	s.mu.Lock()
	s.seq++
	heap.Push(&s.heap, &jobHeapElem{
		executeAt: executeAt,
		seq:       s.seq,
		dbJobID:   id,
	})
	s.pokeLocked()
	s.mu.Unlock()

	return id, nil
}

// Cancel deletes a pending job from DB. If it's also in the in-memory heap,
// the heap dispatcher will skip it when it fires (DB won't be there to capture).
func (s *DefaultScheduler) Cancel(id string) error {
	// Delete from DB. If already captured/fired, Delete returns not-found — that's fine.
	return s.store.ScheduledJobs().Delete(id)
}

func (s *DefaultScheduler) pokeLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Start begins both goroutines. Safe to call multiple times.
func (s *DefaultScheduler) Start() {
	s.once.Do(func() {
		s.started = true
		s.dispatcherStop = make(chan struct{})
		s.checkerStop = make(chan struct{})

		s.wg.Add(1)
		go s.heapDispatcher()

		s.wg.Add(1)
		go s.jobStoreChecker()
	})
}

// Stop halts both goroutines. In-memory heap jobs are dropped; DB jobs survive
// to be picked up by the job-store checker after next start.
func (s *DefaultScheduler) Stop() {
	s.once.Do(func() {})
	if s.started {
		close(s.dispatcherStop)
		close(s.checkerStop)
		s.wg.Wait()
	}
}

// heapDispatcher fires jobs that are in-memory. It still goes through Capture()
// on the DB row for multi-safety, and always invokes the handler via
// invokeJob which uses Capture's result for consistency.
func (s *DefaultScheduler) heapDispatcher() {
	defer s.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		s.mu.Lock()
		for s.heap.Len() > 0 {
			top := s.heap[0]
			if top.executeAt.After(time.Now()) {
				break
			}
			heap.Pop(&s.heap)

			// Capture via optimistic lock — the DB row must still be there
			// AND not already captured by another scheduler.
			dbJob, err := s.store.ScheduledJobs().Get(top.dbJobID)
			if err != nil || dbJob == nil {
				continue // deleted or missing — skip
			}
			captured, err := s.store.ScheduledJobs().Capture(dbJob.ID, dbJob.CapturedAt, db.Now())
			if err != nil || !captured {
				continue // another scheduler claimed it — skip
			}
			s.mu.Unlock()
			s.invokeAndDelete(dbJob)
			s.mu.Lock()
		}

		var nextDelay time.Duration
		if s.heap.Len() == 0 {
			nextDelay = time.Hour
		} else {
			nextDelay = s.heap[0].executeAt.Sub(time.Now())
			if nextDelay < 0 {
				nextDelay = 0
			}
		}
		s.mu.Unlock()

		timer.Reset(nextDelay)

		select {
		case <-s.dispatcherStop:
			return
		case <-s.wake:
			continue
		case <-timer.C:
			// loop back to pop
		}
	}
}

// jobStoreChecker is the crash-recovery + DB-driven path — poll DB for due jobs
// that aren't captured (or whose captures are stale due to scheduler crash).
// Mirrors Python DefaultScheduler._job_store_checker + _process_store_jobs.
func (s *DefaultScheduler) jobStoreChecker() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.checkerStop:
			return
		case <-ticker.C:
			s.processStoreJobs()
		}
	}
}

func (s *DefaultScheduler) processStoreJobs() {
	jobs, err := s.store.ScheduledJobs().GetDueJobs(db.Now(), s.captureTimeout, 0)
	if err != nil {
		log.Printf("scheduler: GetDueJobs: %v", err)
		return
	}
	for _, j := range jobs {
		captured, err := s.store.ScheduledJobs().Capture(j.ID, j.CapturedAt, db.Now())
		if err != nil || !captured {
			continue // another scheduler claimed it
		}
		s.invokeAndDelete(j)
	}
}

func (s *DefaultScheduler) invokeAndDelete(j *db.ScheduledJob) {
	jobHandlersMu.RLock()
	handler, ok := jobHandlers[JobType(j.JobType)]
	jobHandlersMu.RUnlock()

	if !ok {
		log.Printf("scheduler: no handler for job type %s (id=%s) — deleting", j.JobType, j.ID)
		_ = s.store.ScheduledJobs().Delete(j.ID)
		return
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("scheduler: job %s id=%s panic: %v", j.JobType, j.ID, r)
			}
		}()
		handler(j.JobArgs)
	}()

	if err := s.store.ScheduledJobs().Delete(j.ID); err != nil {
		log.Printf("scheduler: delete job %s: %v", j.ID, err)
	}
}
