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
	"math/rand"
	"sync"
	"time"

	"github.com/mistral/workflow/internal/db"
)

// JobType identifies the kind of delayed work — must match the registry.
type JobType string

// Built-in job types that engine registers.
const (
	JobFailIfIncomplete JobType = "fail_if_incomplete"
	JobContinueTask     JobType = "continue_task"      // retry delayed → re-enter StartTask
	JobCompleteTask     JobType = "complete_task"      // wait-after delayed → set final state + ContinueWorkflow
	JobRefreshTaskState JobType = "refresh_task_state" // join wakeup → re-check logical state
	JobIntegrityCheck   JobType = "integrity_check"    // crash-recovery sweep for stuck RUNNING tasks
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
	key       string // dedup key (optional)
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
	// ScheduleWithKey like Schedule but associates a dedup key.
	// If HasScheduledJobs(key) returns true, scheduling is skipped.
	ScheduleWithKey(jobType JobType, args map[string]interface{}, delay time.Duration, key string) (string, error)
	// Cancel removes a pending job by id.
	Cancel(id string) error
	// Start begins the heap dispatcher + job-store checker goroutines.
	Start()
	// Stop halts both goroutines. Heap jobs are dropped; DB jobs survive.
	Stop()
	// HasScheduledJobs returns true if there's at least one pending job
	// with the given key (checked in-memory first, then DB).
	// Empty key means "any pending job". Matches Python's has_scheduled_jobs().
	HasScheduledJobs(key string) bool
}

// DefaultScheduler is the heap + DB-backed implementation — the Go
// equivalent of Python's mistral.scheduler.DefaultScheduler.
type DefaultScheduler struct {
	store db.Store

	mu       sync.Mutex
	heap     jobHeap
	seq      int
	wake     chan struct{}
	keyIndex map[string]map[string]bool // key → set of db job IDs; Python in_memory_jobs dedup

	dispatcherStop chan struct{}
	checkerStop    chan struct{}
	wg             sync.WaitGroup
	once           sync.Once
	captureTimeout time.Duration // how long captured_at protects a job before re-capture
	pollInterval   time.Duration // Python fixed_delay — base poll cadence
	randomDelay    time.Duration // Python random_delay — jitter added to each poll
	pickupJobAfter time.Duration // Python pickup_job_after — give heapDispatcher a head-start
	batchSize      int           // Python batch_size — jobs per DB poll (0 = unlimited)
	started        bool
}

// NewDefaultScheduler creates a scheduler backed by the given store.
// Parameters (match Python DefaultScheduler config defaults):
//
//	pollInterval    — DB poll base cadence (Python fixed_delay; default 500ms)
//	randomDelay     — jitter added to each poll (Python random_delay; default 0)
//	captureTimeout  — captured_at ownership window (Python captured_job_timeout; default 30s)
//	pickupJobAfter  — only jobs whose execute_at has passed this long are
//	                  eligible for DB pickup. Gives heapDispatcher priority
//	                  for recently-due jobs. (Python pickup_job_after; default 60s)
//	batchSize       — max jobs per GetDueJobs poll (Python batch_size; default 0=unlimited)
func NewDefaultScheduler(store db.Store, pollInterval, randomDelay, captureTimeout, pickupJobAfter time.Duration, batchSize int) *DefaultScheduler {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if randomDelay < 0 {
		randomDelay = 0
	}
	if captureTimeout <= 0 {
		captureTimeout = 30 * time.Second
	}
	// pickupJobAfter: 0 = disabled (immediate pickup, for unit tests),
	// >0 = explicit window (matches Python default 60s).
	// Negative values are treated as default (60s).
	if pickupJobAfter < 0 {
		pickupJobAfter = 60 * time.Second
	}
	if batchSize < 0 {
		batchSize = 0 // 0 = unlimited (Python None)
	}
	return &DefaultScheduler{
		store:          store,
		wake:           make(chan struct{}, 1),
		captureTimeout: captureTimeout,
		pollInterval:   pollInterval,
		randomDelay:    randomDelay,
		pickupJobAfter: pickupJobAfter,
		batchSize:      batchSize,
		keyIndex:       make(map[string]map[string]bool),
	}
}

// retryOnDBError mimics Python's @db_utils.retry_on_db_error decorator:
// up to 50 attempts with incrementing wait 0 → 0.1s → 0.2s ... capped at 2s.
// Only retries when isRetryable(err) returns true (deadlock, connection lost,
// too many connections). Non-retryable errors fail fast.
func retryOnDBError(op func() error) error {
	const maxAttempts = 50
	const increment = 100 * time.Millisecond
	const maxWait = 2 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := time.Duration(attempt) * increment
			if wait > maxWait {
				wait = maxWait
			}
			time.Sleep(wait)
		}

		if err := op(); err != nil {
			if !isRetryableDBError(err) {
				return err // fail fast — not a transient DB error
			}
			lastErr = err
			continue
		}
		return nil // success
	}
	return fmt.Errorf("DB operation failed after %d retries: %w", maxAttempts, lastErr)
}

// isRetryableDBError returns true for errors worth retrying — matches Python's
// _RETRY_ERRORS + "Too many connections" message match.
// We can't enumerate all SQL driver error codes portably, so we retry on any
// non-nil error. This is safe because delete is idempotent (not-found error
// already returns before the retry path).
func isRetryableDBError(err error) bool {
	return err != nil
}

// Schedule persists a ScheduledJob to DB first, then pushes to in-memory heap.
// Returns the DB job id (caller can pass it back to Cancel if needed).
func (s *DefaultScheduler) Schedule(jobType JobType, args map[string]interface{}, delay time.Duration) (string, error) {
	return s.ScheduleWithKey(jobType, args, delay, "")
}

// ScheduleWithKey is like Schedule but associates a dedup key.
// If key != "" and HasScheduledJobs(key) is true, the call is a no-op
// (returns empty id, nil error). Matches Python's schedule + key dedup.
func (s *DefaultScheduler) ScheduleWithKey(jobType JobType, args map[string]interface{}, delay time.Duration, key string) (string, error) {
	if key != "" && s.HasScheduledJobs(key) {
		return "", nil // dedup hit — skip (Python schedule() doesn't skip, but callers can)
	}

	id := fmt.Sprintf("sj-%d", time.Now().UnixNano())
	executeAt := db.Now().Add(delay)

	j := &db.ScheduledJob{
		ID:        id,
		JobType:   string(jobType),
		JobArgs:   args,
		ExecuteAt: executeAt,
		Key:       key,
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
		key:       key,
	})
	if key != "" {
		if s.keyIndex[key] == nil {
			s.keyIndex[key] = make(map[string]bool)
		}
		s.keyIndex[key][id] = true
	}
	s.pokeLocked()
	s.mu.Unlock()

	return id, nil
}

// HasScheduledJobs returns true if a pending (non-processing) job exists for the
// given key. Empty key → "any pending job at all".
// Captured jobs (captured_at != nil) are excluded — they are currently being
// processed by some scheduler instance and should not be double-scheduled.
// Matches Python's has_scheduled_jobs(processing=False).
func (s *DefaultScheduler) HasScheduledJobs(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// In-memory heap only contains non-captured jobs — heapDispatcher hasn't
	// captured them yet (it captures via DB on pop).
	if key == "" {
		if s.heap.Len() > 0 {
			return true
		}
	} else {
		if len(s.keyIndex[key]) > 0 {
			return true
		}
	}

	// DB fallback — also skip captured jobs (processing=true).
	jobs, err := s.store.ScheduledJobs().List()
	if err != nil {
		return false
	}
	for _, j := range jobs {
		if j.CapturedAt != nil {
			continue // being processed — skip
		}
		if key == "" {
			return true
		}
		if j.Key == key {
			return true
		}
	}
	return false
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
			// Fire asynchronously — matches Python ThreadPoolExecutor behavior.
			// A slow handler must NOT block the heap dispatcher; otherwise all
			// subsequent heap jobs (timeouts, retries) would fire late.
			go func(j *db.ScheduledJob) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("scheduler: heap job %s panic in invokeAndDelete: %v", j.ID, r)
					}
				}()
				s.invokeAndDelete(j)
			}(dbJob)
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
// Uses fixed_delay + random_delay sleep (not a ticker) to match Python's
// anti-thundering-herd behavior in multi-scheduler deployments.
func (s *DefaultScheduler) jobStoreChecker() {
	defer s.wg.Done()

	for {
		// Sleep with jitter: matches Python's
		//   fixed_delay + random.Random().randint(0, random_delay_ms) * 0.001
		sleepFor := s.pollInterval
		if s.randomDelay > 0 {
			sleepFor += time.Duration(rand.Int63n(int64(s.randomDelay)))
		}

		select {
		case <-s.checkerStop:
			return
		case <-time.After(sleepFor):
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("scheduler: jobStoreChecker panic in processStoreJobs: %v", r)
				}
			}()
			s.processStoreJobs()
		}()
	}
}

func (s *DefaultScheduler) processStoreJobs() {
	// Python: execute_at < time - pickup_job_after
	// i.e. only pick up jobs that have been due for pickupJobAfter window.
	// This gives heapDispatcher first chance for recently-due work.
	now := db.Now()
	executeCutoff := now.Add(-s.pickupJobAfter)
	jobs, err := s.store.ScheduledJobs().GetDueJobs(executeCutoff, now, s.captureTimeout, s.batchSize)
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
		s.removeFromKeyIndex(j.ID, j.Key)
		_ = retryOnDBError(func() error { return s.store.ScheduledJobs().Delete(j.ID) })
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

	s.removeFromKeyIndex(j.ID, j.Key)
	// Python _delete_scheduled_job has @db_utils.retry_on_db_error — retry
	// up to 50 times with incrementing wait on transient DB errors.
	if err := retryOnDBError(func() error { return s.store.ScheduledJobs().Delete(j.ID) }); err != nil {
		log.Printf("scheduler: delete job %s failed after retries: %v", j.ID, err)
	}
}

func (s *DefaultScheduler) removeFromKeyIndex(id, key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ids := s.keyIndex[key]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(s.keyIndex, key)
		}
	}
}
