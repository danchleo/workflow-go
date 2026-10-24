// Package scheduler provides a general-purpose delayed-job scheduler
// and a cron-triggered workflow scheduler.
//
// The core idea (from mistral-master/mistral/scheduler/default_scheduler.py):
// Mistral policies (wait-before, wait-after, retry delay, timeout) don't
// block — they schedule a callback for later. This scheduler is the Go
// equivalent of Python's SchedulerJob.
//
// Design principles:
//   - Pure generic scheduler takes func() closures, no domain imports.
//   - container/heap min-heap keyed by RunAt + goroutine with time.Timer.
//   - Cron-specific RunOnce stays as a method on a *separate* struct
//     (CronScheduler) that wraps the generic one — it needs db.Store
//     and engine.WorkflowStarter but those don't belong in the core.
package scheduler

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"
)

// Job is one scheduled unit of work.
type Job struct {
	ID     string
	RunAt  time.Time
	Func   func() // closure to execute at RunAt
	cancel bool   // marked for removal by Cancel()
}

// Scheduler accepts delayed jobs and fires them at the correct time.
type Scheduler interface {
	// Schedule schedules fn to run after delay. Returns the job id.
	Schedule(delay time.Duration, fn func()) string
	// ScheduleAt schedules fn to run at a specific time.
	ScheduleAt(at time.Time, fn func()) string
	// Cancel removes a pending job by id. Returns false if not found or already fired.
	Cancel(id string) bool
	// Start begins the dispatch loop in a background goroutine. Safe to call multiple times.
	Start()
	// Stop halts the dispatch loop. Pending jobs are discarded.
	Stop()
}

// DefaultScheduler is the in-memory heap-based implementation.
type DefaultScheduler struct {
	mu    sync.Mutex
	heap  jobHeap
	timer *time.Timer
	wake  chan struct{} // signal to re-poll heap after Schedule/Cancel

	wg     sync.WaitGroup
	stopCh chan struct{}
	once   sync.Once
	started bool
}

// NewDefaultScheduler creates a scheduler. Call Start() to run it.
func NewDefaultScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		heap:    jobHeap{},
		wake:    make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		started: false,
	}
}

// Schedule schedules fn to run after delay. Thread-safe.
func (s *DefaultScheduler) Schedule(delay time.Duration, fn func()) string {
	return s.ScheduleAt(time.Now().Add(delay), fn)
}

// ScheduleAt schedules fn to run at a specific time. Thread-safe.
func (s *DefaultScheduler) ScheduleAt(at time.Time, fn func()) string {
	id := fmt.Sprintf("job-%d", time.Now().UnixNano())
	s.mu.Lock()
	heap.Push(&s.heap, &Job{ID: id, RunAt: at, Func: fn})
	s.pokeLocked()
	s.mu.Unlock()
	return id
}

// Cancel removes a pending job. Returns false if not found or already fired.
func (s *DefaultScheduler) Cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.heap {
		if j.ID == id {
			j.cancel = true
			s.pokeLocked()
			return true
		}
	}
	return false
}

// pokeLocked wakes the dispatch goroutine (caller must hold s.mu).
func (s *DefaultScheduler) pokeLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Start begins the dispatch loop.
func (s *DefaultScheduler) Start() {
	s.once.Do(func() {
		s.started = true
		s.wg.Add(1)
		go s.dispatch()
	})
}

// Stop halts the loop and discards pending jobs.
func (s *DefaultScheduler) Stop() {
	s.once.Do(func() {}) // prevent panic if never started
	if s.started {
		close(s.stopCh)
		s.wg.Wait()
	}
}

// dispatch is the background goroutine that fires due jobs.
func (s *DefaultScheduler) dispatch() {
	defer s.wg.Done()
	s.timer = time.NewTimer(time.Hour) // dummy long timer; we reset it
	defer s.timer.Stop()

	for {
		s.mu.Lock()
		// Pop cancelled jobs off the top.
		for s.heap.Len() > 0 && s.heap[0].cancel {
			heap.Pop(&s.heap)
		}

		var nextDelay time.Duration
		if s.heap.Len() == 0 {
			nextDelay = time.Hour // idle
		} else {
			nextDelay = s.heap[0].RunAt.Sub(time.Now())
			if nextDelay < 0 {
				nextDelay = 0
			}
		}
		s.mu.Unlock()

		s.timer.Reset(nextDelay)

		select {
		case <-s.stopCh:
			return
		case <-s.wake:
			// new job or cancellation — loop back to re-poll
			continue
		case <-s.timer.C:
			// fire all due jobs (and pop cancelled ones)
			s.mu.Lock()
			for s.heap.Len() > 0 {
				top := s.heap[0]
				if top.cancel {
					heap.Pop(&s.heap)
					continue
				}
				if top.RunAt.After(time.Now()) {
					break
				}
				heap.Pop(&s.heap)
				fn := top.Func
				s.mu.Unlock()

				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("scheduler: job panic: %v", r)
						}
					}()
					fn()
				}()

				s.mu.Lock()
			}
			s.mu.Unlock()
		}
	}
}

// jobHeap is a min-heap ordered by RunAt.
type jobHeap []*Job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].RunAt.Before(h[j].RunAt) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*Job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
