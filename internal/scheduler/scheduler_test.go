package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestDefaultScheduler_FiresInOrder(t *testing.T) {
	s := NewDefaultScheduler()
	s.Start()
	defer s.Stop()

	var mu sync.Mutex
	var order []int

	s.Schedule(50*time.Millisecond, func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, 2)
	})
	s.Schedule(10*time.Millisecond, func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, 1)
	})
	s.Schedule(30*time.Millisecond, func() {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, 3)
	})

	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(order))
	}
	// Order should be 1 (10ms) → 3 (30ms) → 2 (50ms).
	expected := []int{1, 3, 2}
	for i := range expected {
		if order[i] != expected[i] {
			t.Errorf("order[%d] = %d, want %d", i, order[i], expected[i])
		}
	}
}

func TestDefaultScheduler_CancelWorks(t *testing.T) {
	s := NewDefaultScheduler()
	s.Start()
	defer s.Stop()

	var fired bool
	id := s.Schedule(100*time.Millisecond, func() {
		fired = true
	})

	// Cancel immediately before it fires.
	if !s.Cancel(id) {
		t.Error("Cancel returned false")
	}

	time.Sleep(150 * time.Millisecond)

	if fired {
		t.Error("cancelled job fired anyway")
	}
}

func TestDefaultScheduler_ImmediateJobsFire(t *testing.T) {
	s := NewDefaultScheduler()
	s.Start()
	defer s.Stop()

	var ch = make(chan struct{}, 1)
	s.Schedule(0, func() {
		ch <- struct{}{}
	})

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("immediate job didn't fire")
	}
}
