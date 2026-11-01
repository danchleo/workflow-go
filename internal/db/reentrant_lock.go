package db

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
)

// ReentrantLock is a mutex that the same goroutine can Lock multiple times
// without deadlocking. Needed because engine entry methods call each other
// on the same goroutine (e.g. OnActionComplete → advanceWorkflow → ...).
// Different goroutines are serialized correctly.
type ReentrantLock interface {
	Lock()
	Unlock()
}

// reentrantLock is a chan-based binary semaphore with goroutine-ID tracking
// for re-entrant acquisition by the same goroutine.
type reentrantLock struct {
	mu    sync.Mutex    // protects owner + depth
	ch    chan struct{} // binary semaphore: 1 token = unlocked
	owner int64         // goroutine ID holding the lock
	depth int32
}

func newReentrantLock() *reentrantLock {
	return &reentrantLock{ch: make(chan struct{}, 1)}
}

func (l *reentrantLock) Lock() {
	gid := goroutineID()
	l.mu.Lock()
	if l.owner == gid {
		l.depth++
		l.mu.Unlock()
		return
	}
	l.mu.Unlock() // release metadata lock BEFORE blocking on channel
	l.ch <- struct{}{}
	l.mu.Lock()
	l.owner = gid
	l.depth = 1
	l.mu.Unlock()
}

func (l *reentrantLock) Unlock() {
	gid := goroutineID()
	l.mu.Lock()
	if l.owner != gid {
		l.mu.Unlock()
		return
	}
	l.depth--
	if l.depth == 0 {
		l.owner = 0
		l.mu.Unlock()
		<-l.ch
		return
	}
	l.mu.Unlock()
}

// --- Goroutine-local in-transaction marker ---
//
// When RunInTransaction is active on the current goroutine, store CRUD
// methods must NOT re-acquire their own locks — RunInTransaction is the
// single serialisation boundary. This avoids the classic deadlock:
// RunInTransaction holds s.mu (write), then fn() calls amExecStore.Get()
// which tries s.mu.RLock() and blocks forever on its own write lock.
//
// We key depth by goroutine ID (not atomic global) so multiple concurrent
// goroutines can be in their own transactions independently.
var (
	inTxMu     sync.Mutex
	inTxDepths = make(map[int64]int)
)

func enterTx() {
	gid := goroutineID()
	inTxMu.Lock()
	inTxDepths[gid]++
	inTxMu.Unlock()
}

func exitTx() {
	gid := goroutineID()
	inTxMu.Lock()
	inTxDepths[gid]--
	if inTxDepths[gid] == 0 {
		delete(inTxDepths, gid)
	}
	inTxMu.Unlock()
}

func inTx() bool {
	gid := goroutineID()
	inTxMu.Lock()
	d := inTxDepths[gid]
	inTxMu.Unlock()
	return d > 0
}

// txDepth returns the current goroutine's transaction nesting depth.
// 0 = outside any RunInTransaction; 1 = outermost call; >1 = nested savepoint.
func txDepth() int {
	gid := goroutineID()
	inTxMu.Lock()
	d := inTxDepths[gid]
	inTxMu.Unlock()
	return d
}

// goroutineID returns the current goroutine's runtime ID by parsing
// runtime.Stack output. This is the official Go approach — the runtime
// doesn't expose a direct getter. Called only on lock acquisition paths,
// not on every data access, so performance is acceptable.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// format: "goroutine 123 [running]..."
	fields := strings.Fields(string(buf[:n]))
	if len(fields) >= 2 {
		var id int64
		fmt.Sscanf(fields[1], "%d", &id)
		return id
	}
	return 0
}
