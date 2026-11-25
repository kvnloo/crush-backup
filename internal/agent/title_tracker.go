package agent

import (
	"context"
	"sync"
	"time"
)

// titleTimeout bounds one title generation so a stalled request ends.
const titleTimeout = 60 * time.Second

// titleTracker tracks in-flight title generations so shutdown can stop them
// and wait for their last database write before the database closes. The
// zero value is ready to use.
type titleTracker struct {
	mu      sync.Mutex
	n       int
	idle    chan struct{} // Closed when n drops back to zero.
	cancels map[*int]context.CancelFunc
	stopped bool
}

// begin registers a title generation and returns the context it must run
// under, detached from parent's cancellation so the title outlives its run,
// but cancelled by stop. Pair each call with the returned end.
func (t *titleTracker) begin(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	key := new(int)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.n == 0 {
		t.idle = make(chan struct{})
	}
	t.n++
	if t.stopped {
		cancel()
	}
	if t.cancels == nil {
		t.cancels = make(map[*int]context.CancelFunc)
	}
	t.cancels[key] = cancel

	return ctx, func() {
		cancel()
		t.mu.Lock()
		defer t.mu.Unlock()
		delete(t.cancels, key)
		t.n--
		if t.n == 0 {
			close(t.idle)
		}
	}
}

// stop cancels every title in flight and every one begun later. A cancelled
// generation still saves a fallback title, so the session is never left
// untitled; it just does not wait on the model for a better one.
func (t *titleTracker) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	for _, cancel := range t.cancels {
		cancel()
	}
}

// wait blocks until no title is in flight or timeout passes, and reports
// whether every title finished.
func (t *titleTracker) wait(timeout time.Duration) bool {
	t.mu.Lock()
	n, idle := t.n, t.idle
	t.mu.Unlock()
	if n == 0 {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-idle:
		return true
	case <-timer.C:
		return false
	}
}
