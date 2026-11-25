// Package startuptrace records a Go execution trace of Crush's startup, from
// process start until the TUI draws its first frame. Set CRUSH_TRACE to a file
// path to enable it, then analyze the file with scripts/critpath.
package startuptrace

import (
	"context"
	"os"
	"runtime/trace"
	"sync"
)

var (
	file *os.File
	once sync.Once
)

// Start begins tracing when CRUSH_TRACE names a file. It is a no-op otherwise.
func Start() {
	path := os.Getenv("CRUSH_TRACE")
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	if err := trace.Start(f); err != nil {
		_ = f.Close()
		return
	}
	file = f
}

// Stop ends the trace. Only the first call has any effect.
func Stop() {
	once.Do(func() {
		if file == nil {
			return
		}
		// The analyzer ends the critical path at this event.
		trace.Log(context.Background(), "crush", "first-frame")
		trace.Stop()
		_ = file.Close()
	})
}

// Phase runs fn inside a named trace region so the analyzer can split the
// critical path by startup phase.
func Phase(ctx context.Context, name string, fn func()) {
	trace.WithRegion(ctx, name, fn)
}
