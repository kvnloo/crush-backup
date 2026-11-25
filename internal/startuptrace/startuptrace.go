// Package startuptrace records Go execution traces of Crush's startup and
// shutdown for scripts/critpath to analyze.
//
// CRUSH_TRACE=file traces from process start until the TUI draws its first
// frame. CRUSH_TRACE_SHUTDOWN=file traces from the moment the user confirms
// quitting until main returns, which is when the shell gets its prompt back.
package startuptrace

import (
	"context"
	"os"
	"runtime/trace"
	"sync"
)

// End markers the analyzer looks for to end the critical path.
const (
	FirstFrameMarker = "first-frame"
	ExitMarker       = "exit"
)

type recording struct {
	mu     sync.Mutex
	file   *os.File
	marker string
}

var rec recording

func (r *recording) start(env, marker string) {
	path := os.Getenv(env)
	if path == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file != nil {
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
	r.file, r.marker = f, marker
}

func (r *recording) stop(marker string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil || r.marker != marker {
		return
	}
	trace.Log(context.Background(), "crush", marker)
	trace.Stop()
	_ = r.file.Close()
	r.file = nil
}

// Start begins tracing startup when CRUSH_TRACE names a file.
func Start() { rec.start("CRUSH_TRACE", FirstFrameMarker) }

// FirstFrame ends a startup trace.
func FirstFrame() { rec.stop(FirstFrameMarker) }

// Quitting begins tracing shutdown when CRUSH_TRACE_SHUTDOWN names a file.
func Quitting() { rec.start("CRUSH_TRACE_SHUTDOWN", ExitMarker) }

// Exiting ends a shutdown trace.
func Exiting() { rec.stop(ExitMarker) }

// Phase runs fn inside a named trace region so the analyzer can split the
// critical path by phase.
func Phase(ctx context.Context, name string, fn func()) {
	trace.WithRegion(ctx, name, fn)
}
