package event

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/crush/internal/version"
	"github.com/posthog/posthog-go"
)

const (
	endpoint = "https://data.charm.land"
	key      = "phc_4zt4VgDWLqbYnJYEwLRxFoaTL2noNrQij0C6E8k3I0V"

	nonInteractiveAttrName       = "NonInteractive"
	nonInteractiveNestedAttrName = "NonInteractiveNested"
	continueSessionByIDAttrName  = "ContinueSessionByID"
	continueLastSessionAttrName  = "ContinueLastSession"
)

var (
	client posthog.Client

	baseProps = posthog.NewProperties().
			Set("GOOS", runtime.GOOS).
			Set("GOARCH", runtime.GOARCH).
			Set("TERM", os.Getenv("TERM")).
			Set("SHELL", filepath.Base(os.Getenv("SHELL"))).
			Set("Version", version.Version).
			Set("GoVersion", runtime.Version()).
			Set(nonInteractiveAttrName, false).
			Set(nonInteractiveNestedAttrName, false)
)

func SetNonInteractive(nonInteractive bool) {
	baseProps = baseProps.
		Set(nonInteractiveAttrName, nonInteractive).
		Set(nonInteractiveNestedAttrName, nonInteractive && os.Getenv("CRUSH") == "1")
}

func SetContinueBySessionID(continueBySessionID bool) {
	baseProps = baseProps.Set(continueSessionByIDAttrName, continueBySessionID)
}

func SetContinueLastSession(continueLastSession bool) {
	baseProps = baseProps.Set(continueLastSessionAttrName, continueLastSession)
}

// Setting up the client is slow: the machine ID comes from running ioreg,
// and PostHog probes the OS version with sw_vers. Init therefore does it in
// the background, and events sent in the meantime wait in pending until the
// client is ready.
var (
	started atomic.Bool
	ready   = make(chan struct{})
	mu      sync.Mutex
	isReady bool
	pending []func()
)

// Init starts the PostHog client in the background. Only the first call has
// any effect.
func Init() {
	if !started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		c, err := posthog.NewWithConfig(key, posthog.Config{
			Endpoint:        endpoint,
			Logger:          logger{},
			ShutdownTimeout: 500 * time.Millisecond,
		})
		if err != nil {
			slog.Error("Failed to initialize PostHog client", "error", err)
		}
		id := getDistinctId()

		mu.Lock()
		defer mu.Unlock()
		client, distinctId = c, id
		isReady = true
		close(ready)
		for _, f := range pending {
			f()
		}
		pending = nil
	}()
}

// whenReady runs f once the client is ready, right away if it already is.
// Without Init it drops f, as metrics are disabled.
func whenReady(f func()) {
	if !started.Load() {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if isReady {
		f()
		return
	}
	pending = append(pending, f)
}

// waitReady blocks until Init has finished, and reports whether it ran.
func waitReady() bool {
	if !started.Load() {
		return false
	}
	<-ready
	return true
}

// GetID returns the machine's anonymous ID, waiting for Init to compute it.
func GetID() string {
	waitReady()
	return distinctId
}

// send logs an event to PostHog with the given event name and properties.
func send(event string, props ...any) {
	properties := pairsToProps(props...).Merge(baseProps)
	whenReady(func() {
		if client == nil {
			return
		}
		err := client.Enqueue(posthog.Capture{
			DistinctId: distinctId,
			Event:      event,
			Properties: properties,
		})
		if err != nil {
			slog.Error("Failed to enqueue PostHog event", "event", event, "props", props, "error", err)
		}
	})
}

// Error logs an error event to PostHog with the error type and message.
func Error(errToLog any, props ...any) {
	if errToLog == nil {
		return
	}
	at := time.Now()
	errType, msg := reflect.TypeOf(errToLog).String(), fmt.Sprintf("%v", errToLog)
	properties := pairsToProps(props...)
	whenReady(func() {
		if client == nil || distinctId == "" {
			return
		}
		exception := posthog.NewDefaultException(at, distinctId, errType, msg)
		if exception.Properties == nil {
			exception.Properties = posthog.NewProperties()
		}
		exception.Properties = exception.Properties.Merge(properties)
		if posthogErr := client.Enqueue(exception); posthogErr != nil {
			slog.Error("Failed to enqueue PostHog error", "err", errToLog, "props", props, "posthogErr", posthogErr)
		}
	})
}

// Flush sends every queued event, waiting for Init to finish first.
func Flush() {
	if !waitReady() || client == nil {
		return
	}
	if err := client.Close(); err != nil {
		slog.Error("Failed to flush PostHog events", "error", err)
	}
}

func pairsToProps(props ...any) posthog.Properties {
	p := posthog.NewProperties()

	if !isEven(len(props)) {
		slog.Error("Event properties must be provided as key-value pairs", "props", props)
		return p
	}

	for i := 0; i < len(props); i += 2 {
		key, ok := props[i].(string)
		if !ok {
			slog.Error("Event property key must be a string", "key", props[i], "index", i)
			continue
		}
		value := props[i+1]
		p = p.Set(key, value)
	}
	return p
}

func isEven(n int) bool {
	return n%2 == 0
}
