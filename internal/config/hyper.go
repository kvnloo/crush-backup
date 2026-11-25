package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"charm.land/catwalk/v3/pkg/catwalk"
	"github.com/charmbracelet/crush/internal/agent/hyper"
	xetag "github.com/charmbracelet/x/etag"
)

type hyperClient interface {
	// Get returns the provider and the response body it was decoded from,
	// which the cache keeps verbatim so its ETag matches the server's. See
	// httpCatwalkClient for why.
	Get(ctx context.Context, etag string) (catwalk.Provider, []byte, error)
}

var _ syncer[catwalk.Provider] = (*hyperSync)(nil)

// hyperSync serves the Hyper provider stale-while-revalidate, like
// catwalkSync.
type hyperSync struct {
	once       sync.Once
	mu         sync.Mutex // Guards result, err and stale after the once.
	result     catwalk.Provider
	err        error
	stale      bool
	cache      cache[catwalk.Provider]
	client     hyperClient
	autoupdate bool
	init       atomic.Bool
}

func (s *hyperSync) Init(client hyperClient, path string, autoupdate bool) {
	s.client = client
	s.cache = newCache[catwalk.Provider](path)
	s.autoupdate = autoupdate
	s.init.Store(true)
}

// SetClient replaces the HTTP client used for fetching. This is used
// before Refetch to ensure the latest credentials are used.
func (s *hyperSync) SetClient(client hyperClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
}

func (s *hyperSync) Get(ctx context.Context) (catwalk.Provider, error) {
	if !s.init.Load() {
		panic("called Get before Init")
	}

	// The result and the error are memoized together so that every caller
	// sees the same outcome, not just the one that won the once.
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.autoupdate {
			slog.Info("Using embedded Hyper provider")
			s.result = hyper.Embedded()
			return
		}
		if cached, _, err := s.cache.Get(); err == nil && cached.ID != "" {
			slog.Info("Using cached Hyper provider until revalidated")
			s.result, s.stale = cached, true
			return
		}
		s.result, _, s.err = s.fetch(ctx, hyper.Embedded(), "")
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result, s.err
}

// Refetch fetches the Hyper provider now, ignoring the cache. Used after
// signing in, when the catalog may differ for the new account.
func (s *hyperSync) Refetch(ctx context.Context) (catwalk.Provider, error) {
	if !s.init.Load() {
		panic("called Refetch before Init")
	}
	s.once.Do(func() {}) // A later Get must not replace this result.
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.autoupdate {
		s.result, s.err = hyper.Embedded(), nil
		return s.result, nil
	}
	s.stale = false
	fallback := s.result
	if fallback.ID == "" {
		fallback = hyper.Embedded()
	}
	_, etag, _ := s.cache.Get()
	s.result, _, s.err = s.fetch(ctx, fallback, etag)
	return s.result, s.err
}

// Revalidate asks Hyper whether the provider Get served from the cache is
// still current, and reports whether it changed. It does nothing unless Get
// served a cached provider.
func (s *hyperSync) Revalidate(ctx context.Context) (bool, error) {
	if !s.init.Load() {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stale {
		return false, nil
	}
	s.stale = false
	_, etag, _ := s.cache.Get()
	result, changed, err := s.fetch(ctx, s.result, etag)
	s.result = result
	return changed, err
}

// fetch asks Hyper for the provider, falling back to fallback when it is
// unchanged or unreachable. Caller must hold mu.
func (s *hyperSync) fetch(ctx context.Context, fallback catwalk.Provider, etag string) (catwalk.Provider, bool, error) {
	slog.Info("Fetching Hyper provider")
	result, raw, err := s.client.Get(ctx, etag)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		slog.Warn("Hyper provider not updated in time")
		return fallback, false, nil
	case errors.Is(err, catwalk.ErrNotModified):
		slog.Info("Hyper provider not modified")
		return fallback, false, nil
	case err != nil:
		slog.Warn("Could not fetch the Hyper provider", "error", err)
		return fallback, false, nil
	case len(result.Models) == 0:
		slog.Warn("Hyper did not return any models")
		return fallback, false, nil
	}

	// The provider is usable from here on. A cache write failure only costs
	// the next run a refresh, so it is reported alongside a valid result
	// rather than in place of one.
	return result, true, s.cache.StoreRaw(raw)
}

var _ hyperClient = realHyperClient{}

type realHyperClient struct {
	baseURL      string
	resolveKey   func() string
	refreshToken func(context.Context) error
}

// Get implements hyperClient.
func (r realHyperClient) Get(ctx context.Context, etag string) (catwalk.Provider, []byte, error) {
	result, raw, err := r.doGet(ctx, etag)
	if err != nil && isHTTPUnauthorized(err) && r.refreshToken != nil {
		slog.Info("Received 401 fetching Hyper provider, refreshing token and retrying")
		if refreshErr := r.refreshToken(ctx); refreshErr != nil {
			slog.Warn("Failed to refresh Hyper token", "error", refreshErr)
			return result, raw, err
		}
		result, raw, err = r.doGet(ctx, "")
	}
	return result, raw, err
}

func (r realHyperClient) doGet(ctx context.Context, etag string) (catwalk.Provider, []byte, error) {
	var result catwalk.Provider
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		r.baseURL+"/api/v1/provider/v3",
		nil,
	)
	if err != nil {
		return result, nil, fmt.Errorf("could not create request: %w", err)
	}
	xetag.Request(req, etag)
	if apiKey := r.resolveKey(); apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return result, nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		return result, nil, catwalk.ErrNotModified
	case http.StatusUnauthorized:
		return result, nil, errUnauthorized
	default:
		return result, nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, nil, fmt.Errorf("failed to read response: %w", err)
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return result, body, nil
}

// errUnauthorized is a sentinel for HTTP 401 responses from the Hyper API.
var errUnauthorized = errors.New("unauthorized")

// isHTTPUnauthorized reports whether err is or wraps errUnauthorized.
func isHTTPUnauthorized(err error) bool {
	return errors.Is(err, errUnauthorized)
}
