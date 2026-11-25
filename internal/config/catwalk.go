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
	"charm.land/catwalk/v3/pkg/embedded"
	xetag "github.com/charmbracelet/x/etag"
)

type catwalkClient interface {
	// GetProviders returns the catalog and the response body it was decoded
	// from, or catwalk.ErrNotModified while etag still matches.
	GetProviders(ctx context.Context, etag string) ([]catwalk.Provider, []byte, error)
}

// httpCatwalkClient fetches the catalog like catwalk.Client, but also hands
// back the response body. The cache must keep those exact bytes: Catwalk's
// ETag is a hash of them, while a re-marshaled copy drops every field this
// build does not know about. Its hash then never matches, and every startup
// downloads the whole catalog again and reports it as updated.
type httpCatwalkClient struct {
	url    string
	client *http.Client
}

func newCatwalkClient(url string) httpCatwalkClient {
	return httpCatwalkClient{url: url, client: &http.Client{Timeout: 30 * time.Second}}
}

func (c httpCatwalkClient) GetProviders(ctx context.Context, etag string) ([]catwalk.Provider, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/v3/providers", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create request: %w", err)
	}
	xetag.Request(req, etag)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		return nil, nil, catwalk.ErrNotModified
	default:
		return nil, nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}
	var providers []catwalk.Provider
	if err := json.Unmarshal(body, &providers); err != nil {
		return nil, nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return providers, body, nil
}

var _ syncer[[]catwalk.Provider] = (*catwalkSync)(nil)

// catwalkSync serves the Catwalk catalog stale-while-revalidate: Get answers
// from the cache without touching the network, and Revalidate refreshes it
// later in the background. Only a cold start with no cache fetches inline.
type catwalkSync struct {
	once       sync.Once
	mu         sync.Mutex // Guards result and err after the once.
	result     []catwalk.Provider
	err        error
	stale      bool // result came from the cache and awaits Revalidate.
	cache      cache[[]catwalk.Provider]
	client     catwalkClient
	autoupdate bool
	init       atomic.Bool
	updated    atomic.Bool
}

func (s *catwalkSync) Init(client catwalkClient, path string, autoupdate bool) {
	s.client = client
	s.cache = newCache[[]catwalk.Provider](path)
	s.autoupdate = autoupdate
	s.init.Store(true)
}

func (s *catwalkSync) Get(ctx context.Context) ([]catwalk.Provider, error) {
	if !s.init.Load() {
		panic("called Get before Init")
	}

	// The result and the error are memoized together so that every caller
	// sees the same outcome, not just the one that won the once.
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		cached, _, cachedErr := s.cache.Get()
		hasCache := len(cached) > 0 && cachedErr == nil
		switch {
		case !s.autoupdate && hasCache:
			slog.Info("Using cached Catwalk providers (auto-update disabled)")
			s.result = cached
		case !s.autoupdate:
			slog.Info("Using embedded Catwalk providers")
			s.result = embedded.GetAll()
		case hasCache:
			slog.Info("Using cached Catwalk providers until revalidated")
			s.result, s.stale = cached, true
		default:
			s.result, _, s.err = s.fetch(ctx, embedded.GetAll(), "")
		}
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result, s.err
}

// Revalidate asks Catwalk whether the catalog Get served from the cache is
// still current, and reports whether it changed. It does nothing unless Get
// served a cached catalog, so it is safe to call unconditionally.
func (s *catwalkSync) Revalidate(ctx context.Context) (bool, error) {
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

// fetch asks Catwalk for the catalog, falling back to fallback when it is
// unchanged or unreachable. Caller must hold mu.
func (s *catwalkSync) fetch(ctx context.Context, fallback []catwalk.Provider, etag string) ([]catwalk.Provider, bool, error) {
	slog.Info("Fetching providers from Catwalk")
	result, raw, err := s.client.GetProviders(ctx, etag)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		slog.Warn("Catwalk providers not updated in time")
		return fallback, false, nil
	case errors.Is(err, catwalk.ErrNotModified):
		slog.Info("Catwalk providers not modified")
		return fallback, false, nil
	case err != nil:
		// Being offline is routine and the fallback is sound, so this is
		// logged rather than reported to the caller.
		slog.Warn("Could not fetch providers from Catwalk", "error", err)
		return fallback, false, nil
	case len(result) == 0:
		return fallback, false, errors.New("empty providers list from catwalk")
	}

	// The catalog is usable from here on. A cache write failure only costs
	// the next run a refresh, so it is reported alongside a valid result
	// rather than in place of one.
	s.updated.Store(true)
	return result, true, s.cache.StoreRaw(raw)
}

// Updated reports whether Get or Revalidate replaced the catalog with
// fresh data from Catwalk rather than falling back to the cache, the
// embedded copy, or a 304 Not Modified response. It stays false for the
// whole process when auto-update is disabled, which makes it the signal for
// refreshing catalogs that shadow Catwalk's, such as the ChatGPT model
// catalog.
func (s *catwalkSync) Updated() bool {
	return s.updated.Load()
}
