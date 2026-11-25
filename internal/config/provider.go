package config

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"charm.land/catwalk/v3/pkg/catwalk"
	"charm.land/catwalk/v3/pkg/embedded"
	"github.com/charmbracelet/crush/internal/agent/hyper"
	"github.com/charmbracelet/crush/internal/csync"
	"github.com/charmbracelet/x/etag"
)

type syncer[T any] interface {
	Get(context.Context) (T, error)
}

var (
	providerOnce sync.Once
	providerMu   sync.RWMutex // Guards providerList and providerErr.
	providerList []catwalk.Provider
	providerErr  error
)

// file to cache provider data. It resolves through GlobalConfigData so the
// catalog follows CRUSH_GLOBAL_DATA like the rest of the data directory.
func cachePathFor(name string) string {
	return filepath.Join(filepath.Dir(GlobalConfigData()), name+".json")
}

// UpdateProviders updates the Catwalk providers list from a specified source.
func UpdateProviders(pathOrURL string) error {
	var data []byte
	pathOrURL = cmp.Or(pathOrURL, os.Getenv("CATWALK_URL"), defaultCatwalkURL)

	switch {
	case pathOrURL == "embedded":
		var err error
		if data, err = json.Marshal(embedded.GetAll()); err != nil {
			return fmt.Errorf("failed to marshal embedded providers: %w", err)
		}
	case strings.HasPrefix(pathOrURL, "http://") || strings.HasPrefix(pathOrURL, "https://"):
		var err error
		_, data, err = newCatwalkClient(pathOrURL).GetProviders(context.Background(), "")
		if err != nil {
			return fmt.Errorf("failed to fetch providers from Catwalk: %w", err)
		}
	default:
		var err error
		if data, err = os.ReadFile(pathOrURL); err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		var providers []catwalk.Provider
		if err := json.Unmarshal(data, &providers); err != nil {
			return fmt.Errorf("failed to unmarshal provider data: %w", err)
		}
		if len(providers) == 0 {
			return fmt.Errorf("no providers found in the provided source")
		}
	}

	if err := newCache[[]catwalk.Provider](cachePathFor("providers")).StoreRaw(data); err != nil {
		return fmt.Errorf("failed to save providers to cache: %w", err)
	}

	slog.Info("Providers updated successfully", "from", pathOrURL, "to", cachePathFor)
	return nil
}

// ResolveHyperAPIKey returns the Hyper API key from the environment or
// the raw config value. The env var takes precedence.
func ResolveHyperAPIKey(cfg *Config) string {
	if key := os.Getenv("HYPER_API_KEY"); key != "" {
		return key
	}
	if cfg == nil || cfg.Providers == nil {
		return ""
	}
	pc, ok := cfg.Providers.Get("hyper")
	if !ok {
		return ""
	}
	return pc.APIKey
}

// HyperTokenRefresher is a function that refreshes the Hyper OAuth
// token. It is passed to Providers so the catalog fetch can retry on
// 401 without relying on package-global state.
type HyperTokenRefresher func(context.Context) error

// UpdateHyper updates the Hyper provider information from a specified URL.
func UpdateHyper(pathOrURL string) error {
	var data []byte
	pathOrURL = cmp.Or(pathOrURL, hyper.BaseURL())

	switch {
	case pathOrURL == "embedded":
		var err error
		if data, err = json.Marshal(hyper.Embedded()); err != nil {
			return fmt.Errorf("failed to marshal embedded Hyper provider: %w", err)
		}
	case strings.HasPrefix(pathOrURL, "http://") || strings.HasPrefix(pathOrURL, "https://"):
		client := realHyperClient{
			baseURL:    pathOrURL,
			resolveKey: func() string { return ResolveHyperAPIKey(nil) },
		}
		var err error
		if _, data, err = client.Get(context.Background(), ""); err != nil {
			return fmt.Errorf("failed to fetch provider from Hyper: %w", err)
		}
	default:
		var err error
		if data, err = os.ReadFile(pathOrURL); err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		var provider catwalk.Provider
		if err := json.Unmarshal(data, &provider); err != nil {
			return fmt.Errorf("failed to unmarshal provider data: %w", err)
		}
	}

	if err := newCache[catwalk.Provider](cachePathFor("hyper")).StoreRaw(data); err != nil {
		return fmt.Errorf("failed to save Hyper provider to cache: %w", err)
	}

	slog.Info("Hyper provider updated successfully", "from", pathOrURL, "to", cachePathFor("hyper"))
	return nil
}

var (
	catwalkSyncer = &catwalkSync{}
	hyperSyncer   = &hyperSync{}
)

// CatwalkUpdated reports whether the Catwalk provider catalog was
// refreshed with new data during this run, either by a cold start's inline
// fetch or by RefreshProviders. Callers use it to renew catalogs that
// shadow Catwalk's, such as the ChatGPT model catalog.
func CatwalkUpdated() bool {
	return catwalkSyncer.Updated()
}

// Providers returns the list of providers, taking into account cached results
// and whether or not auto update is enabled.
//
// It will:
// 1. if auto update is disabled, it'll return the embedded providers at the
// time of release.
// 2. return the cached providers, leaving RefreshProviders to revalidate
// them in the background.
// 3. with no cache, fetch the providers, and return either this new list or
// the embedded list if that fails.
//
// A returned error is advisory: it reports that the catalog could not be
// cached, or that an upstream returned nothing usable. It never means that no
// providers are available, so callers should surface it as a warning and keep
// using the returned list. A refresh that simply could not reach the network
// is not an error at all: the cached or embedded catalog is a sound answer, so
// those are logged and the fallback is returned.
func Providers(cfg *Config) ([]catwalk.Provider, error) {
	return LoadProviders(context.Background(), cfg, nil)
}

// LoadProviders is Providers with a context bounding the catalog fetch and a
// Hyper token refresher for retrying on 401. Only the first call in a process
// fetches; later calls return the memoized result and ignore both arguments.
func LoadProviders(ctx context.Context, cfg *Config, refreshHyper HyperTokenRefresher) ([]catwalk.Provider, error) {
	providerOnce.Do(func() {
		var wg sync.WaitGroup
		providers := csync.NewSlice[catwalk.Provider]()
		autoupdate := !cfg.Options.DisableProviderAutoUpdate
		customProvidersOnly := cfg.Options.DisableDefaultProviders

		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()

		// Each goroutine owns its own error so the two can report
		// independently without racing on a shared slice.
		var catwalkErr, hyperErr error
		var hyperProvider catwalk.Provider

		wg.Go(func() {
			if customProvidersOnly {
				return
			}
			catwalkURL := cmp.Or(os.Getenv("CATWALK_URL"), defaultCatwalkURL)
			client := newCatwalkClient(catwalkURL)
			path := cachePathFor("providers")
			catwalkSyncer.Init(client, path, autoupdate)

			// A failure to refresh or cache the catalog is worth
			// reporting, but the syncer still hands back the cached or
			// embedded list. Dropping that would leave the user with no
			// providers at all over a transient disk or network problem.
			items, err := catwalkSyncer.Get(ctx)
			if err != nil {
				catwalkURL := fmt.Sprintf("%s/v3/providers", cmp.Or(os.Getenv("CATWALK_URL"), defaultCatwalkURL))
				catwalkErr = fmt.Errorf("Crush was unable to fetch an updated list of providers from %s. Consider setting CRUSH_DISABLE_PROVIDER_AUTO_UPDATE=1 to use the embedded providers bundled at the time of this Crush release. You can also update providers manually. For more info see crush update-providers --help.\n\nCause: %w", catwalkURL, err) //nolint:staticcheck
			}
			providers.Append(items...)
		})

		wg.Go(func() {
			if customProvidersOnly {
				return
			}
			path := cachePathFor("hyper")
			cfgSnapshot := cfg
			hyperSyncer.Init(realHyperClient{
				baseURL:      hyper.BaseURL(),
				resolveKey:   func() string { return ResolveHyperAPIKey(cfgSnapshot) },
				refreshToken: refreshHyper,
			}, path, autoupdate)

			// As above: keep whatever provider we were handed. The syncer
			// already falls back to the cached or embedded copy, so an
			// error here means "could not refresh", not "no Hyper". This
			// matters more than for other providers because Hyper's
			// endpoint and model list live in the catalog rather than in
			// the user's config: dropping it signs a logged-in user out.
			item, err := hyperSyncer.Get(ctx)
			if err != nil {
				hyperErr = fmt.Errorf("Crush was unable to fetch updated information from Hyper: %w", err) //nolint:staticcheck
			}
			hyperProvider = item
		})

		wg.Wait()

		providerMu.Lock()
		providerList = joinProviders(hyperProvider, slices.Collect(providers.Seq()))
		providerErr = errors.Join(catwalkErr, hyperErr)
		providerMu.Unlock()
	})
	providerMu.RLock()
	defer providerMu.RUnlock()
	return providerList, providerErr
}

func joinProviders(hyperProvider catwalk.Provider, rest []catwalk.Provider) []catwalk.Provider {
	if hyperProvider.ID == "" {
		return rest
	}
	return append([]catwalk.Provider{hyperProvider}, rest...)
}

// RefreshProviders revalidates the Catwalk and Hyper catalogs that
// LoadProviders served from the cache, and reports whether either changed.
// Later calls to Providers return the refreshed list. Meant to run in the
// background after startup; it does nothing before LoadProviders or when
// the catalogs were fetched fresh.
func RefreshProviders(ctx context.Context) (bool, error) {
	var catwalkChanged, hyperChanged bool
	var catwalkErr, hyperErr error
	var wg sync.WaitGroup
	wg.Go(func() { catwalkChanged, catwalkErr = catwalkSyncer.Revalidate(ctx) })
	wg.Go(func() { hyperChanged, hyperErr = hyperSyncer.Revalidate(ctx) })
	wg.Wait()
	// An error here is advisory, as in LoadProviders: the syncers still
	// hold a usable catalog.
	err := errors.Join(catwalkErr, hyperErr)
	if !catwalkChanged && !hyperChanged {
		return false, err
	}

	var items []catwalk.Provider
	var hyperProvider catwalk.Provider
	if catwalkSyncer.init.Load() {
		items, _ = catwalkSyncer.Get(ctx)
	}
	if hyperSyncer.init.Load() {
		hyperProvider, _ = hyperSyncer.Get(ctx)
	}
	providerMu.Lock()
	providerList = joinProviders(hyperProvider, items)
	providerMu.Unlock()
	return true, err
}

// UpdateProviderInList replaces a provider in the memoized provider list
// returned by Providers(). This is used after re-fetching a single
// provider (e.g. Hyper after OAuth) so that all callers of Providers()
// see the updated entry without needing to reset sync.Once.
func UpdateProviderInList(provider catwalk.Provider) {
	providerMu.Lock()
	defer providerMu.Unlock()
	for i, p := range providerList {
		if p.ID == provider.ID {
			providerList[i] = provider
			return
		}
	}
	// Provider not found in list; prepend it.
	providerList = append([]catwalk.Provider{provider}, providerList...)
}

type cache[T any] struct {
	path string
}

func newCache[T any](path string) cache[T] {
	return cache[T]{path: path}
}

func (c cache[T]) Get() (T, string, error) {
	var v T
	data, err := readFile(c.path)
	if err != nil {
		return v, "", fmt.Errorf("failed to read provider cache file: %w", err)
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return v, "", fmt.Errorf("failed to unmarshal provider data from cache: %w", err)
	}

	return v, etag.Of(data), nil
}

func (c cache[T]) Store(v T) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal provider data: %w", err)
	}
	return c.StoreRaw(data)
}

// StoreRaw caches data exactly as given, so that Get derives the same ETag
// the server computed for it.
func (c cache[T]) StoreRaw(data []byte) error {
	slog.Info("Saving provider data to disk", "path", c.path)
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return fmt.Errorf("failed to create directory for provider cache: %w", err)
	}

	// Written through a temporary file and renamed into place. Several Crush
	// instances start independently and race to refresh this cache, and a
	// truncating write would let one of them read a half-written catalog and
	// silently fall back to the bundled copy.
	if err := atomicWriteFile(c.path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write provider data to cache: %w", err)
	}
	return nil
}
