package config

import (
	"cmp"
	"context"
	"log/slog"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"time"

	"charm.land/catwalk/v3/pkg/catwalk"
	"github.com/charmbracelet/crush/internal/discover"
)

// discoveryProbeTimeout bounds the inline probe of a provider that has no
// cached models yet.
const discoveryProbeTimeout = 3 * time.Second

// discoveryJob is one custom provider whose models come from its /models
// endpoint.
type discoveryJob struct {
	id           string
	cfg          discover.Config
	providerType string
}

// discoveredModels is the on-disk cache entry for one provider. BaseURL is
// the unresolved config value, so pointing a provider elsewhere invalidates
// its entry.
type discoveredModels struct {
	BaseURL string          `json:"base_url"`
	Models  []catwalk.Model `json:"models"`
}

// discoveryJobs returns the custom providers that need model discovery:
// enabled, with a base URL, and either opted in through discover_models or
// lacking an explicit models list.
func discoveryJobs(c *Config, known map[string]bool) []discoveryJob {
	var jobs []discoveryJob
	for id, pc := range c.Providers.Seq2() {
		if known[id] || pc.Disable || pc.BaseURL == "" {
			continue
		}
		wanted := len(pc.Models) == 0
		if pc.AutoDiscoverModels != nil {
			wanted = *pc.AutoDiscoverModels
		}
		if !wanted {
			continue
		}
		jobs = append(jobs, discoveryJob{
			id: id,
			cfg: discover.Config{
				ID:           cmp.Or(pc.ID, id),
				BaseURL:      pc.BaseURL,
				APIKey:       pc.APIKey,
				ExtraHeaders: pc.ExtraHeaders,
			},
			providerType: string(cmp.Or(pc.Type, catwalk.TypeCompletions)),
		})
	}
	return jobs
}

// resolveDiscoveredModels returns the discovered models for each job. Jobs
// with a cache entry use it without touching the network and are recorded
// so RefreshDiscoveredModels can update them later; the rest are probed
// inline, bounded by discoveryProbeTimeout and ctx.
//
// A probe that finds nothing is cached as an empty entry. The provider has
// no models either way, so the next startup does not wait on it again and
// leaves it to the background refresh instead. Without this, an unreachable
// host costs every startup the whole probe timeout.
func (s *ConfigStore) resolveDiscoveredModels(ctx context.Context, jobs []discoveryJob, resolver VariableResolver) map[string][]catwalk.Model {
	found := make(map[string][]catwalk.Model, len(jobs))
	var cached, misses []discoveryJob
	for _, job := range jobs {
		if models, ok := s.cachedModels(job); ok {
			found[job.id] = models
			cached = append(cached, job)
			continue
		}
		misses = append(misses, job)
	}
	s.cachedDiscovery = cached

	probeCtx, cancel := context.WithTimeout(ctx, discoveryProbeTimeout)
	defer cancel()
	probed := s.probeAndCache(probeCtx, misses, resolver)
	for _, job := range misses {
		models, ok := probed[job.id]
		if !ok {
			s.cacheModels(job, nil)
			continue
		}
		found[job.id] = models
	}
	return found
}

// RefreshDiscoveredModels re-probes every provider whose models came from
// the discovery cache, and reloads the config when any of them changed. It
// reports whether a reload happened. Meant to run in the background after
// startup.
func (s *ConfigStore) RefreshDiscoveredModels(ctx context.Context) (bool, error) {
	if !s.refreshDiscovered(ctx) {
		return false, nil
	}
	return true, s.ReloadFromDisk(ctx)
}

// refreshDiscovered re-probes the providers whose models came from the
// discovery cache and reports whether any of them changed, without
// reloading.
func (s *ConfigStore) refreshDiscovered(ctx context.Context) bool {
	s.writeMu.RLock()
	jobs := s.cachedDiscovery
	resolver := s.resolver
	s.writeMu.RUnlock()

	if len(jobs) == 0 {
		return false
	}

	before := make(map[string][]catwalk.Model, len(jobs))
	for _, job := range jobs {
		before[job.id], _ = s.cachedModels(job)
	}
	changed := false
	for id, fresh := range s.probeAndCache(ctx, jobs, resolver) {
		changed = changed || !reflect.DeepEqual(before[id], fresh)
	}
	return changed
}

// catalogRefreshTimeout bounds the network work of RefreshCatalogs.
const catalogRefreshTimeout = 30 * time.Second

// RefreshCatalogs brings every catalog startup served from a cache up to
// date: Catwalk and Hyper, the ChatGPT and Grok catalogs that shadow
// Catwalk's, and the models of custom providers. They are fetched
// concurrently, and the config is reloaded once if any of them changed.
// It reports whether a reload happened. Meant to run in the background
// after startup.
func (s *ConfigStore) RefreshCatalogs(ctx context.Context) (bool, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, catalogRefreshTimeout)
	defer cancel()

	var providersChanged, modelsChanged bool
	var wg sync.WaitGroup
	wg.Go(func() {
		changed, err := RefreshProviders(fetchCtx)
		if err != nil {
			slog.Warn("Continuing with the previously known providers", "error", err)
		}
		// When Catwalk refreshed its catalog this run, give the model
		// catalogs that shadow Catwalk's the same treatment: the ChatGPT and
		// Grok catalogs are otherwise only fetched at login and would freeze
		// there while Catwalk keeps moving. Best effort; a failed fetch
		// keeps the catalog loaded from config.
		if CatwalkUpdated() {
			s.refetchOpenAIModels(fetchCtx, ScopeGlobal)
			s.refetchGrokModels(fetchCtx, ScopeGlobal)
			changed = true
		}
		providersChanged = changed
	})
	wg.Go(func() { modelsChanged = s.refreshDiscovered(fetchCtx) })
	wg.Wait()

	if !providersChanged && !modelsChanged {
		return false, nil
	}
	return true, s.ReloadFromDisk(ctx)
}

// probeAndCache discovers models for jobs concurrently and caches every
// non-empty result. Failures are logged and left out of the returned map.
func (s *ConfigStore) probeAndCache(ctx context.Context, jobs []discoveryJob, resolver VariableResolver) map[string][]catwalk.Model {
	found := make(map[string][]catwalk.Model, len(jobs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Go(func() {
			models, err := discover.DiscoverModels(ctx, job.cfg, resolver)
			if err != nil {
				slog.Warn("Model discovery failed", "provider", job.id, "error", err)
				return
			}
			if len(models) == 0 {
				return
			}
			if enricher := discover.GetEnricher(job.providerType); enricher != nil {
				models, _ = enricher.EnrichModels(ctx, job.cfg, resolver, models)
			}
			slog.Info("Discovered models for provider", "provider", job.id, "count", len(models))
			s.cacheModels(job, models)
			mu.Lock()
			found[job.id] = models
			mu.Unlock()
		})
	}
	wg.Wait()
	return found
}

// discoveryCachePath is empty when the store has no data directory, which
// disables the cache.
func (s *ConfigStore) discoveryCachePath(providerID string) string {
	if s.globalDataPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(s.globalDataPath), "discovered_models", url.PathEscape(providerID)+".json")
}

func (s *ConfigStore) cachedModels(job discoveryJob) ([]catwalk.Model, bool) {
	path := s.discoveryCachePath(job.id)
	if path == "" {
		return nil, false
	}
	entry, _, err := newCache[discoveredModels](path).Get()
	if err != nil || entry.BaseURL != job.cfg.BaseURL {
		return nil, false
	}
	return entry.Models, true
}

func (s *ConfigStore) cacheModels(job discoveryJob, models []catwalk.Model) {
	path := s.discoveryCachePath(job.id)
	if path == "" {
		return
	}
	entry := discoveredModels{BaseURL: job.cfg.BaseURL, Models: models}
	if err := newCache[discoveredModels](path).Store(entry); err != nil {
		slog.Warn("Failed to cache discovered models", "provider", job.id, "error", err)
	}
}

// mergeModels appends the discovered models whose IDs the config does not
// already list; models from the config win.
func mergeModels(configured, discovered []catwalk.Model) []catwalk.Model {
	listed := make(map[string]bool, len(configured))
	for _, m := range configured {
		listed[m.ID] = true
	}
	merged := slices.Clone(configured)
	for _, m := range discovered {
		if !listed[m.ID] {
			merged = append(merged, m)
		}
	}
	return merged
}
