package honcho

import (
	"errors"
	"io/fs"
	"os"
	"sync"

	"github.com/charmbracelet/crush/internal/config"
)

// Feature is the name a skill puts in its `requires:` field to be
// hidden unless Honcho memory is switched on.
const Feature = "honcho"

// FromCrushConfig maps Crush's `honcho` config block onto the
// overrides Resolve consumes.
//
// A nil block yields the zero value, which defers entirely to the
// shared cross-harness config file and the environment.
func FromCrushConfig(h *config.Honcho) Config {
	if h == nil {
		return Config{}
	}
	out := Config{
		Enabled:         h.Enabled,
		APIKey:          h.APIKey,
		BaseURL:         h.BaseURL,
		Workspace:       h.Workspace,
		PeerName:        h.PeerName,
		AgentPeer:       h.AgentPeer,
		RecallMode:      RecallMode(h.RecallMode),
		ObservationMode: ObservationMode(h.ObservationMode),
		SessionStrategy: SessionStrategy(h.SessionStrategy),
		AgentObserveMe:  h.AgentObserveMe,
		MaxConclusions:  h.MaxConclusions,
		ContextTokens:   h.ContextTokens,
	}
	// Tool capture defaults to on, so an omitted field means "use the
	// default" and only an explicit false turns it off.
	out.CaptureTools = h.CaptureTools == nil || *h.CaptureTools
	return out
}

// Features returns the feature names to expose to skill discovery, so
// memory-specific skills stay out of the system prompt when memory is
// off.
//
// The TUI asks on every frame, so the answer is memoized. It depends on
// Crush's config, the environment, and two small files another process may
// rewrite, such as `crush login honcho` in another terminal; the key covers
// all of them, so checking it costs two stats rather than reading and
// parsing both files.
func Features(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}
	if !enabled(FromCrushConfig(cfg.Honcho)) {
		return nil
	}
	return []string{Feature}
}

// resolveEnv lists every variable Resolve and the file paths read.
var resolveEnv = [...]string{
	"HOME", "HONCHO_CONFIG_DIR", "HONCHO_API_KEY", "HONCHO_URL", "HONCHO_BASE_URL",
	"HONCHO_WORKSPACE", "HONCHO_WORKSPACE_ID", "HONCHO_PEER_NAME", "HONCHO_AI_PEER",
}

type enabledKey struct {
	overrides     Config
	env           [len(resolveEnv)]string
	shared, token fileStamp
}

// fileStamp identifies one version of a file, or its absence.
type fileStamp struct {
	path    string
	exists  bool
	size    int64
	modTime int64
}

var enabledCache struct {
	mu      sync.Mutex
	valid   bool
	key     enabledKey
	enabled bool
}

// enabled reports Resolve(overrides).Enabled, memoized on its inputs.
func enabled(overrides Config) bool {
	key, ok := enabledKeyFor(overrides)
	if !ok {
		return Resolve(overrides).Enabled
	}
	enabledCache.mu.Lock()
	defer enabledCache.mu.Unlock()
	if !enabledCache.valid || enabledCache.key != key {
		// Resolve may read a file newer than the stamp; the next call
		// then stamps it differently and resolves again.
		enabledCache.key = key
		enabledCache.enabled = Resolve(overrides).Enabled
		enabledCache.valid = true
	}
	return enabledCache.enabled
}

// enabledKeyFor builds the cache key, or reports false when a file cannot
// be stamped and the answer must be resolved afresh.
func enabledKeyFor(overrides Config) (enabledKey, bool) {
	key := enabledKey{overrides: overrides}
	for i, name := range resolveEnv {
		key.env[i] = os.Getenv(name)
	}
	var ok1, ok2 bool
	sharedPath, err := SharedConfigPath()
	key.shared, ok1 = stamp(sharedPath, err)
	tokenPath, err := TokenPath()
	key.token, ok2 = stamp(tokenPath, err)
	return key, ok1 && ok2
}

func stamp(path string, pathErr error) (fileStamp, bool) {
	if pathErr != nil {
		return fileStamp{}, false
	}
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fileStamp{path: path}, true
	case err != nil:
		return fileStamp{}, false
	}
	return fileStamp{path: path, exists: true, size: info.Size(), modTime: info.ModTime().UnixNano()}, true
}
