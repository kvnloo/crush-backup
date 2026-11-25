package honcho

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/charmbracelet/crush/internal/config"
	"github.com/stretchr/testify/require"
)

// TestFeaturesFollowsEveryInput guards the memo in Features: it must still
// change the moment any input to Resolve does, including files another
// process rewrites.
func TestFeaturesFollowsEveryInput(t *testing.T) {
	dir := isolateConfig(t)
	off := &config.Config{}
	on := []string{Feature}

	require.Empty(t, Features(off))
	require.Empty(t, Features(off), "a repeated question gets the same answer")

	require.NoError(t, SaveToken(tsToken()))
	require.Equal(t, on, Features(off), "signing in turns memory on")
	require.NoError(t, DeleteToken())
	require.Empty(t, Features(off), "signing out turns it off")

	writeShared(t, dir, `{"apiKey": "from-the-shared-file"}`)
	require.Equal(t, on, Features(off), "the shared file is read again when it changes")
	require.NoError(t, os.Remove(filepath.Join(dir, "config.json")))
	require.Empty(t, Features(off))

	t.Setenv("HONCHO_API_KEY", "from-the-environment")
	require.Equal(t, on, Features(off), "the environment is part of the key")
	t.Setenv("HONCHO_API_KEY", "")
	require.Empty(t, Features(off))

	require.Equal(t, on, Features(&config.Config{Honcho: &config.Honcho{Enabled: true}}), "Crush's config is part of the key")
	require.Empty(t, Features(off))
}
