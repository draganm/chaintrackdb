package store_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/chaintrackdb/store"
	"github.com/stretchr/testify/require"
)

func NewTempDir(t *testing.T) (string, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	return td, func() {
		os.RemoveAll(td)
	}
}

func TestSegment(t *testing.T) {
	td, cleanup := NewTempDir(t)

	defer cleanup()

	t.Run("when I create a new segment", func(t *testing.T) {
		seg, err := store.CreateSegment(filepath.Join(td, "seg1"), 1024*1024, 1)
		require.NoError(t, err)

		t.Run("when I append a new block to segment", func(t *testing.T) {
			addr, _, err := seg.AppendBlock(255)
			require.NoError(t, err)
			t.Run("then the address of the first block should be 1", func(t *testing.T) {
				require.Equal(t, store.Address(1), addr)
			})
		})
	})
}
