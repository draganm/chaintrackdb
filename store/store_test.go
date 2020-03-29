package store_test

import (
	"io/ioutil"
	"os"
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

func TestCreatingNewStore(t *testing.T) {

	td, cleanup := NewTempDir(t)
	defer cleanup()

	t.Run("when I open store in an empty dir", func(t *testing.T) {
		st, err := store.Open(td)
		require.NoError(t, err)

		t.Run("when I append a new block to the store", func(t *testing.T) {
			bw, err := st.AppendBlock(store.TypeBTreeNode, 0, 20)
			require.NoError(t, err)
			t.Run("it should return a block writer", func(t *testing.T) {
				require.Equal(t, 20, len(bw.Data))
			})
		})

	})

}
