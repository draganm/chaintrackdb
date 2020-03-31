package store_test

import (
	"context"
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
			tx, err := st.NewWriteTransaction(context.Background())
			require.NoError(t, err)

			defer tx.Rollback()

			bw, err := tx.AppendBlock(store.TypeBTreeNode, 0, 8)
			require.NoError(t, err)

			t.Run("it should return a block writer", func(t *testing.T) {
				require.Equal(t, 8, len(bw.Data))
			})

			t.Run("when I commit the transaction", func(t *testing.T) {
				newRootAddress, err := tx.Commit(bw.Address)
				require.NoError(t, err)
				require.NotEqual(t, store.NilAddress, newRootAddress)
			})
		})

	})

}

func TestOpeningExistingStore(t *testing.T) {

	td, cleanup := NewTempDir(t)
	defer cleanup()

	t.Run("when I open store in an empty dir", func(t *testing.T) {
		st, err := store.Open(td)
		require.NoError(t, err)

		t.Run("and I close and re-open the store", func(t *testing.T) {
			err = st.Close()
			require.NoError(t, err)

			st, err = store.Open(td)
			require.NoError(t, err)

			t.Run("when I append a new block to the store", func(t *testing.T) {
				tx, err := st.NewWriteTransaction(context.Background())
				require.NoError(t, err)

				bw, err := tx.AppendBlock(store.TypeBTreeNode, 0, 20)
				require.NoError(t, err)

				require.NoError(t, err)
				t.Run("it should return a block writer", func(t *testing.T) {
					require.Equal(t, 20, len(bw.Data))
				})
			})

		})

	})

}
