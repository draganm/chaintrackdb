package chaintrackdb_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/draganm/chaintrackdb"
	"github.com/stretchr/testify/require"
)

func NewTempDir(t *testing.T) (string, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	return td, func() {
		os.RemoveAll(td)
	}
}

func NewEmptyDB(t *testing.T) (*chaintrackdb.DB, func()) {
	td, tempDirCleanup := NewTempDir(t)

	db, err := chaintrackdb.Open(td)
	require.NoError(t, err)

	return db, func() {
		err = db.Close()
		require.NoError(t, err)
		tempDirCleanup()
	}
}

func TestOpenAndCloseEmptyDatabase(t *testing.T) {
	td, cleanup := NewTempDir(t)
	defer cleanup()

	db, err := chaintrackdb.Open(td)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}

func TestCreatingEmptyMap(t *testing.T) {
	db, cleanup := NewEmptyDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
		return tx.CreateMap("abc")
	})
	require.NoError(t, err)

	err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
		return tx.CreateMap("def")
	})
	require.NoError(t, err)

	t.Run("creating a sub-map", func(t *testing.T) {
		err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
			return tx.CreateMap("abc/def")
		})
		require.NoError(t, err)

	})

}
