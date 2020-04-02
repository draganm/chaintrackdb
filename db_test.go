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

	t.Run("when I create an empty map", func(t *testing.T) {
		err := db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
			return tx.CreateMap("abc")
		})
		require.NoError(t, err)
		t.Run("then the map should exist", func(t *testing.T) {
			var exists bool
			err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
				exists, err = tx.Exists("abc")
				return err
			})
			require.NoError(t, err)
			require.True(t, exists)

		})

		t.Run("then the count of the parent map should be 1", func(t *testing.T) {
			var count uint64
			err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
				count, err = tx.Count("/")
				return err
			})
			require.NoError(t, err)
			require.Equal(t, uint64(1), count)
		})
	})

}

func TestCreatingEmptySubMap(t *testing.T) {
	db, cleanup := NewEmptyDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("when I create an empty map", func(t *testing.T) {
		err := db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
			return tx.CreateMap("abc")
		})
		require.NoError(t, err)

		t.Run("and I create a sub-map", func(t *testing.T) {
			err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
				return tx.CreateMap("abc/def")
			})
			require.NoError(t, err)

			t.Run("then the sub-map should exist", func(t *testing.T) {
				var exists bool
				db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
					exists, err = tx.Exists("abc/def")
					return err
				})
				require.True(t, exists)

			})

		})
	})

}

func TestPuttingDataToRoot(t *testing.T) {
	db, cleanup := NewEmptyDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("when I put data to root", func(t *testing.T) {
		err := db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
			return tx.Put("abc", []byte{1, 2, 3})
		})
		require.NoError(t, err)
		t.Run("and when I get the data in a separate transaction", func(t *testing.T) {
			var d []byte
			err = db.WriteTransaction(ctx, func(tx *chaintrackdb.WriteTransaction) error {
				d, err = tx.Get("abc")
				return err
			})
			require.NoError(t, err)
			require.Equal(t, []byte{1, 2, 3}, d)

		})
	})

}
