package chaintrackdb_test

import (
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

func TestOpenEmptyDatabase(t *testing.T) {
	td, cleanup := NewTempDir(t)
	defer cleanup()

	db, err := chaintrackdb.Open(td)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)
}
