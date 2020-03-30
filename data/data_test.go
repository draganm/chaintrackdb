package data_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/draganm/chaintrackdb/data"
	"github.com/draganm/chaintrackdb/store"
	"github.com/stretchr/testify/require"
)

func createTempDir(t *testing.T) (string, func() error) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return td, func() error {
		return os.RemoveAll(td)
	}
}

func newWriteTransaction(t *testing.T) (store.WriteTransaction, func() error) {
	td, cleanup := createTempDir(t)

	st, err := store.Open(td)
	require.NoError(t, err)

	wtx, err := st.NewWriteTransaction(context.Background())
	require.NoError(t, err)

	return wtx, func() error {

		err = wtx.Rollback()
		if err != nil {
			return err
		}

		err = st.Close()
		if err != nil {
			return err
		}
		return cleanup()
	}
}

func TestStore(t *testing.T) {
	t.Run("data has same length as max segment size", func(t *testing.T) {
		st, cleanup := newWriteTransaction(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		sr, err := st.GetBlock(k)
		require.NoError(t, err)
		require.True(t, sr.NumberOfChildren() == 0, "segment should not have children")
		require.Equal(t, []byte{1, 2, 3}, sr.GetData())

	})

	t.Run("data is one byte longer than max segment size", func(t *testing.T) {
		st, cleanup := newWriteTransaction(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		require.NoError(t, err)

		sr, err := st.GetBlock(k)
		require.NoError(t, err)

		require.Equal(t, store.TypeDataNode, sr.Type(), "should be a data node segment")

		count := binary.BigEndian.Uint64(sr.GetData())
		require.NoError(t, err)

		require.Equal(t, uint64(4), count, "data node should record total size of 4")

		require.Equal(t, 2, sr.NumberOfChildren(), "should have two children")

		t.Run("first child should have first 3 bytes", func(t *testing.T) {

			cr, err := st.GetBlock(sr.GetChildAddress(0))
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3}, cr.GetData())

		})

		t.Run("second child should have last byte", func(t *testing.T) {
			cr, err := st.GetBlock(sr.GetChildAddress(1))
			require.NoError(t, err)

			require.Equal(t, []byte{4}, cr.GetData())

		})

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("data size requires two levels of indirection", func(t *testing.T) {
		st, cleanup := newWriteTransaction(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 1, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		sr, err := st.GetBlock(k)
		require.NoError(t, err)

		require.Equal(t, store.TypeDataNode, sr.Type(), "should be a data node segment")

		size := binary.BigEndian.Uint64(sr.GetData())
		require.Equal(t, uint64(4), size, "data node should record total size of 4")

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("reading and writing empty data", func(t *testing.T) {
		st, cleanup := newWriteTransaction(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 5, 2)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, 0, len(d))

		})

	})

	t.Run("reading and writing large amount of data", func(t *testing.T) {
		st, cleanup := newWriteTransaction(t)
		defer cleanup()

		dw := data.NewDataWriter(st, 5, 2)

		dataSize := 8193

		randomData := make([]byte, dataSize)

		n, err := rand.Read(randomData)
		require.NoError(t, err)
		require.Equal(t, dataSize, n)

		n, err = dw.Write(randomData)

		require.Equal(t, dataSize, n)

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, dataSize, len(d))

			require.Equal(t, randomData, d)
		})

	})

}
