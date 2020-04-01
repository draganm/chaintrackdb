package btree

import (
	"context"
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

func NewWriteTransaction(t *testing.T) (*store.WriteTransaction, func() error) {
	td, cleanup := createTempDir(t)

	st, err := store.Open(td)
	require.NoError(t, err)

	wtx, _, err := st.NewWriteTransaction(context.Background())
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

func TestPersistingAndLoadingLeaf(t *testing.T) {
	ts, cleanup := NewWriteTransaction(t)
	defer cleanup()

	v1, err := data.StoreData(ts, []byte{3, 3, 3}, 256, 4)
	require.NoError(t, err)

	v2, err := data.StoreData(ts, []byte{3, 3, 4}, 256, 4)
	require.NoError(t, err)

	n := &node{
		Count: 2,
		m:     1,
		KVS: []keyValue{
			{Key: []byte{1, 2, 3}, Value: v1},
			{Key: []byte{1, 2, 4}, Value: v2},
		},
		reader:  ts,
		writer:  ts,
		address: store.NilAddress,
	}

	t.Run("when I store the node", func(t *testing.T) {
		addr, err := n.persist()
		require.NoError(t, err)

		require.Equal(t, addr, n.address)

		t.Run("when I load the node", func(t *testing.T) {
			err = n.load()
			require.NoError(t, err)

			t.Run("then the node should be the same", func(t *testing.T) {
				requireJSONEqual(
					t,
					`
					  {
						"Count": 2,
						"KVS": [
						  "[1 2 3]: 29",
						  "[1 2 4]: 52"
						]
					  }`,
					n.toJSON(),
				)
			})
		})
	})
}

func TestPersistingAndLoadingNode(t *testing.T) {
	ts, cleanup := NewWriteTransaction(t)
	defer cleanup()

	v1, err := data.StoreData(ts, []byte{3, 3, 3}, 256, 4)
	require.NoError(t, err)

	v2, err := data.StoreData(ts, []byte{3, 3, 4}, 256, 4)
	require.NoError(t, err)

	v3, err := data.StoreData(ts, []byte{3, 3, 5}, 256, 4)
	require.NoError(t, err)

	n := &node{
		Count: 3,
		m:     1,
		KVS: []keyValue{
			{Key: []byte{1, 2, 4}, Value: v2},
		},
		reader:  ts,
		writer:  ts,
		address: store.NilAddress,
		Children: []*node{
			{
				Count: 1,
				m:     1,
				KVS: []keyValue{
					{Key: []byte{1, 2, 3}, Value: v1},
				},
				reader:  ts,
				writer:  ts,
				address: store.NilAddress,
			},
			{
				Count: 1,
				m:     1,
				KVS: []keyValue{
					{Key: []byte{1, 2, 5}, Value: v3},
				},
				reader:  ts,
				writer:  ts,
				address: store.NilAddress,
			},
		},
	}

	t.Run("when I store the node", func(t *testing.T) {
		addr, err := n.persist()
		require.NoError(t, err)

		require.Equal(t, addr, n.address)

		t.Run("when I load the node", func(t *testing.T) {
			err = n.load()
			require.NoError(t, err)

			for _, c := range n.Children {
				err = c.load()
				require.NoError(t, err)
			}

			t.Run("then the node should be the same", func(t *testing.T) {
				requireJSONEqual(
					t,
					`
					  {
						"Count": 3,
						"KVS": [
						  "[1 2 4]: 52"
						],
						"Children": [
						  {
							"Count": 1,
							"KVS": [
							  "[1 2 3]: 29"
							]
						  },
						  {
							"Count": 1,
							"KVS": [
							  "[1 2 5]: 75"
							]
						  }
						]
					  }`,
					n.toJSON(),
				)

				require.Equal(t, store.NilAddress, n.address)
				for _, c := range n.Children {
					require.Equal(t, store.NilAddress, c.address)
				}
			})
		})
	})
}
