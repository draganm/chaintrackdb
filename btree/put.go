package btree

import "github.com/draganm/chaintrackdb/store"

// M limits the maximal number of keys in a btree node.
// Max number of nodes is M*2+1.
const M = 1

// Put creates a new BTree containing the given key/value
func Put(rw store.ReaderWriter, root store.Address, key []byte, value store.Address) (store.Address, error) {
	n := &node{
		m:       M,
		address: root,
		reader:  rw,
		writer:  rw,
	}

	rn, err := insertIntoBtree(n, keyValue{key, value})
	if err != nil {
		return store.NilAddress, err
	}

	return rn.persist()

}

// CreateEmpty creates an empty btree
func CreateEmpty(rw store.ReaderWriter) (store.Address, error) {

	n := &node{
		Count:    0,
		Children: nil,
		KVS:      nil,
		address:  store.NilAddress,
		m:        M,
		reader:   rw,
		writer:   rw,
	}

	return n.persist()
}
