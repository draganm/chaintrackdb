package btree

import "github.com/draganm/chaintrackdb/store"

const M = 15

func Put(rw store.ReaderWriter, root store.Address, key []byte, value store.Address) (store.Address, error) {
	n := &node{
		m:       15,
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
