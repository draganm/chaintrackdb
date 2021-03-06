package btree

import "github.com/draganm/chaintrackdb/store"

// Get returns address of the value for the given key
func Get(r store.Reader, root store.Address, key []byte) (store.Address, error) {

	n := &node{
		m:       M,
		address: root,
		reader:  r,
	}

	return n.get(key)
}
