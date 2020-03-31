package btree

import "github.com/draganm/chaintrackdb/store"

func Get(r store.Reader, root store.Address, key []byte) (store.Address, error) {

	n := &node{
		m:       15,
		address: root,
		reader:  r,
	}

	return n.get(key)
}
