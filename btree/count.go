package btree

import "github.com/draganm/chaintrackdb/store"

// Count return the number of keys in the btree
func Count(r store.Reader, root store.Address) (uint64, error) {
	n := &node{
		address: root,
		reader:  r,
	}

	err := n.load()
	if err != nil {
		return 0, err
	}

	return n.Count, nil
}
