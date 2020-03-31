package btree

import "github.com/draganm/chaintrackdb/store"

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
