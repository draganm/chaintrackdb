package store

type ReadTransaction interface {
	GetBlock(a Address) (BlockReader, error)
}

type rtx struct {
	s *Store
}

func (r *rtx) GetBlock(a Address) (BlockReader, error) {
	return r.s.getBlockReader(a)
}
