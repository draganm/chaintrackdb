package store

type ReadTransaction struct {
	s *Store
}

func (r *ReadTransaction) GetBlock(a Address) (BlockReader, error) {
	return r.s.getBlockReader(a)
}
