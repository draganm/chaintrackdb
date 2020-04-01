package chaintrackdb

import (
	"github.com/draganm/chaintrackdb/store"
	"github.com/pkg/errors"
)

type DB struct {
	s *store.Store
}

func Open(path string) (*DB, error) {
	s, err := store.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "while opening db")
	}

	return &DB{
		s: s,
	}, nil
}

func (d *DB) Close() error {
	return d.s.Close()
}
