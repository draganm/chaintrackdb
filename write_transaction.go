package chaintrackdb

import (
	"context"

	"github.com/draganm/chaintrackdb/btree"
	"github.com/draganm/chaintrackdb/dbpath"
	"github.com/draganm/chaintrackdb/store"
	"github.com/pkg/errors"
)

type WriteTransaction struct {
	root store.Address
	swt  *store.WriteTransaction
}

func (d *DB) WriteTransaction(ctx context.Context, f func(tx *WriteTransaction) error) error {

	swt, root, err := d.s.NewWriteTransaction(ctx)
	if err != nil {
		return errors.Wrap(err, "while creating store transaction")
	}

	tx := &WriteTransaction{
		root: root,
		swt:  swt,
	}

	err = f(tx)

	if err != nil {
		rbe := swt.Rollback()
		if rbe != nil {
			return errors.Wrap(err, "while rolling back transaction")
		}
		return err
	}

	_, err = swt.Commit(tx.root)

	if err != nil {
		return errors.Wrap(err, "while commiting transaction")
	}

	return nil
}

func (w *WriteTransaction) CreateMap(path string) error {
	return w.modifyPath(path, func(ad store.Address, key string) (store.Address, error) {
		addr, err := btree.CreateEmpty(w.swt)
		if err != nil {
			return store.NilAddress, errors.Wrap(err, "while creating empty map")
		}

		return btree.Put(w.swt, w.root, []byte(key), addr)
	})
}

func (w *WriteTransaction) modifyPath(path string, f func(ad store.Address, key string) (store.Address, error)) error {
	pth, err := dbpath.Split(path)
	if err != nil {
		return errors.Wrapf(err, "while parsing dbpath %q", path)
	}
	nr, err := modifyPath(w.swt, w.root, pth, f)
	if err != nil {
		return errors.Wrap(err, "while modifying path")
	}
	w.root = nr
	return nil
}

func modifyPath(st store.ReaderWriter, ad store.Address, path []string, f func(ad store.Address, key string) (store.Address, error)) (store.Address, error) {

	if len(path) == 0 {
		return store.NilAddress, errors.New("attempted to modify parent of root")
	}

	if len(path) > 1 {
		ca, err := btree.Get(st, ad, []byte(path[0]))
		if err != nil {
			return store.NilAddress, err
		}
		nca, err := modifyPath(st, ca, path[1:], f)
		if err != nil {
			return store.NilAddress, err
		}
		return btree.Put(st, ad, []byte(path[0]), nca)
	}

	return f(ad, path[0])

}
