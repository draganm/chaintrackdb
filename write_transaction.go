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
	pth, err := dbpath.Split(path)
	if err != nil {
		return errors.Wrap(err, "while parsing dbpath")
	}

	if len(pth) != 1 {
		return errors.New("onlu path of length 1 is supported")
	}

	addr, err := btree.CreateEmpty(w.swt)
	if err != nil {
		return errors.Wrap(err, "while creating empty map")
	}

	newRoot, err := btree.Put(w.swt, w.root, []byte(pth[0]), addr)
	if err != nil {
		return errors.Wrap(err, "while changing root")
	}

	w.root = newRoot

	return nil
}
