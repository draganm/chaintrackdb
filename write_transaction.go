package chaintrackdb

import (
	"context"

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
