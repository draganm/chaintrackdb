package data

import (
	"github.com/draganm/chaintrackdb/store"
	"github.com/pkg/errors"
)

func StoreData(st store.ReaderWriter, data []byte, segSize, fanout int) (store.Address, error) {
	w := NewDataWriter(st, segSize, fanout)
	_, err := w.Write(data)
	if err != nil {
		return store.NilAddress, errors.Wrap(err, "while writing to data writer")
	}

	return w.Finish()

}
