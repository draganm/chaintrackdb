package store

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

type BlockWriter struct {
	st Reader
	BlockReader
	Data []byte
	Address
}

func (w BlockWriter) SetChild(i int, addr Address) error {

	if i >= w.NumberOfChildren() {
		return errors.New("trying to set child that segment does not have")
	}

	oldChildAddress := w.BlockReader.GetChildAddress(i)

	if oldChildAddress != NilAddress {
		oldChildReader, err := w.st.GetBlock(oldChildAddress)
		if err != nil {
			return errors.Wrap(err, "while getting child block reader")
		}

		err = w.subtractUsedData(oldChildReader.GetUsedDataSize())
		if err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint64(w.BlockReader[2+8+8+1+1+i*8:], uint64(addr))

	if addr == NilAddress {
		return nil
	}

	newChildReader, err := w.st.GetBlock(addr)
	if err != nil {
		return errors.Wrap(err, "while getting child block reader")
	}

	w.addUsedData(newChildReader.GetUsedDataSize())

	lowest := w.Address

	for i := 0; i < w.NumberOfChildren(); i++ {
		newChildReader, err = w.st.GetBlock(w.GetChildAddress(i))
		if err != nil {
			return errors.Wrap(err, "while getting child block reader")
		}

		lcd := newChildReader.GetLowestDescendentAddress()
		if lowest > lcd {
			lowest = lcd
		}
	}

	binary.BigEndian.PutUint64(w.BlockReader[2+8:], uint64(lowest))

	return nil

}

func (w BlockWriter) subtractUsedData(bytes uint64) error {
	used := w.GetUsedDataSize()
	if bytes > used {
		return errors.New("subtracting more data than used")
	}
	binary.BigEndian.PutUint64(w.BlockReader[2:], used-bytes)
	return nil
}

func (w BlockWriter) addUsedData(bytes uint64) {
	used := w.GetUsedDataSize()
	binary.BigEndian.PutUint64(w.BlockReader[2:], used+bytes)
}
