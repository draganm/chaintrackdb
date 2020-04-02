package store

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
)

type Writer interface {
	AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error)
}

type ReaderWriter interface {
	Reader
	Writer
}

type WriteTransaction struct {
	s         *Store
	txSegment *segment
	ctx       context.Context
}

func (w *WriteTransaction) AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error) {
	err := w.ctx.Err()
	if err != nil {
		return BlockWriter{}, err
	}

	if numberOfChildren > 255 {
		return BlockWriter{}, errors.New("block can't have more than 255 children")
	}

	blockSize := uint64(2 + 8 + 8 + 1 + 1 + numberOfChildren*8 + dataSize)

	if blockSize > 0xffff {
		return BlockWriter{}, errors.New("block is too large")
	}

	lastSegment := w.txSegment
	addr, blockData, err := lastSegment.appendBlock(blockSize)
	if err != nil {
		return BlockWriter{}, err
	}

	d := blockData

	binary.BigEndian.PutUint16(blockData, uint16(blockSize))
	blockData = blockData[2:]

	binary.BigEndian.PutUint64(blockData, blockSize)
	blockData = blockData[8:]

	binary.BigEndian.PutUint64(blockData, uint64(addr))
	blockData = blockData[8:]

	blockData[0] = byte(blockType)
	blockData = blockData[1:]

	blockData[0] = byte(numberOfChildren)
	blockData = blockData[1+8*numberOfChildren:]

	return BlockWriter{
		st:          w,
		BlockReader: BlockReader(d),
		Data:        blockData,
		Address:     addr,
	}, nil

}

func (w *WriteTransaction) GetBlock(a Address) (BlockReader, error) {
	err := w.ctx.Err()
	if err != nil {
		return nil, err
	}

	if w.txSegment.hasBlock(a) {
		return w.txSegment.getBlock(a)
	}
	return w.s.GetBlock(a)
}

func (w *WriteTransaction) Rollback() error {
	w.s.txRolledBack()
	err := w.txSegment.closeAndRemove()
	if err != nil {
		return errors.Wrap(err, "while closing and removing tx segment")
	}
	return nil
}

func (w *WriteTransaction) Commit(a Address) (newRoot Address, err error) {

	defer w.txSegment.closeAndRemove()

	newRoot = a
	if a >= w.txSegment.startAddress() {
		shouldCopy := func(a Address) bool {
			return a >= txStartAddress
		}

		newRoot, err = copyBlocks(w, w.s.lastSegment(), a, shouldCopy)
		if err != nil {
			return NilAddress, err
		}

	}

	return w.s.txCommited(newRoot)

}

func copyBlocks(r Reader, w *segment, current Address, shouldCopy func(Address) bool) (Address, error) {

	if current == NilAddress {
		return NilAddress, nil
	}

	if !shouldCopy(current) {
		return current, nil
	}

	br, err := r.GetBlock(current)
	if err != nil {
		return NilAddress, errors.Wrapf(err, "while getting block %d", current)
	}

	numberOfChildren := br.NumberOfChildren()
	children := make([]Address, numberOfChildren)

	for i := 0; i < br.NumberOfChildren(); i++ {
		newAddress, err := copyBlocks(r, w, br.GetChildAddress(i), shouldCopy)
		if err != nil {
			return NilAddress, err
		}

		children[i] = newAddress
	}

	addr, nbd, err := w.appendBlock(uint64(len(br)))
	if err != nil {
		return NilAddress, errors.Wrap(err, "while appending block")
	}

	copy(nbd, br)

	// set total data to block size
	binary.BigEndian.PutUint64(nbd[2:], uint64(len(nbd)))

	// set lowest address to block address
	binary.BigEndian.PutUint64(nbd[2+8:], uint64(addr))

	// zero all children
	for i := 0; i < numberOfChildren; i++ {
		binary.BigEndian.PutUint64(nbd[2+8+8+1+1+8*i:], 0)
	}

	// uint64(2 + 8 + 8 + 1 + 1 + numberOfChildren*8 + dataSize)

	nbr, err := NewBlockReader(nbd)
	if err != nil {
		return NilAddress, errors.Wrap(err, "while creating reader for the copied block")
	}

	// TODO: write children first, then create a new block

	bw := BlockWriter{
		st:          r,
		Address:     addr,
		BlockReader: nbr,
		Data:        nbr.GetData(),
	}

	for i := 0; i < numberOfChildren; i++ {
		err = bw.SetChild(i, children[i])
		if err != nil {
			return NilAddress, errors.Wrap(err, "while setting child address of the new block")
		}
	}

	return addr, nil

}
