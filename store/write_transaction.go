package store

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
)

type WriteTransaction interface {
	ReadTransaction
	AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error)
	Rollback() error
	Commit(Address) (Address, error)
}

type wtx struct {
	s         *Store
	txSegment *segment
	ctx       context.Context
}

func (w *wtx) AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error) {
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
		st:          w.s,
		BlockReader: BlockReader(d),
		Data:        blockData,
		Address:     addr,
	}, nil

}

func (w *wtx) GetBlock(a Address) (BlockReader, error) {
	err := w.ctx.Err()
	if err != nil {
		return nil, err
	}

	if w.txSegment.hasBlock(a) {
		return w.txSegment.getBlock(a)
	}
	return w.s.getBlockReader(a)
}

func (w *wtx) Rollback() error {
	w.s.txRolledBack()
	err := w.txSegment.closeAndRemove()
	if err != nil {
		return errors.Wrap(err, "while closing and removing tx segment")
	}
	return nil
}

func (w *wtx) Commit(a Address) (Address, error) {
	return NilAddress, errors.New("Commit is not yet supported")
}
