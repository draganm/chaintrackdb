package store

import (
	"encoding/binary"
	"errors"
)

type WriteTransaction struct {
	s         *Store
	txSegment *segment
}

func (w *WriteTransaction) AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error) {
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

	binary.BigEndian.PutUint64(blockData, uint64(blockSize))
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
		BlockReader: BlockReader(d),
		Data:        blockData,
		Address:     addr,
	}, nil

}

func (w *WriteTransaction) GetBlock(a Address) (BlockReader, error) {
	if w.txSegment.hasBlock(a) {
		return w.txSegment.getBlock(a)
	}
	return w.s.getBlockReader(a)
}
