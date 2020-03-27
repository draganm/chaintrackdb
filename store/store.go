package store

import (
	"encoding/binary"
	"errors"
	"sync"
)

type Store struct {
	segments []*Segment
	mu       sync.Mutex
}

func (s *Store) getBlockReader(a Address) (BlockReader, error) {
	for _, s := range s.segments {
		br, err := s.getBlock(a)
		if err == ErrBlockNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		return br, nil
	}

	return nil, ErrBlockNotFound
}

func (s *Store) AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if numberOfChildren > 255 {
		return BlockWriter{}, errors.New("block can't have more than 255 children")
	}

	blockSize := uint64(2 + 8 + 8 + 1 + 1 + numberOfChildren*8 + dataSize)

	if blockSize > 0xffff {
		return BlockWriter{}, errors.New("block is too large")
	}

	lastSegment := s.segments[len(s.segments)-1]
	addr, blockData, err := lastSegment.AppendBlock(blockSize)
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
