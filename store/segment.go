package store

import (
	"encoding/binary"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// Segment layout

// start addres - 8 bytes
// next block oofset - 8 bytes
// data - rest

type Segment struct {
	f           *os.File
	MMap        mmap.MMap
	currentSize uint64
}

// func OpenSegment(fileName string, maxSize uint) (*Segment, error) {
// 	f, err := os.OpenFile(fileName, os.O_RDWR, 0600)
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "while opening file %q", fileName)
// 	}

// 	fs, err := f.Stat()
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "while getting stats of file %q", fileName)
// 	}

// 	if fs.Size() <= 16 {
// 		return nil, errors.New("segment file %q does not have more than 16 bytes")
// 	}

// 	mm, err := mmap.MapRegion(f, int(maxSize), mmap.RDWR, 0, 0)
// 	if err != nil {
// 		f.Close()
// 		return nil, errors.Wrapf(err, "while mmaping file %q", fileName)
// 	}

// 	err = unix.Madvise(mm, unix.MADV_RANDOM)
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "while setting madvise to random for segment file %q", fileName)
// 	}

// 	s := &Segment{
// 		f:           f,
// 		MMap:        mm,
// 		currentSize: uint64(fs.Size()),
// 	}

// 	if s.LastBlockAddress()-s.StartAddress() >= s.currentSize {
// 		return nil, errors.New("last block offset is past end of segment")
// 	}

// 	br, err := NewBlockReader(s.MMap[int(s.LastBlockAddress()-s.StartAddress()):])
// 	if err != nil {
// 		return nil, errors.Wrap(err, "while reading last block")
// 	}

// 	if br.BlockSize()+s.LastBlockAddress()-s.StartAddress() >= s.currentSize {
// 		return nil, errors.New("end of last block is past end of segment")
// 	}

// 	return s, nil

// }

func (s *Segment) StartAddress() uint64 {
	return binary.BigEndian.Uint64(s.MMap)
}

func CreateSegment(fileName string, maxSize, offset uint64) (*Segment, error) {

	if offset == 0 {
		return nil, errors.New("offset must be > 0")
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	addressAndNextBlockOffset := make([]byte, 16)

	binary.BigEndian.PutUint64(addressAndNextBlockOffset, offset)
	binary.BigEndian.PutUint64(addressAndNextBlockOffset[8:], 16)

	_, err = f.Write(addressAndNextBlockOffset)
	if err != nil {
		return nil, errors.Wrapf(err, "while appending data to %q", fileName)
	}

	mm, err := mmap.MapRegion(f, int(maxSize), mmap.RDWR, 0, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err, "while mmaping file %q", fileName)
	}

	err = unix.Madvise(mm, unix.MADV_RANDOM)
	if err != nil {
		return nil, errors.Wrapf(err, "while setting madvise to random for segment file %q", fileName)
	}

	s := &Segment{
		f:           f,
		MMap:        mm,
		currentSize: uint64(len(addressAndNextBlockOffset)),
	}

	return s, nil

}

func (s *Segment) Close() error {
	err := s.MMap.Unmap()
	if err != nil {
		return errors.Wrapf(err, "while unmmaping %q", s.f.Name())
	}

	err = s.f.Close()
	if err != nil {
		return errors.Wrapf(err, "while closing %s", s.f.Name())
	}

	return nil

}

func (s *Segment) AppendBlock(blockType BlockType, numberOfChildren int, dataSize int) (BlockWriter, error) {

	// block length: 2 bytes
	// data size: 8 bytes
	// lowest descendent address: 8 bytes
	// type: byte
	// number_of_children: 1 byte
	// number_of_children * 8 bytes

	if numberOfChildren > 255 {
		return BlockWriter{}, errors.New("block can't have more than 255 children")
	}

	blockSize := uint64(2 + 8 + 8 + 1 + 1 + numberOfChildren*8 + dataSize)

	if blockSize > 0xffff {
		return BlockWriter{}, errors.New("block is too large")
	}

	err := s.ensureSpace(blockSize)
	if err != nil {
		return BlockWriter{}, err
	}

	addr := s.nextBlockOffset() + s.StartAddress() - 16

	blockData := s.MMap[s.nextBlockOffset() : s.nextBlockOffset()+blockSize]

	d := blockData

	// write end of new block
	binary.BigEndian.PutUint64(s.MMap[8:], s.nextBlockOffset()+blockSize)

	binary.BigEndian.PutUint64(blockData, uint64(blockSize))
	blockData = blockData[2:]

	binary.BigEndian.PutUint64(blockData, blockSize)
	blockData = blockData[8:]

	binary.BigEndian.PutUint64(blockData, addr)
	blockData = blockData[8:]

	blockData[0] = byte(blockType)
	blockData = blockData[1:]

	blockData[0] = byte(numberOfChildren)
	blockData = blockData[1+8*numberOfChildren:]

	return BlockWriter{
		BlockReader: BlockReader(d),
		Data:        blockData,
		Address:     Address(addr),
	}, nil

}

const minGrowSize = 16 * 1024 * 1024

func (s *Segment) ensureSpace(len uint64) error {

	if s.currentSize-s.nextBlockOffset() >= len {
		return nil
	}

	growsNeeded := (len - (s.nextBlockOffset() - s.currentSize)) / minGrowSize

	if (len-(s.nextBlockOffset()-s.currentSize))%minGrowSize > 0 {
		growsNeeded++
	}

	growBy := growsNeeded * minGrowSize

	err := s.f.Truncate(int64(s.currentSize + growBy))
	if err != nil {
		return errors.Wrapf(err, "wile growing %q to %d bytes", s.f.Name(), int64(s.currentSize+growBy))
	}

	s.currentSize += growBy

	return nil

}

func (s *Segment) nextBlockOffset() uint64 {
	return binary.BigEndian.Uint64(s.MMap[8:])
}
