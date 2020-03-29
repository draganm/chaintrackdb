package store

import (
	"encoding/binary"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// segment layout

// start addres - 8 bytes
// next block oofset - 8 bytes
// data - rest

type segment struct {
	f           *os.File
	MMap        mmap.MMap
	currentSize uint64
}

func (s *segment) StartAddress() Address {
	return Address(binary.BigEndian.Uint64(s.MMap))
}

func (s *segment) endAddress() Address {
	return s.StartAddress() + Address(binary.BigEndian.Uint64(s.MMap[8:])) - 16
}

func (s *segment) hasBlock(a Address) bool {
	if s.StartAddress() > a {
		return false
	}

	if s.endAddress() < a {
		return false
	}

	return true
}

var ErrBlockNotFound = errors.New("block not found")

func (s *segment) getBlock(a Address) (BlockReader, error) {
	if !s.hasBlock(a) {
		return nil, ErrBlockNotFound
	}

	idx := uint64(a - s.StartAddress() + 16)

	return NewBlockReader(s.MMap[idx:])
}

func createSegment(fileName string, maxSize uint64, offset Address) (*segment, error) {

	if offset == 0 {
		return nil, errors.New("offset must be > 0")
	}

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	addressAndNextBlockOffset := make([]byte, 16)

	binary.BigEndian.PutUint64(addressAndNextBlockOffset, uint64(offset))
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

	s := &segment{
		f:           f,
		MMap:        mm,
		currentSize: uint64(len(addressAndNextBlockOffset)),
	}

	return s, nil

}

func openSegment(fileName string, maxSize uint64) (*segment, error) {

	f, err := os.OpenFile(fileName, os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	fs, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting fstat of %q", fileName)
	}

	if fs.Size() < 16 {
		return nil, errors.Errorf("file %s is shorter than 16 bytes", fileName)
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

	// TODO: check the last offset

	s := &segment{
		f:           f,
		MMap:        mm,
		currentSize: uint64(fs.Size()),
	}

	return s, nil

}

func (s *segment) close() error {
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

func (s *segment) appendBlock(blockSize uint64) (Address, []byte, error) {
	err := s.ensureSpace(blockSize)
	if err != nil {
		return NilAddress, nil, err
	}

	addr := s.nextBlockOffset() + uint64(s.StartAddress()) - 16

	blockData := s.MMap[s.nextBlockOffset() : s.nextBlockOffset()+blockSize]

	// write end of new block
	binary.BigEndian.PutUint64(s.MMap[8:], s.nextBlockOffset()+blockSize)

	return Address(addr), blockData, nil
}

const minGrowSize = 16 * 1024 * 1024

func (s *segment) ensureSpace(len uint64) error {

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

func (s *segment) nextBlockOffset() uint64 {
	return binary.BigEndian.Uint64(s.MMap[8:])
}
