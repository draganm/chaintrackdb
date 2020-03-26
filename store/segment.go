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
// last block address - 8 bytes
// data - rest

type Segment struct {
	f           *os.File
	MMap        mmap.MMap
	currentSize uint64
}

func OpenSegment(fileName string, maxSize uint) (*Segment, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	fs, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting stats of file %q", fileName)
	}

	if fs.Size() <= 16 {
		return nil, errors.New("segment file %q does not have more than 16 bytes")
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
		currentSize: uint64(fs.Size()),
	}

	if s.LastBlockAddress()-s.StartAddress() >= s.currentSize {
		return nil, errors.New("last block offset is past end of segment")
	}

	br, err := NewBlockReader(s.MMap[int(s.LastBlockAddress()-s.StartAddress()):])
	if err != nil {
		return nil, errors.Wrap(err, "while reading last block")
	}

	if br.BlockSize()+s.LastBlockAddress()-s.StartAddress() >= s.currentSize {
		return nil, errors.New("end of last block is past end of segment")
	}

	return s, nil

}

func (s *Segment) IsEmpty() bool {
	return s.LastBlockAddress() == 0
}

func (s *Segment) StartAddress() uint64 {
	return binary.BigEndian.Uint64(s.MMap)
}

func (s *Segment) LastBlockAddress() uint64 {
	return binary.BigEndian.Uint64(s.MMap[8:])
}

func CreateSegment(fileName string, maxSize, offset uint64) (*Segment, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	offsetAndLastBlockAddress := make([]byte, 16)

	binary.BigEndian.PutUint64(offsetAndLastBlockAddress, offset)
	binary.BigEndian.PutUint64(offsetAndLastBlockAddress[8:], 0)

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
		currentSize: uint64(len(offsetAndLastBlockAddress)),
	}

	return s, nil

}
