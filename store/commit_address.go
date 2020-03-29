package store

import (
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type commitAddress struct {
	f    *os.File
	MMap mmap.MMap
}

func openCommitAddress(fileName string) (*commitAddress, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "while opening file %q", fileName)
	}

	fs, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "while getting fstat of %q", fileName)
	}

	switch s := fs.Size(); s {
	case 0:
		b := make([]byte, 8)
		_, err = f.Write(b)
		if err != nil {
			return nil, errors.Wrap(err, "while writing nil commit address")
		}
	case 8:
		// all good
	default:
		return nil, errors.Errorf("file %s bas %d bytes - expected 0 or 8", fileName, s)
	}

	mm, err := mmap.MapRegion(f, 8, mmap.RDWR, 0, 0)
	if err != nil {
		f.Close()
		return nil, errors.Wrapf(err, "while mmaping file %q", fileName)
	}

	err = unix.Madvise(mm, unix.MADV_RANDOM)
	if err != nil {
		return nil, errors.Wrapf(err, "while setting madvise to random for segment file %q", fileName)
	}

	s := &commitAddress{
		f:    f,
		MMap: mm,
	}

	return s, nil

}
