package store

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type Store struct {
	segments          []*segment
	mu                sync.Mutex
	lastCommitAddress *commitAddress
}

var storeRegexp = regexp.MustCompile("^segment-[0-9]*.dat$")

const MaxSegmentSize = 1024 * 1024 * 1024 * 1024

func Open(dir string) (*Store, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "while reading dir %s", dir)
	}

	segmentFiles := []string{}

	for _, f := range files {
		if f.Mode().IsRegular() && storeRegexp.MatchString(f.Name()) {
			segmentFiles = append(segmentFiles, filepath.Join(dir, f.Name()))
		}
	}

	sort.Strings(segmentFiles)

	st := &Store{}

	for _, sf := range segmentFiles {
		s, err := openSegment(sf, MaxSegmentSize)
		if err != nil {
			return nil, err
		}
		st.segments = append(st.segments, s)
	}

	if len(st.segments) == 0 {
		s, err := createSegment(filepath.Join(dir, segmentName(1)), MaxSegmentSize, 1)
		if err != nil {
			return nil, err
		}
		st.segments = []*segment{s}
	}

	ca, err := openCommitAddress(filepath.Join(dir, "commitAddress"))
	if err != nil {
		return nil, err
	}

	st.lastCommitAddress = ca

	return st, nil

}

func segmentName(startAddress uint64) string {
	return fmt.Sprintf("segment-%016d", startAddress)
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

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.lastCommitAddress.close()
	if err != nil {
		return errors.Wrap(err, "while cosing last commit address")
	}

	for _, seg := range s.segments {
		err = seg.close()
		if err != nil {
			return errors.Wrap(err, "while closing a segment")
		}
	}
	return nil
}
