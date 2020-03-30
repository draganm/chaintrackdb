package store

import (
	"context"
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
	dir                        string
	segments                   []*segment
	mu                         *sync.Mutex
	lastCommitAddress          *commitAddress
	readerTransactions         int
	writeTransactionInProgress bool
	writeTransactionCond       *sync.Cond
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
	l := new(sync.Mutex)
	wtc := sync.NewCond(l)
	st := &Store{
		dir:                  dir,
		mu:                   l,
		writeTransactionCond: wtc,
	}

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

	if ca.address() == NilAddress {

		// create empty btree root
		lastSeg := st.segments[len(st.segments)-1]

		blockSize := uint64(2 + 8 + 8 + 1 + 1 + 8)

		rootAddress, data, err := lastSeg.appendBlock(blockSize)
		if err != nil {
			return nil, errors.Wrap(err, "while appending inital commit block")
		}
		binary.BigEndian.PutUint16(data, uint16(blockSize))
		binary.BigEndian.PutUint64(data[2:], blockSize)
		binary.BigEndian.PutUint64(data[10:], uint64(rootAddress))
		data[18] = byte(TypeBTreeNode)

		ca.setAddress(rootAddress)

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

func (s *Store) NewReadTransaction() ReadTransaction {
	return &rtx{s}
}

func (s *Store) nextAddress() Address {
	ls := s.segments[len(s.segments)-1]
	return ls.endAddress()
}

func (s *Store) txRolledBack() {
	s.mu.Lock()
	s.writeTransactionInProgress = false
	s.writeTransactionCond.Broadcast()
	s.mu.Unlock()
}

func (s *Store) NewWriteTransaction(ctx context.Context) (WriteTransaction, error) {
	go func() {
		dc := ctx.Done()
		if dc != nil {
			select {
			case <-dc:
				s.mu.Lock()
				s.writeTransactionCond.Broadcast()
				s.mu.Unlock()
			}
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !s.writeTransactionInProgress {
			break
		}
		s.writeTransactionCond.Wait()
	}

	s.writeTransactionInProgress = true

	txSegment, err := createSegment(filepath.Join(s.dir, "tx"), MaxSegmentSize, s.nextAddress())

	if err != nil {
		return nil, errors.Wrap(err, "while creating tx segment")
	}

	return &wtx{
		s:         s,
		txSegment: txSegment,
		ctx:       ctx,
	}, nil

}
