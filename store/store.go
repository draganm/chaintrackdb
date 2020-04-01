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

func segmentName(startAddress Address) string {
	return fmt.Sprintf("segment-%016d", startAddress)
}

func (s *Store) GetBlock(a Address) (BlockReader, error) {
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

func (s *Store) NewReadTransaction() *ReadTransaction {
	return &ReadTransaction{s}
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

func (s *Store) copyBlocks(current, lowestAddress Address) (Address, error) {

	if current == NilAddress {
		return NilAddress, nil
	}

	if current >= lowestAddress {
		return current, nil
	}

	lastSegment := s.segments[len(s.segments)-1]

	br, err := s.GetBlock(current)
	if err != nil {
		return NilAddress, errors.Wrapf(err, "while getting block %d", current)
	}

	addr, nbd, err := lastSegment.appendBlock(uint64(len(br)))
	if err != nil {
		return NilAddress, errors.Wrap(err, "while appending block")
	}

	copy(nbd, br)

	// set total data to block size
	binary.BigEndian.PutUint64(nbd[2:], uint64(len(nbd)))

	// set lowest address to block address
	binary.BigEndian.PutUint64(nbd[2+8:], uint64(addr))

	// zero all children
	numberOfChildren := int(nbd[2+8+8+1])
	for i := 0; i < numberOfChildren; i++ {
		binary.BigEndian.PutUint64(nbd[2+8+8+1+1+8*i:], 0)
	}

	// uint64(2 + 8 + 8 + 1 + 1 + numberOfChildren*8 + dataSize)

	nbr, err := NewBlockReader(nbd)
	if err != nil {
		return NilAddress, errors.Wrap(err, "while creating reader for the copied block")
	}

	bw := BlockWriter{
		st:          s,
		Address:     addr,
		BlockReader: nbr,
		Data:        nbr.GetData(),
	}

	for i := 0; i < numberOfChildren; i++ {
		newAddress, err := s.copyBlocks(br.GetChildAddress(i), lowestAddress)
		if err != nil {
			return NilAddress, err
		}
		err = bw.SetChild(i, newAddress)
		if err != nil {
			return NilAddress, errors.Wrap(err, "while setting child address of the new block")
		}
	}

	return addr, nil

}

func (s *Store) txCommited(newRoot Address) (Address, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldRoot := s.lastCommitAddress.address()

	if oldRoot == newRoot {
		return oldRoot, nil
	}

	oldRootReader, err := s.GetBlock(oldRoot)
	if err != nil {
		return NilAddress, errors.Wrap(err, "while getting reader for the old root")
	}

	s.lastCommitAddress.setAddress(newRoot)
	br, err := s.GetBlock(newRoot)
	if err != nil {
		return NilAddress, errors.Wrap(err, "while getting reader for the new root")
	}

	lda := br.GetLowestDescendentAddress()
	highestAddress := newRoot + Address(len(br))

	dataWritten := uint64(highestAddress - (oldRoot + Address(len(oldRootReader))))

	newLda := lda + Address(dataWritten)

	rolledRoot, err := s.copyBlocks(newRoot, newLda)

	s.lastCommitAddress.setAddress(rolledRoot)

	err = s.createNewSegmentIfNeeded()
	if err != nil {
		return NilAddress, err
	}

	err = s.removeUnusedSegments()
	if err != nil {
		return NilAddress, err
	}

	return rolledRoot, nil
}

func (s *Store) totalSize() (uint64, error) {
	rootAddress := s.lastCommitAddress.address()
	rr, err := s.GetBlock(rootAddress)
	if err != nil {
		return 0, errors.Wrap(err, "while reading root block")
	}

	return uint64(rootAddress-rr.GetLowestDescendentAddress()) + uint64(len(rr)), nil
}

func (s *Store) removeUnusedSegments() error {
	rootAddress := s.lastCommitAddress.address()
	rr, err := s.GetBlock(rootAddress)
	if err != nil {
		return errors.Wrap(err, "while reading root block")
	}
	lowest := rr.GetLowestDescendentAddress()
	for s.segments[0].endAddress() < lowest {
		err = s.segments[0].closeAndRemove()
		if err != nil {
			return err
		}
		s.segments = s.segments[1:]
	}
	return nil
}

func (s *Store) createNewSegmentIfNeeded() error {

	totalSize, err := s.totalSize()
	if err != nil {
		return err
	}

	lastSeg := s.segments[len(s.segments)-1]

	if lastSeg.dataContained() < (totalSize >> 4) {
		return nil
	}

	addr := lastSeg.endAddress()

	name := filepath.Join(s.dir, segmentName(addr))

	newSeg, err := createSegment(name, MaxSegmentSize, addr)
	if err != nil {
		return errors.Wrapf(err, "while creating segment %s", name)
	}

	s.segments = append(s.segments, newSeg)

	return nil

}

func (s *Store) NewWriteTransaction(ctx context.Context) (*WriteTransaction, Address, error) {
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
			return nil, NilAddress, ctx.Err()
		}
		if !s.writeTransactionInProgress {
			break
		}
		s.writeTransactionCond.Wait()
	}

	s.writeTransactionInProgress = true

	txSegment, err := createSegment(filepath.Join(s.dir, "tx"), MaxSegmentSize, s.nextAddress())

	if err != nil {
		return nil, NilAddress, errors.Wrap(err, "while creating tx segment")
	}

	return &WriteTransaction{
		s:         s,
		txSegment: txSegment,
		ctx:       ctx,
	}, s.lastCommitAddress.address(), nil

}
