package store

import "encoding/binary"

type BlockWriter struct {
	seg Segment
	BlockReader
	Data []byte
	Address
}

func (s BlockWriter) SetChild(i int, addr Address) {

	if i >= s.NumberOfChildren() {
		panic("trying to set child that segment does not have")
	}

	oldChildAddress := s.SegmentReader.GetChildAddress(i)

	if oldChildAddress != NilAddress {
		for i := 0; i < 4; i++ {
			oldChildReader := s.st.GetSegment(oldChildAddress)
			newSize := s.GetLayerTotalSize(i) - oldChildReader.GetLayerTotalSize(i)
			s.SetLayerTotalSize(i, newSize)
		}
	}

	binary.BigEndian.PutUint64(s.SegmentReader[4+1+4*8+1+i*8:], uint64(addr))

	if addr == NilAddress {
		return
	}

	newChildReader := NewSegmentReader(s.st.GetSegment(addr))

	for i := 0; i < 4; i++ {
		newSize := s.GetLayerTotalSize(i) + newChildReader.GetLayerTotalSize(i)
		s.SetLayerTotalSize(i, newSize)
	}

}
