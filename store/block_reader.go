package store

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// layout
// block length: 2 bytes
// data size: 8 bytes
// lowest descendent address: 8 bytes
// type: byte
// number_of_children: 1 byte
// number_of_children * 8 bytes
//

type BlockReader []byte

func NewBlockReader(data []byte) (BlockReader, error) {
	if len(data) < 2+8+8+1+1 {
		return nil, errors.New("block data is too short")
	}

	totalLength := int(binary.BigEndian.Uint16(data))

	if len(data) < totalLength {
		panic(errors.New("block data is too short"))
	}

	numberOfChildren := data[2+8+8+1]

	headerLength := 2 + 8 + 8 + 1 + 1 + numberOfChildren*8

	if int(headerLength) > totalLength {
		panic(errors.New("total length is too short"))
	}

	return data[:totalLength], nil

}

func (s BlockReader) NumberOfChildren() int {
	return int(s[2+8+8+1])
}

func (s BlockReader) BlockSize() uint64 {
	return uint64(binary.BigEndian.Uint16(s))
}

func (s BlockReader) GetChildAddress(i int) Address {
	if i < 0 {
		panic("negative child index")
	}

	if i >= s.NumberOfChildren() {
		panic("trying to get address of not existing child")
	}

	return Address(binary.BigEndian.Uint64(s[2+8+8+1+1+8*i:]))
}

func (s BlockReader) GetData() []byte {
	nc := s.NumberOfChildren()
	return s[2+8+8+1+1+8*nc:]
}

func (s BlockReader) GetUsedDataSize() uint64 {
	return binary.BigEndian.Uint64(s[2:])
}

func (s BlockReader) GetLowestDescendentAddress() uint64 {
	return binary.BigEndian.Uint64(s[2+8:])
}

func (s BlockReader) Type() BlockType {
	return BlockType(s[2+8+8])
}

func (s BlockReader) String() string {
	nc := s.NumberOfChildren()
	t := s.Type()
	children := make([]Address, nc)
	for i := 0; i < nc; i++ {
		children[i] = s.GetChildAddress(i)
	}

	return fmt.Sprintf("type %s data: %x, children: %q", t, s.GetData(), children)
}
