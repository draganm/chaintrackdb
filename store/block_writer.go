package store

type BlockWriter struct {
	seg Segment
	BlockReader
	Data []byte
	Address
}
