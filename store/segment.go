package store

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// Segment layout

// start addres - 8 bytes
// last block address - 8 bytes
// data - rest

type Segment struct {
	f    *os.File
	MMap mmap.MMap
}
