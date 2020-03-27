package store

import "sync"

type Store struct {
	segments []*Segment
	mu       sync.Mutex
}
