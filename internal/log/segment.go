package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type segment struct {
	file        *os.File
	firstOffset uint64
	index       *index
	mu          sync.RWMutex
	nextOffset  uint64
	dir         string
	position    uint64
}

func (s *segment) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	n, err := s.file.Write(p)
	if err != nil {
		return 0, err
	}
	s.nextOffset++
	s.position += uint64(n)
	return n, nil
}

func (s *segment) Read(p []byte) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.init()
	return s.file.Read(p)
}

func (s *segment) ReadAt(p []byte, off int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.init()
	return s.file.ReadAt(p, off)
}

func (s *segment) init() {
	if s.file == nil {
		var err error
		if s.file, err = os.Create(s.path()); err != nil {
			panic(err)
		}
		s.nextOffset = s.firstOffset
		s.index = &index{
			firstOffset: s.firstOffset,
			dir:         s.dir,
		}
	}
}

func (s *segment) path() string {
	return filepath.Join(s.dir, fmt.Sprintf("%d", s.firstOffset))
}
