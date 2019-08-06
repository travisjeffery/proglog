package log

import (
	"bufio"
	"encoding/binary"
	"os"
)

type store struct {
	*os.File
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append the bytes to the end of the store file and returns the position the bytes were written and
// the number of bytes written.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

const lenWidth = 8

var size = make([]byte, lenWidth)

func (s *store) ReadAt(pos uint64) ([]byte, error) {
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}
