package log

import (
	"bufio"
	"encoding/binary"
	"os"
)

type log struct {
	*os.File
	buf  *bufio.Writer
	size uint64
}

func newLog(f *os.File) (*log, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &log{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append the bytes to the end of the log file and returns the position the bytes were written and
// the number of bytes written.
func (l *log) Append(p []byte) (n uint64, pos uint64, err error) {
	pos = l.size
	if err := binary.Write(l.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := l.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	l.size += uint64(w)
	return uint64(w), pos, nil
}

const lenWidth = 8

var size = make([]byte, lenWidth)

func (l *log) ReadAt(pos uint64) ([]byte, error) {
	if err := l.buf.Flush(); err != nil {
		return nil, err
	}
	if _, err := l.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := l.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (l *log) Size() uint64 {
	return l.size
}
