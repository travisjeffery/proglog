package log

import (
	"bytes"
	"encoding/binary"
	"os"
)

const (
	lenWidth = 8
)

var (
	size = make([]byte, lenWidth)
)

type log struct {
	*os.File
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
	}, nil
}

// Append the bytes to the end of the log file and returns the position the bytes were written and
// the number of bytes written.
func (l *log) Append(p []byte) (uint64, uint64, error) {
	pos := l.size
	var buf bytes.Buffer
	if err := binary.Write(&buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	if _, err := buf.Write(p); err != nil {
		return 0, 0, err
	}
	n, err := l.WriteAt(buf.Bytes(), int64(pos))
	if err != nil {
		return 0, 0, err
	}
	l.size += uint64(n)
	return uint64(n), pos, nil
}

func (l *log) ReadAt(pos uint64) ([]byte, error) {
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
