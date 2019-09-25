package log

import (
	"os"
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
	n, err := l.WriteAt(p, int64(pos))
	if err != nil {
		return 0, 0, err
	}
	l.size += uint64(n)
	return uint64(n), pos, nil
}

func (l *log) Size() uint64 {
	return l.size
}
