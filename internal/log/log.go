package log

import (
	"os"
	"sync/atomic"
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
	return &log{
		File: f,
		size: uint64(fi.Size()),
	}, nil
}

// Append append the bytes to the end of the log file and returns the length of the file.
func (l *log) Append(p []byte) (uint64, error) {
	n, err := l.Write(p)
	if err != nil {
		return 0, err
	}
	return atomic.AddUint64(&l.size, uint64(n)), nil
}
