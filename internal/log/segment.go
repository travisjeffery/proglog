package log

import (
	"fmt"
	"os"
	"path"
	"strings"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"
)

func trimSuffix(name string) string {
	return strings.TrimSuffix(name, path.Ext(name))
}

type segment struct {
	log                    *log
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	logFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, logSuffix)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if s.log, err = newLog(logFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, indexSuffix)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if err = indexFile.Truncate(
		int64(nearestMultiple(c.MaxIndexBytes, entryWidth)),
	); err != nil {
		return nil, err
	}
	var lastEntry entry
	if s.index, lastEntry, err = newIndex(indexFile); err != nil {
		return nil, err
	}
	if lastEntry.IsZero() {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = lastEntry.Off + 1
	}
	return s, nil
}

// Append the bytes to the segment and return the next offset and bytes in the segment.
func (s *segment) Append(p []byte) (nextOffset, size uint64, err error) {
	n, pos, err := s.log.Append(p)
	if err != nil {
		return 0, 0, err
	}
	if err = s.index.Write(entry{
		// index offsets are relative to base offset
		Off: s.nextOffset - s.baseOffset,
		Pos: pos,
		Len: n,
	}); err != nil {
		return 0, 0, err
	}
	s.nextOffset++
	return s.nextOffset, pos + n, nil
}

func (s *segment) FindIndex(off uint64) (entry, error) {
	return s.index.Read(off - s.baseOffset)
}

func (s *segment) ReadAt(p []byte, off int64) (int, error) {
	return s.log.ReadAt(p, off)
}

func (s *segment) IsMaxed() bool {
	return s.log.size > s.config.MaxSegmentBytes ||
		s.index.pos > s.config.MaxIndexBytes
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k

}
