package log

import (
	"io/ioutil"
	"sort"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	api "github.com/travisjeffery/proglog/api/v1"
)

type Log struct {
	sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

type Config struct {
	MaxSegmentBytes uint64
	MaxIndexBytes   uint64
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.MaxSegmentBytes == 0 {
		c.MaxSegmentBytes = 1024
	}
	if c.MaxIndexBytes == 0 {
		c.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var baseOffsets []uint64
	for _, file := range files {
		off, _ := strconv.ParseUint(trimSuffix(file.Name()), 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return nil, err
		}
		// baseOffset contains dup for index and log so we skip the dup
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(0); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Log) AppendBatch(batch *api.RecordBatch) (uint64, error) {
	l.Lock()
	defer l.Unlock()
	p, err := proto.Marshal(batch)
	if err != nil {
		return 0, err
	}
	curr := l.activeSegment.nextOffset
	next, _, err := l.activeSegment.Append(p)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(next)
	}
	return curr, err
}

func (l *Log) ReadBatch(offset uint64) (*api.RecordBatch, error) {
	l.RLock()
	defer l.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= offset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= offset {
		return nil, api.ErrOffsetOutOfRange{Offset: offset}
	}
	entry, err := s.FindIndex(offset)
	if err != nil {
		return nil, err
	}
	p := make([]byte, entry.Len)
	_, err = s.ReadAt(p, int64(entry.Pos))
	if err != nil {
		return nil, err
	}
	batch := &api.RecordBatch{}
	err = proto.Unmarshal(p, batch)
	return batch, err
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
