package log

import (
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	api "github.com/travisjeffery/proglog/api/v1"
)

type CommitLog struct {
	sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

type Config struct {
	Segment struct {
		MaxLogBytes   uint64
		MaxIndexBytes uint64
	}
}

func NewCommitLog(dir string, c Config) (*CommitLog, error) {
	if c.Segment.MaxLogBytes == 0 {
		c.Segment.MaxLogBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &CommitLog{
		Dir:    dir,
		Config: c,
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
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

func (l *CommitLog) AppendBatch(batch *api.RecordBatch) (uint64, error) {
	l.Lock()
	defer l.Unlock()
	p, err := proto.Marshal(batch)
	if err != nil {
		return 0, err
	}
	off, err := l.activeSegment.Append(p)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *CommitLog) ReadBatch(off uint64) (*api.RecordBatch, error) {
	l.RLock()
	defer l.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	p, err := s.Read(off)
	if err != nil {
		return nil, err
	}
	batch := &api.RecordBatch{}
	err = proto.Unmarshal(p, batch)
	return batch, err
}

func (l *CommitLog) Close() error {
	l.Lock()
	defer l.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *CommitLog) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
