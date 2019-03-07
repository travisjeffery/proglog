package log

import (
	"fmt"
	"sync"

	api "github.com/travisjeffery/proglog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	activeSegment *segment
	segments      []*segment
}

func (l *Log) AppendBatch(batch *api.RecordBatch) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()
	b, err := batch.Marshal()
	if err != nil {
		return 0, err
	}
	offset, position := l.activeSegment.nextOffset, l.activeSegment.position
	_, err = l.activeSegment.Write(b)
	if err != nil {
		return 0, err
	}
	if err = l.activeSegment.index.writeEntry(entry{
		Offset:   offset,
		Position: position,
		Length:   uint64(batch.Size()),
	}); err != nil {
		return 0, err
	}
	return offset, nil
}

func (l *Log) ReadBatch(offset uint64) (*api.RecordBatch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()
	if l.activeSegment.nextOffset == 0 ||
		l.activeSegment.nextOffset <= offset {
		fmt.Println("heyheyhey")
		return nil, api.ErrOffsetOutOfRange
	}
	entry, err := l.activeSegment.index.readEntry(offset)
	if err != nil {
		return nil, err
	}
	p := make([]byte, entry.Length)
	_, err = l.activeSegment.ReadAt(p, int64(entry.Position))
	if err != nil {
		return nil, err
	}
	batch := &api.RecordBatch{}
	err = batch.Unmarshal(p)
	return batch, err

}

func (l *Log) init() {
	if l.activeSegment == nil {
		l.activeSegment = &segment{
			dir: l.Dir, firstOffset: 0,
			index: &index{dir: l.Dir, firstOffset: 0},
		}
	}
}
