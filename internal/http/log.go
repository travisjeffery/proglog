package http

import (
	"fmt"
	"sync"
)

type CommitLog struct {
	sync.Mutex
	currOffset uint64
	data       []RecordBatch
}

func NewCommitLog() *CommitLog {
	return &CommitLog{}
}

func (c *CommitLog) AppendBatch(batch RecordBatch) (uint64, error) {
	c.Lock()
	defer c.Unlock()
	c.currOffset++
	c.data[c.currOffset] = batch
	return c.currOffset, nil
}

func (c *CommitLog) ReadBatch(offset uint64) (RecordBatch, error) {
	c.Lock()
	defer c.Unlock()
	if offset >= uint64(len(c.data)) {
		return RecordBatch{}, ErrOffsetNotFound
	}
	return c.data[offset], nil
}

type Record struct {
	Value       []byte `json:"value"`
	OffsetDelta uint32 `json:"offset_delta"`
}

type RecordBatch struct {
	FirstOffset uint64   `json:"first_offset"`
	Records     []Record `json:"records"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
