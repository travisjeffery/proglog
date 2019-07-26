package log

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	encoding = binary.BigEndian
)

const (
	offsetWidth   = 8
	positionWidth = 8
	lengthWidth   = 8
	entryWidth    = offsetWidth + positionWidth + lengthWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	pos  uint64
}

// nextIndex returns the created index and its last entry.
func newIndex(f *os.File) (*index, entry, error) {
	idx := &index{
		file: f,
	}
	var err error
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, entry{}, err
	}
	is := &indexScanner{idx: idx}
	for is.Scan() {
		idx.pos += entryWidth
	}
	return idx, is.Entry(), nil
}

type entry struct {
	Off uint64
	Pos uint64
	Len uint64
}

func (e entry) IsZero() bool {
	return e.Off == 0 &&
		e.Pos == 0 &&
		e.Len == 0
}

func (i *index) Read(offset uint64) (e entry, err error) {
	pos := offset * entryWidth
	if uint64(len(i.mmap)) < pos+entryWidth {
		return e, io.EOF
	}
	p := make([]byte, entryWidth)
	copy(p, i.mmap[pos:pos+entryWidth])
	b := bytes.NewReader(p)
	if err = binary.Read(b, encoding, &e); err != nil {
		return e, err
	}
	if e.IsZero() {
		return e, io.EOF
	}
	return e, nil
}

func (i *index) Write(e entry) error {
	b := new(bytes.Buffer)
	if err := binary.Write(b, encoding, e); err != nil {
		return err
	}
	if uint64(len(i.mmap)) < i.pos+entryWidth {
		return io.EOF
	}
	n := copy(i.mmap[i.pos:i.pos+entryWidth], b.Bytes())
	i.pos += uint64(n)
	return nil
}

type indexScanner struct {
	// idx must be set
	idx *index
	off uint64
	cur entry
	err error
}

func (is *indexScanner) Scan() bool {
	var e entry
	if is.err != nil {
		return false
	}
	if e, is.err = is.idx.Read(is.off); is.err != nil {
		return false
	}
	is.cur = e
	is.off++
	return true
}

func (is *indexScanner) Entry() entry {
	return is.cur
}

func (is *indexScanner) Err() error {
	return is.err
}
