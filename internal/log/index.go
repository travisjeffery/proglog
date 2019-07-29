package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	enc = binary.BigEndian
)

const (
	offWidth = 4
	posWidth = 8
	lenWidth = 8
	entWidth = offWidth + posWidth + lenWidth
)

type entry struct {
	Off uint32
	Pos uint64
	Len uint64
}

func (e entry) IsZero() bool {
	return e.Off == 0 &&
		e.Pos == 0 &&
		e.Len == 0
}

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
		idx.pos += entWidth
	}
	return idx, is.Entry(), nil
}

// Read takes in an offset relative to the index's base offset and returns the associated index
// entry, i.e. 0 is always the index's first entry.
func (i *index) Read(offset uint32) (e entry, err error) {
	pos := offset * entWidth
	if uint32(len(i.mmap)) < pos+entWidth {
		return e, io.EOF
	}
	p := make([]byte, entWidth)
	copy(p, i.mmap[pos:pos+entWidth])
	b := bytes.NewReader(p)
	if err = binary.Read(b, enc, &e); err != nil {
		return e, err
	}
	if e.IsZero() {
		return e, io.EOF
	}
	return e, nil
}

func (i *index) Write(e entry) error {
	if uint64(len(i.mmap)) < i.pos+entWidth {
		fmt.Println("heyheyhey", len(i.mmap), i.pos+entWidth)
		return io.EOF
	}
	b := new(bytes.Buffer)
	if err := binary.Write(b, enc, e); err != nil {
		return err
	}
	n := copy(i.mmap[i.pos:i.pos+entWidth], b.Bytes())
	i.pos += uint64(n)
	return nil
}

type indexScanner struct {
	// idx must be set
	idx *index
	off uint32
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
