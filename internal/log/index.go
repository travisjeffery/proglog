package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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
	maxEntries    = 1000
)

type index struct {
	mu          sync.Mutex
	mmap        gommap.MMap
	position    uint64
	file        *os.File
	dir         string
	firstOffset uint64
}

type entry struct {
	Offset   uint64
	Position uint64
	Length   uint64
}

func (i *index) readEntry(offset uint64) (e entry, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.init()
	p := make([]byte, entryWidth)
	pos := offset * entryWidth
	copy(p, i.mmap[pos:pos+entryWidth])
	b := bytes.NewReader(p)
	err = binary.Read(b, encoding, &e)
	return e, err
}

func (i *index) writeEntry(e entry) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.init()
	b := new(bytes.Buffer)
	if err := binary.Write(b, encoding, e); err != nil {
		return err
	}
	n, err := i.WriteAt(b.Bytes(), int64(i.position))
	if err != nil {
		return err
	}
	i.position += uint64(n)
	return nil

}

func (i *index) WriteAt(p []byte, offset int64) (int, error) {
	i.init()
	n := copy(i.mmap[offset:offset+entryWidth], p)
	return n, nil
}

func (i *index) init() {
	if i.file == nil {
		var err error
		if i.file, err = os.Create(i.path()); err != nil {
			panic(err)
		}
		if err = i.file.Truncate(entryWidth * maxEntries); err != nil {
			panic(err)
		}
		if i.mmap, err = gommap.Map(
			i.file.Fd(),
			gommap.PROT_READ|gommap.PROT_WRITE,
			gommap.MAP_SHARED,
		); err != nil {
			panic(err)
		}
	}
}

func (i *index) path() string {
	return filepath.Join(i.dir, fmt.Sprintf("%d.index", i.firstOffset))
}
