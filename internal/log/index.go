package log

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	enc             = binary.BigEndian
	posWidth uint64 = 8
	entWidth        = posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	pos  uint64
}

// nextIndex returns the created index and the offset of the last entry.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.pos = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read returns the log's offset and the log's position in the log file, the given log offset must be relative to the
// base offset. For example, 0 always returns the first entry in index. -1 returns the last entry.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.pos == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.pos / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.pos < pos+entWidth {
		return 0, 0, io.EOF
	}
	p := make([]byte, entWidth)
	copy(p, i.mmap[pos:pos+entWidth])
	pos = enc.Uint64(p)
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.pos+entWidth {
		return io.EOF
	}
	p := make([]byte, entWidth)
	enc.PutUint64(p, pos)
	copy(i.mmap[i.pos:i.pos+entWidth], p)
	i.pos += uint64(entWidth)
	return nil
}

func (i *index) Close() error {
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.pos)); err != nil {
		return err
	}
	return i.file.Close()
}
