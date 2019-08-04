package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	req.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	req.NoError(t, err)
	_, _, err = idx.Read(-1)
	req.Error(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		req.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		req.NoError(t, err)
		req.Equal(t, want.Pos, pos)
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	req.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR|os.O_EXCL, 0600)
	idx, err = newIndex(f, c)
	req.NoError(t, err)
	off, pos, err := idx.Read(-1)
	req.NoError(t, err)
	req.Equal(t, uint32(1), off)
	req.Equal(t, entries[1].Pos, pos)
}
