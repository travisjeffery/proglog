package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	p := []byte("hello world")
	l := uint64(len(p))

	s, err := newSegment(dir, 16, Config{
		MaxSegmentBytes: 1024,
		MaxIndexBytes:   entryWidth * 2,
	})
	req.NoError(t, err)
	req.Equal(t, uint64(16), s.nextOffset)

	for i := uint64(1); i < 3; i++ {
		off, size, err := s.Append(p)
		req.NoError(t, err)
		req.Equal(t, l*i, size)
		req.Equal(t, 16+i, off)

		e, err := s.FindIndex(off - 1)
		req.NoError(t, err)
		req.Equal(t, i-1, e.Off)
		req.Equal(t, e.Pos, size-l)
		req.Equal(t, e.Len, l)

	}

	_, _, err = s.Append(p)
	req.Equal(t, io.EOF, err)
}
