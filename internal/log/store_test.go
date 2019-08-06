package log

import (
	"os"
	"path"
	"testing"

	req "github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStore(t *testing.T) {
	name := path.Join(os.TempDir(), "log_test")
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND,
		0600,
	)
	req.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	req.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)

	s, _ = newStore(f)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		req.NoError(t, err)
		req.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.ReadAt(pos)
		req.NoError(t, err)
		req.Equal(t, write, read)
		pos += width
	}
}
