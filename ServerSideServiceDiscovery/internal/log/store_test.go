// START: intro
package log

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)

	s, _ = newStore(f)
	testRead(t, s)
}
// END: intro

// START: end
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.ReadAt(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}
// END: end
