package log

import (
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestLog(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "log_test")
	req.NoError(t, err)
	defer os.Remove(f.Name())

	l, err := newLog(f)
	req.NoError(t, err)
	req.Equal(t, uint64(0), l.Size())

	testAppend(t, l)
	testRead(t, l)

	l, _ = newLog(f)
	testRead(t, l)
}

func testAppend(t *testing.T, l *log) {
	for i := uint64(1); i < 4; i++ {
		n, pos, err := l.Append(write)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		req.Equal(t, pos+n, width*i)
		req.Equal(t, l.Size(), width*i)
	}
}

func testRead(t *testing.T, l *log) {
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := l.ReadAt(pos)
		req.NoError(t, err)
		req.Equal(t, write, read)
		pos += width
	}
}
