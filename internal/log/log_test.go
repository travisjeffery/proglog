// START: intro
package log_test

import (
	"io/ioutil"
	"os"
	"testing"

	req "github.com/stretchr/testify/require"
	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/log"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *log.Log){
		"append and read a batch succeeds": testAppendRead,
		"offset out of range error":        testOutOfRangeErr,
		"init with existing segments":      testInitExisting,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			req.NoError(t, err)
			defer os.RemoveAll(dir)

			c := log.Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := log.NewLog(dir, c)
			req.NoError(t, err)

			fn(t, log)
		})
	}
}

// END: intro

// START: tests
func testAppendRead(t *testing.T, log *log.Log) {
	append := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}
	off, err := log.Append(append)
	req.NoError(t, err)
	req.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	req.NoError(t, err)
	req.Equal(t, append, read)

}

func testOutOfRangeErr(t *testing.T, log *log.Log) {
	read, err := log.Read(1)
	req.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	req.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, o *log.Log) {
	append := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}
	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		req.NoError(t, err)
	}
	req.NoError(t, o.Close())

	n, err := log.NewLog(o.Dir, o.Config)
	req.NoError(t, err)
	off, err := n.Append(append)
	req.NoError(t, err)
	req.Equal(t, uint64(3), off)
}

// END: tests
