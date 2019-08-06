package log_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	req "github.com/stretchr/testify/require"
	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/log"
)

func TestCommitLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *log.Log){
		"append and read a batch succeeds": func(t *testing.T, log *log.Log) {
			append := &api.RecordBatch{
				Records: []*api.Record{{
					Value: []byte("hello world"),
				}},
			}
			off, err := log.Append(append)
			if err != nil {
				t.Fatal(err)
			}
			if off != 0 {
				t.Fatalf("got off: %d, want: %d", off, 0)
			}
			read, err := log.Read(off)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(append, read) {
				t.Fatalf("got read: %v, want: %v", read, append)
			}
		},
		"offset out of range error": func(t *testing.T, log *log.Log) {
			read, err := log.Read(0)
			if read != nil {
				t.Fatalf("expected read to be nil")
			}
			apiErr, ok := err.(api.ErrOffsetOutOfRange)
			if !ok {
				t.Fatalf("err type not ErrOffsetOutOfRange")
			}
			if apiErr.Offset != 0 {
				t.Fatalf("got offset: %d, want: %d", apiErr.Offset, 0)
			}
		},
		"init with existing segments": func(t *testing.T, o *log.Log) {
			append := &api.RecordBatch{
				Records: []*api.Record{{
					Value: []byte("hello world"),
				}},
			}
			for i := 0; i < 3; i++ {
				_, _ = o.Append(append)
			}

			req.NoError(t, o.Close())

			n, err := log.NewLog(o.Dir, o.Config)
			req.NoError(t, err)
			off, err := n.Append(append)
			req.NoError(t, err)
			req.Equal(t, uint64(3), off)
		},
	} {
		t.Run(scenario, func(t *testing.T) {
			base, err := ioutil.TempDir("", "store-test")
			req.NoError(t, err)
			defer os.RemoveAll(base)

			c := log.Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := log.NewLog(base, c)
			req.NoError(t, err)

			fn(t, log)
		})
	}
}
