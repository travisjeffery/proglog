package log

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	api "github.com/travisjeffery/proglog/api/v1"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a batch succeeds": func(t *testing.T, log *Log) {
			append := &api.RecordBatch{
				Records: []*api.Record{{
					Value: []byte("hello world"),
				}},
			}
			off, err := log.AppendBatch(append)
			if err != nil {
				t.Fatal(err)
			}
			if off != 0 {
				t.Fatalf("got off: %d, want: %d", off, 0)
			}
			read, err := log.ReadBatch(off)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(append, read) {
				t.Fatalf("got read: %v, want: %v", read, append)
			}
		},
		"offset out of range error": func(t *testing.T, log *Log) {
			read, err := log.ReadBatch(0)
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
	} {
		t.Run(scenario, func(t *testing.T) {
			base, err := ioutil.TempDir("", "log-test")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(base)
			log := &Log{Dir: base}
			fn(t, log)
		})
	}
}
