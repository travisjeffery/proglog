package log

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a batch succeeds": func(t *testing.T, log *Log) {
			append := &log_v1.RecordBatch{
				Records: []*log_v1.Record{{
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
