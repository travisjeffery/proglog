package server

import (
	"context"
	"net"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/travisjeffery/proglog/api/v1"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	conn, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	c := api.NewLogClient(conn)

	config := &Config{
		CommitLog: make(mocklog),
	}
	srv, err := NewAPI(config)
	require.NoError(t, err)

	go func() {
		srv.Serve(l)
	}()

	defer func() {
		srv.Stop()
		l.Close()
	}()

	want := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}

	ctx := context.Background()
	produce, err := c.Produce(ctx, &api.ProduceRequest{
		RecordBatch: want,
	})
	require.NoError(t, err)

	consume, err := c.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	require.NoError(t, err)

	got := consume.RecordBatch

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("API.Produce/Consume, got: %v, want %v", got, want)
	}
}

type mocklog map[uint64]*api.RecordBatch

func (m mocklog) AppendBatch(b *api.RecordBatch) (uint64, error) {
	off := uint64(len(m))
	m[off] = b
	return off, nil
}

func (m mocklog) ReadBatch(off uint64) (*api.RecordBatch, error) {
	return m[off], nil
}
