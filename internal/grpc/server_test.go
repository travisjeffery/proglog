package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os/user"
	"path/filepath"
	"reflect"
	"testing"

	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, srv *grpc.Server, client api.LogClient){
		"consume empty log fails":                             testConsumeEmpty,
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			check(t, err)

			rawCACert, err := ioutil.ReadFile(caCrt)
			if err != nil {
				t.Fatal(err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(rawCACert)

			ccreds := credentials.NewTLS(&tls.Config{
				RootCAs: caCertPool,
			})

			cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(ccreds))
			check(t, err)
			defer cc.Close()

			tls, err := credentials.NewServerTLSFromFile(serverCrt, serverKey)
			check(t, err)
			screds := grpc.Creds(tls)

			dir, err := ioutil.TempDir("", "server-test")
			check(t, err)

			srv := NewAPI(&log.Log{Dir: dir}, screds)

			go func() {
				srv.Serve(l)
			}()
			defer func() {
				srv.Stop()
				l.Close()
			}()

			client := api.NewLogClient(cc)

			fn(t, srv, client)
		})
	}
}

func testConsumeEmpty(t *testing.T, srv *grpc.Server, client api.LogClient) {
	consume, err := client.Consume(context.Background(), &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("got consume: %v, want: nil", consume)
	}
	got, want := grpc.Code(err), grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got code: %v, want: %v, err: %v", got, want, err)
	}
}

func testProduceConsume(t *testing.T, srv *grpc.Server, client api.LogClient) {
	ctx := context.Background()

	want := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}

	produce, err := client.Produce(context.Background(), &api.ProduceRequest{
		RecordBatch: want,
	})
	check(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	check(t, err)
	equal(t, consume.RecordBatch, want)
}

func testConsumePastBoundary(t *testing.T, srv *grpc.Server, client api.LogClient) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		RecordBatch: &api.RecordBatch{
			Records: []*api.Record{{
				Value: []byte("hello world"),
			}},
		},
	})
	check(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got, want := grpc.Code(err), grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, srv *grpc.Server, client api.LogClient) {
	ctx := context.Background()

	batches := []*api.RecordBatch{{
		Records: []*api.Record{{
			Value: []byte("first message"),
		}},
	}, {
		Records: []*api.Record{{
			Value: []byte("second message"),
		}},
	}}

	{
		stream, err := client.ProduceStream(ctx)
		check(t, err)

		for offset, batch := range batches {
			err = stream.Send(&api.ProduceRequest{
				RecordBatch: batch,
			})
			check(t, err)
			res, err := stream.Recv()
			check(t, err)
			if res.FirstOffset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.FirstOffset, offset)
			}
		}

	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		check(t, err)

		for _, batch := range batches {
			res, err := stream.Recv()
			check(t, err)
			equal(t, res.RecordBatch, batch)
		}
	}
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func equal(t *testing.T, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got: %v, want %v", got, want)
	}
}

var (
	caCrt     = configFile("ca.pem")
	serverCrt = configFile("server.pem")
	serverKey = configFile("server-key.pem")
)

func configFile(filename string) string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return filepath.Join(u.HomeDir, ".proglog", filename)
}
