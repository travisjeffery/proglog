// START: intro
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os/user"
	"path/filepath"
	"testing"

	req "github.com/stretchr/testify/require"
	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := testSetup(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

// END: intro

// START: setup
func testSetup(t *testing.T, fn func(*Config)) (
	api.LogClient,
	*Config,
	func(),
) {
	t.Helper()

	// START: ca
	l, err := net.Listen("tcp", "127.0.0.1:0")
	req.NoError(t, err)

	rawCACert, err := ioutil.ReadFile(caCrt)
	if err != nil {
		t.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rawCACert)

	crt, err := tls.LoadX509KeyPair(clientCrt, clientKey)
	req.NoError(t, err)
	clientCreds := credentials.NewTLS(&tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{crt},
	})
	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	req.NoError(t, err)

	client := api.NewLogClient(cc)
	// END: ca

	// START: tls
	crt, err = tls.LoadX509KeyPair(serverCrt, serverKey)
	req.NoError(t, err)
	tlsCreds := credentials.NewTLS(&tls.Config{
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{crt},
	})

	dir, err := ioutil.TempDir("", "server-test")
	req.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	req.NoError(t, err)

	config := &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(config)
	}
	server, err := NewAPI(config, grpc.Creds(tlsCreds))
	req.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, config, func() {
		server.Stop()
		cc.Close()
		l.Close()
	}
	// END: tls
}

// END: setup

// START: produceconsume
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}

	produce, err := client.Produce(
		context.Background(),
		&api.ProduceRequest{
			RecordBatch: want,
		},
	)
	req.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	req.NoError(t, err)
	req.Equal(t, want, consume.RecordBatch)
}

// END: produceconsume

// START: consumeerror
func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		RecordBatch: &api.RecordBatch{
			Records: []*api.Record{{
				Value: []byte("hello world"),
			}},
		},
	})
	req.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

// END: consumeerror

// START: stream
func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
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
		req.NoError(t, err)

		for offset, batch := range batches {
			err = stream.Send(&api.ProduceRequest{
				RecordBatch: batch,
			})
			req.NoError(t, err)
			res, err := stream.Recv()
			req.NoError(t, err)
			if res.FirstOffset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.FirstOffset,
					offset,
				)
			}
		}

	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		req.NoError(t, err)

		for _, batch := range batches {
			res, err := stream.Recv()
			req.NoError(t, err)
			req.Equal(t, res.RecordBatch, batch)
		}
	}
}

// END: stream

// START: config

var (
	caCrt     = configFile("ca.pem")
	serverCrt = configFile("server.pem")
	serverKey = configFile("server-key.pem")
	clientCrt = configFile("client.pem")
	clientKey = configFile("client-key.pem")
)

func configFile(filename string) string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return filepath.Join(u.HomeDir, ".proglog", filename)
}

// END: config
