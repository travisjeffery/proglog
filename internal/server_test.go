package proglog

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	req "github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"consume empty log fails":                             testConsumeEmpty,
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"replication succeeds":                                testReplication,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := testSetup(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testConsumeEmpty(t *testing.T, client api.LogClient, config *Config) {
	consume, err := client.Consume(context.Background(), &api.ConsumeRequest{
		Offset: 0,
	})
	req.Nil(t, consume)
	req.Equal(t, grpc.Code(err), grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err()))
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}

	produce, err := client.Produce(context.Background(), &api.ProduceRequest{
		RecordBatch: want,
	})
	req.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	req.NoError(t, err)
	req.Equal(t, want, consume.RecordBatch)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
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
	got, want := grpc.Code(err), grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
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
				t.Fatalf("got offset: %d, want: %d", res.FirstOffset, offset)
			}
		}

	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		req.NoError(t, err)

		for _, batch := range batches {
			res, err := stream.Recv()
			req.NoError(t, err)
			req.Equal(t, res.RecordBatch, batch)
		}
	}
}

func testReplication(t *testing.T, client1 api.LogClient, config1 *Config) {
	client2, _, teardown2 := testSetup(t, func(config *Config) {
		config.StartJoinAddrs = []string{config1.SerfBindAddr.String()}
	})
	defer teardown2()

	ctx := context.Background()

	want := &api.RecordBatch{
		Records: []*api.Record{{
			Value: []byte("hello world"),
		}},
	}

	produce, err := client1.Produce(ctx, &api.ProduceRequest{
		RecordBatch: want,
	})
	req.NoError(t, err)

	consume, err := client1.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	req.NoError(t, err)
	req.Equal(t, consume.RecordBatch, want)

	time.Sleep(250 * time.Millisecond)

	consume, err = client2.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.FirstOffset,
	})
	req.NoError(t, err)
	req.Equal(t, consume.RecordBatch, want)
}

func testSetup(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	config *Config,
	teardown func(),
) {
	t.Helper()

	ports := dynaport.Get(2)

	rpcAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: ports[0]}
	serfAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: ports[1]}

	l, err := net.Listen("tcp", rpcAddr.String())
	req.NoError(t, err)

	rawCACert, err := ioutil.ReadFile(caCrt)
	req.NoError(t, err)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rawCACert)

	clientCrt, err := tls.LoadX509KeyPair(clientCrt, clientKey)
	req.NoError(t, err)

	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCrt},
		RootCAs:      caCertPool,
	})

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	req.NoError(t, err)

	serverCrt, err := tls.LoadX509KeyPair(serverCrt, serverKey)
	req.NoError(t, err)

	tlsCreds = credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCrt},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	})

	dir, err := ioutil.TempDir("", "server-test")
	req.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	req.NoError(t, err)

	config = &Config{
		RPCAddr:       rpcAddr,
		SerfBindAddr:  serfAddr,
		CommitLog:     clog,
		ClientOptions: clientOptions,
	}
	if fn != nil {
		fn(config)
	}
	server, err := NewAPI(config, grpc.Creds(tlsCreds))
	req.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, config, func() {
		server.Stop()
		cc.Close()
		l.Close()
	}
}

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
