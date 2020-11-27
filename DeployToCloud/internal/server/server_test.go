// START: intro
package server

import (
	"context"
	
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"flag"
	"go.opencensus.io/examples/exporter"

	api "github.com/travisjeffery/proglog/api/v1"
	"github.com/travisjeffery/proglog/internal/auth"
	"github.com/travisjeffery/proglog/internal/config"
	"github.com/travisjeffery/proglog/internal/log"

	"github.com/travisjeffery/go-dynaport"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// debug/TestMain...
// END: intro
var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

// START: intro
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		clients clients,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized fails":                                  testUnauthorized,
		// START_HIGHLIGHT
		"healthcheck succeeds":                                testHealthCheck,
		// END_HIGHLIGHT
	} {
		t.Run(scenario, func(t *testing.T) {
			clients, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, clients, config)
		})
	}
}

type clients struct {
	Root   api.LogClient
	Nobody api.LogClient
	Health healthpb.HealthClient
}
// END: intro

// START: setup
func setupTest(t *testing.T, fn func(*Config)) (
	clients clients,
	cfg *Config,
	teardown func(),
) {
	// START: ca
	t.Helper()

	// START: ports
	ports := dynaport.Get(3)

	rpcAddr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: ports[0]}

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: rpcAddr.IP.String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)

	l, err := net.Listen("tcp", rpcAddr.String())
	require.NoError(t, err)

	dataDir, err := ioutil.TempDir("", "server-test-log")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)

	clog, err := log.NewLog(dataDir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(
		config.ACLModelFile,
		config.ACLPolicyFile,
	)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile: metricsLogFile.Name(),
			TracesLogFile: tracesLogFile.Name(),
			ReportingInterval: time.Second,		
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)		
	}

	// START: config
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	// END: config
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		if err := server.Serve(l); err != nil {
			panic(err)
		}
	}()

	// END: ports

	// START: client_options
	newLogClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile:      crtPath,
			KeyFile:       keyPath,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: rpcAddr.IP.String(),
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, clients.Root, _ = newLogClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, clients.Nobody, _ = newLogClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)
	// END: client_options

	clients.Health = healthpb.NewHealthClient(nobodyConn)

	// START: teardown
	return clients, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()		
			telemetryExporter.Close()
		}
		clog.Remove()
	}
	// END: teardown
}

// END: setup

// START: produceconsume
func testProduceConsume(t *testing.T, clients clients, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := clients.Root.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := clients.Root.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, produce.Offset)
}

// END: produceconsume

// START: consumeerror
func testConsumePastBoundary(
	t *testing.T,
	clients clients,
	config *Config,
) {
	ctx := context.Background()

	produce, err := clients.Root.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := clients.Root.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	require.Nil(t, consume)
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

// END: consumeerror

// START: stream
func testProduceConsumeStream(
	t *testing.T,
	clients clients,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := clients.Root.ProduceStream(ctx)
		require.NoError(t, err)

		for i, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			offset := uint64(i)
			if res.Offset != offset {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}

	}

	{
		stream, err := clients.Root.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

// END: stream

func testUnauthorized(
	t *testing.T,
	clients clients,
	config *Config,
) {
	ctx := context.Background()
	produce, err := clients.Nobody.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := clients.Nobody.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

// START: healthcheck
func testHealthCheck(
	t *testing.T,
	clients clients,
	config *Config,
) {
	ctx := context.Background()
	res, err := clients.Health.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, res.Status)
}
// END: healthcheck
