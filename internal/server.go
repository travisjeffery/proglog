package proglog

import (
	"context"
	"log"
	"net"
	"sync"

	"github.com/davecgh/go-spew/spew"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/hashicorp/serf/serf"
	api "github.com/travisjeffery/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var _ api.LogServer = (*grpcServer)(nil)

// Config is used to configure the server.
type Config struct {
	NodeName       string
	SerfBindAddr   *net.TCPAddr
	RPCAddr        *net.TCPAddr
	StartJoinAddrs []string
	CommitLog      CommitLog
	ClientOptions  []grpc.DialOption
}

// CommitLog definese the interface the server relies on.
type CommitLog interface {
	AppendBatch(*api.RecordBatch) (uint64, error)
	ReadBatch(uint64) (*api.RecordBatch, error)
}

type grpcServer struct {
	mu          sync.Mutex
	config      *Config
	commitlog   CommitLog
	logger      *log.Logger
	serf        *serf.Serf
	events      chan serf.Event
	replicateCh map[string]chan struct{}
}

// NewAPI creates a new *grpc.Server instance  configured per the given config.
func NewAPI(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
		grpc_auth.StreamServerInterceptor(auth),
	)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(auth),
	)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		config:      config,
		commitlog:   config.CommitLog,
		replicateCh: make(map[string]chan struct{}),
	}
	err = srv.setupSerf()
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (s *grpcServer) eventHandler() {
	for e := range s.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			s.replicate(e.(serf.MemberEvent))
		case serf.EventMemberLeave, serf.EventMemberFailed:
			s.stopReplicating(e.(serf.MemberEvent))
		}
	}
}

func (s *grpcServer) replicate(me serf.MemberEvent) error {
	for _, m := range me.Members {
		// don't replicate from ourself
		rpcAddr := m.Tags["rpc_addr"]
		if rpcAddr == s.config.RPCAddr.String() {
			continue
		}

		cc, err := grpc.Dial(rpcAddr, s.config.ClientOptions...)
		if err != nil {
			return err
		}

		client := api.NewLogClient(cc)
		ctx := context.Background()

		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
			Offset: 0,
		})
		if err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.replicateCh[rpcAddr]; ok {
			// already replicating so skip
			continue
		}
		s.replicateCh[rpcAddr] = make(chan struct{})

		go func() {
			defer cc.Close()

			spew.Dump("replicating", rpcAddr)

			for {
				select {
				case <-s.replicateCh[rpcAddr]:
					break
				default:
					recv, err := stream.Recv()
					if err != nil {
						break
					}
					_, err = s.Produce(ctx, &api.ProduceRequest{
						RecordBatch: recv.RecordBatch,
					})
					if err != nil {
						break
					}
				}

			}
		}()
	}

	return nil
}

func (s *grpcServer) stopReplicating(me serf.MemberEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range me.Members {
		rpcAddr := m.Tags["rpc_addr"]
		if rpcAddr == s.config.RPCAddr.String() {
			continue
		}
		close(s.replicateCh[rpcAddr])
		delete(s.replicateCh, rpcAddr)
	}
}

func (s *grpcServer) setupSerf() (err error) {
	config := serf.DefaultConfig()
	config.Init()
	config.NodeName = s.config.SerfBindAddr.String()
	config.MemberlistConfig.BindAddr = s.config.SerfBindAddr.IP.String()
	config.MemberlistConfig.BindPort = s.config.SerfBindAddr.Port
	config.Tags["rpc_addr"] = s.config.RPCAddr.String()

	s.events = make(chan serf.Event)
	config.EventCh = s.events
	s.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go s.eventHandler()

	if s.config.StartJoinAddrs != nil {
		_, err = s.serf.Join(s.config.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.commitlog.AppendBatch(req.RecordBatch)
	if err != nil {
		return nil, err
	}
	spew.Dump("produce", req, offset, s.config.RPCAddr.String())
	return &api.ProduceResponse{FirstOffset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	batch, err := s.commitlog.ReadBatch(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{RecordBatch: batch}, nil
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		res, err := s.Consume(stream.Context(), req)
		spew.Dump("consume stream", res, err)
		switch err.(type) {
		case api.ErrOffsetOutOfRange:
			// expected err, continue consuming until we get something
			continue
		case nil:
			// succesfully got a msg, send it to the stream
		default:
			// unexpected error
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
		spew.Dump("about to increment req offset", req.Offset)
		req.Offset++
	}
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func auth(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if ok {
		tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
		addr := peer.Addr.String()
		username := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
		log.Printf("auth: %s: %s", addr, username)
	}
	return ctx, nil
}
