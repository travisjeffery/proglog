package proglog

import (
	"context"
	"log"
	"net"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/hashicorp/serf/serf"
	api "github.com/travisjeffery/proglog/api/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var _ api.LogServer = (*grpcServer)(nil)

type Config struct {
	NodeName       string
	SerfBindAddr   *net.TCPAddr
	StartJoinAddrs []string
	RPCAddr        *net.TCPAddr
	CommitLog      CommitLog
	ClientOptions  []grpc.DialOption
}

type grpcServer struct {
	config    *Config
	commitlog CommitLog
	logger    *log.Logger
	serf      *serf.Serf
	events    chan serf.Event
}

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
		config:    config,
		commitlog: config.CommitLog,
	}
	err = srv.setupSerf()
	if err != nil {
		return nil, err
	}
	return srv, nil
}

func (s *grpcServer) eventHandler() {
	names := func(e serf.MemberEvent) string {
		names := []string{}
		for _, m := range e.Members {
			names = append(names, m.Name)
		}
		return strings.Join(names, ", ")
	}
	for e := range s.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			log.Printf("[DEBUG] proglog: handled node join event: %v", names(e.(serf.MemberEvent)))
		case serf.EventMemberLeave, serf.EventMemberFailed:
			log.Printf("[DEBUG] proglog: handled node failed event: %v", names(e.(serf.MemberEvent)))
		}
	}
}

func (s *grpcServer) setupSerf() (err error) {
	config := serf.DefaultConfig()
	config.Init()
	config.NodeName = s.config.SerfBindAddr.String()
	config.MemberlistConfig.BindAddr = s.config.SerfBindAddr.IP.String()
	config.MemberlistConfig.BindPort = s.config.SerfBindAddr.Port
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
	res := &api.ProduceResponse{FirstOffset: offset}
	if err = s.replicateProduce(ctx, req); err != nil {
		return res, err
	}
	return res, nil
}

func (s *grpcServer) replicateProduce(ctx context.Context, req *api.ProduceRequest) error {
	if s.serf == nil {
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, member := range s.serf.Members() {
		rpcAddr := member.Tags["rpc_addr"]
		if rpcAddr == s.config.RPCAddr.String() {
			// ignore the member of the current server
			continue
		}
		g.Go(func() error {
			// TODO(tj): optimize this

			cc, err := grpc.Dial(rpcAddr, s.config.ClientOptions...)
			if err != nil {
				return err
			}
			defer cc.Close()

			client := api.NewLogClient(cc)

			_, err = client.Produce(ctx, req)
			if err != nil {
				return err
			}

			return nil
		})
	}
	return g.Wait()
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
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
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

type CommitLog interface {
	AppendBatch(*api.RecordBatch) (uint64, error)
	ReadBatch(uint64) (*api.RecordBatch, error)
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
