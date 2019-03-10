package grpc

import (
	"context"
	"crypto/tls"

	api "github.com/travisjeffery/proglog/api/v1"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var _ api.LogServer = (*grpcServer)(nil)

func NewAPI(log logger, opts ...grpc.ServerOption) *grpc.Server {
	m := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache("/tmp/proglog/certs"),
		HostPolicy: nil,
	}
	tls := &tls.Config{GetCertificate: m.GetCertificate}
	creds := credentials.NewTLS(tls)
	gsrv := grpc.NewServer(grpc.Creds(creds))
	srv := newgrpcServer(log)
	api.RegisterLogServer(gsrv, srv)
	return gsrv
}

func newgrpcServer(log logger) *grpcServer {
	return &grpcServer{
		log: log,
	}
}

type grpcServer struct {
	log logger
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.log.AppendBatch(req.RecordBatch)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{FirstOffset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	batch, err := s.log.ReadBatch(req.Offset)
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

type logger interface {
	AppendBatch(*api.RecordBatch) (uint64, error)
	ReadBatch(uint64) (*api.RecordBatch, error)
}
