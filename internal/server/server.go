// START: types
package server

import (
	"context"

	api "github.com/travisjeffery/proglog/api/v1"
	"google.golang.org/grpc"
)

var _ api.LogServer = (*grpcServer)(nil)

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// END: types

// START: newapi
func NewAPI(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// END: newapi

// START: request_response
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.RecordBatch)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{FirstOffset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	batch, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{RecordBatch: batch}, nil
}

// END: request_response

// START: stream
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

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		res, err := s.Consume(stream.Context(), req)
		switch err.(type) {
		case nil:
		case api.ErrOffsetOutOfRange:
			continue
		default:
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
		req.Offset++
	}
}

// END: stream

// START: commitlog
type CommitLog interface {
	Append(*api.RecordBatch) (uint64, error)
	Read(uint64) (*api.RecordBatch, error)
}

// END: commitlog
