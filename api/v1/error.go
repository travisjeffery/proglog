package log_v1

import grpc "google.golang.org/grpc"

var (
	ErrOffsetOutOfRange = grpc.Errorf(404, "offset out of range")
)
