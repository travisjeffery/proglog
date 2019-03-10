package log_v1

import (
	"fmt"

	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	return status.New(404, fmt.Sprintf("offset out of range: %d", e.Offset))
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
