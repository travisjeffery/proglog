package main

import (
	"net"

	grpclog "github.com/travisjeffery/proglog/internal/grpc"
	"github.com/travisjeffery/proglog/internal/log"
)

var ()

func main() {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	log := &log.Log{}
	srv := grpclog.NewAPI(log)

	srv.Serve(lis)
}
