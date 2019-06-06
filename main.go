package main

import (
	"crypto/tls"
	"flag"
	"net"

	grpclog "github.com/travisjeffery/proglog/internal/grpc"
	"github.com/travisjeffery/proglog/internal/log"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	serfAdvertiseAddr = flag.String("serf_advertise_addr", "", "Address that this server's serf instance listens on.")
	logAPIAddr        = flag.String("api_addr", "", "Address that this server's log api listens on.")
)

func main() {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	log := &log.Log{}
	m := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache("/tmp/proglog/certs"),
		HostPolicy: nil,
	}
	tlsConfig := &tls.Config{GetCertificate: m.GetCertificate}
	tls := credentials.NewTLS(tlsConfig)
	creds := grpc.Creds(tls)
	srv := grpclog.NewAPI(log, creds)
	srv.Serve(lis)
}
