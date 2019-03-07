package main

import (
	"crypto/tls"
	"net"

	grpclog "github.com/travisjeffery/proglog/internal/grpc"
	"github.com/travisjeffery/proglog/internal/log"
	"golang.org/x/crypto/acme/autocert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	host     = ""
	cacheDir = ""
)

func main() {
	m := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(cacheDir),
		HostPolicy: autocert.HostWhitelist(host),
	}
	tls := &tls.Config{GetCertificate: m.GetCertificate}
	creds := credentials.NewTLS(tls)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	log := &log.Log{}
	srv := grpclog.NewAPI(log, grpc.Creds(creds))

	srv.Serve(lis)
}
