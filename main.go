package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"strings"

	proglog "github.com/travisjeffery/proglog/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	serfAddrFlag       = flag.String("serf_addr", "", "Address that this server's serf instance listens on.")
	startJoinAddrsFlag = flag.String("start_join_addrs", "", "Addresses (comma delimited) of existing Serf nodes to join.")
	rpcAddrFlag        = flag.String("rpc_addr", ":0", "Address that this server's RPC log server listens on.")
	caCertFlag         = flag.String("ca_cert", "", "Path to CA cert.")
	clientCertFlag     = flag.String("client_cert", "", "Path to client cert.")
	clientKeyFlag      = flag.String("client_key", "", "Path to client key.")
)

func main() {
	flag.Parse()

	ln, err := net.Listen("tcp", *rpcAddrFlag)
	check(err)

	rawCACert, err := ioutil.ReadFile(*caCertFlag)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rawCACert)

	clientCrt, err := tls.LoadX509KeyPair(*clientCertFlag, *clientKeyFlag)
	check(err)

	tlsCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCrt},
		RootCAs:      caCertPool,
	})

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	config := &proglog.Config{
		CommitLog:     &proglog.Log{},
		ClientOptions: clientOptions,
	}

	if *serfAddrFlag != "" {
		serfAddr, err := net.ResolveTCPAddr("tcp", *serfAddrFlag)
		check(err)
		config.SerfBindAddr = serfAddr
	}

	if *startJoinAddrsFlag != "" {
		config.StartJoinAddrs = strings.Split(*startJoinAddrsFlag, ",")
	}

	srv, err := proglog.NewAPI(config)
	check(err)

	err = srv.Serve(ln)
	check(err)

}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
