package main

import (
	"flag"
	"log"
	"net"
	"strings"

	proglog "github.com/travisjeffery/proglog/internal"
)

var (
	serfAddrFlag       = flag.String("serf_addr", "", "Address that this server's serf instance listens on.")
	startJoinAddrsFlag = flag.String("start_join_addrs", "", "Addresses (comma delimited) of existing Serf nodes to join.")
	logAPIAddrFlag     = flag.String("api_addr", ":0", "Address that this server's log api listens on.")
)

func main() {
	flag.Parse()

	ln, err := net.Listen("tcp", *logAPIAddrFlag)
	if err != nil {
		panic(err)
	}

	config := &proglog.Config{
		CommitLog: &proglog.Log{},
	}

	if *serfAddrFlag != "" {
		serfAddr, err := net.ResolveTCPAddr("tcp", *serfAddrFlag)
		if err != nil {
			log.Fatal(err)
		}
		config.SerfBindAddr = serfAddr
	}

	if *startJoinAddrsFlag != "" {
		config.StartJoinAddrs = strings.Split(*startJoinAddrsFlag, ",")
	}

	srv, err := proglog.NewAPI(config)
	if err != nil {
		log.Fatal(err)
	}

	err = srv.Serve(ln)
	if err != nil {
		log.Fatal(err)
	}

}
