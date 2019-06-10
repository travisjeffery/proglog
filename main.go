package main

import (
	"flag"
	"log"
	"net"

	"github.com/hashicorp/serf/serf"
	proglog "github.com/travisjeffery/proglog/internal"
)

var (
	serfAddr   = flag.String("serf_addr", "", "Address that this server's serf instance listens on.")
	logAPIAddr = flag.String("api_addr", ":0", "Address that this server's log api listens on.")
)

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", *logAPIAddr)
	if err != nil {
		panic(err)
	}

	config := &proglog.Config{
		CommitLog: &proglog.Log{},
	}

	if *serfAddr != "" {
		addr, err := net.ResolveTCPAddr("tcp", *serfAddr)
		if err != nil {
			log.Fatal(err)
		}
		config.SerfConfig = serf.DefaultConfig()
		config.SerfConfig.MemberlistConfig.BindPort = addr.Port
		config.SerfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	}

	srv, err := proglog.NewAPI(config)
	if err != nil {
		log.Fatal(err)
	}

	err = srv.Serve(lis)
	if err != nil {
		log.Fatal(err)
	}

}
