package proglog

import (
	"log"

	"github.com/hashicorp/serf/serf"
)

const rpcAddrKey = "rpc_addr"

func (s *grpcServer) eventHandler() {
	for e := range s.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			s.nodeJoin(e.(serf.MemberEvent))
		case serf.EventMemberLeave, serf.EventMemberFailed:
			s.nodeFailed(e.(serf.MemberEvent))
		}
	}
}

func (s *grpcServer) nodeJoin(e serf.MemberEvent) {
	log.Printf("node join: %v", e)
}

func (s *grpcServer) nodeFailed(e serf.MemberEvent) {
	log.Printf("node failed: %v", e)
}

type serverParts struct {
	rpcAddr string
}

func decodeParts(m serf.Member) *serverParts {
	parts := &serverParts{}
	parts.rpcAddr = m.Tags[rpcAddrKey]
	return parts
}
