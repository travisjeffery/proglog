package proglog

import (
	"context"
	"log"
	"sync"

	api "github.com/travisjeffery/proglog/api/v1"
	"google.golang.org/grpc"
)

type replicator struct {
	// clientOptions are the options to configure the connection to the other servers.
	clientOptions []grpc.DialOption
	// servers is the list of servers currently being replicated, close the associated channel
	// to stop replicating.
	servers map[string]chan struct{}
	// mu locks the replicator for modification
	mu sync.Mutex
	// produce is the function the replicator calls to replicate.
	produce func(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error)
}

func (r *replicator) Add(addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[addr]; ok {
		// already replicating so skip
		return nil
	}
	r.servers[addr] = make(chan struct{})

	go r.add(addr)

	return nil
}

func (r *replicator) add(addr string) {
	cc, err := grpc.Dial(addr, r.clientOptions...)
	if err != nil {
		r.err(err)
		return
	}

	client := api.NewLogClient(cc)
	ctx := context.Background()

	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		r.err(err)
		return
	}

	defer cc.Close()

loop:
	for {
		select {
		case <-r.servers[addr]:
			break loop
		default:
			recv, err := stream.Recv()
			if err != nil {
				r.err(err)
				return
			}
			_, err = r.produce(ctx, &api.ProduceRequest{
				RecordBatch: recv.RecordBatch,
			})
			if err != nil {
				r.err(err)
				return
			}
		}

	}
}

func (r *replicator) Remove(addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[addr]; !ok {
		return nil
	}
	close(r.servers[addr])
	delete(r.servers, addr)
	return nil
}

func (r *replicator) err(err error) {
	log.Printf("[ERROR] proglog: %v", err)
}

func (r *replicator) init() {
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
}
