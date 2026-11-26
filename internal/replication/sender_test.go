package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/replication/v1"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

type testSource struct {
	mu       sync.Mutex
	queue    []uint64
	value    string
	ackCalls int
}

func TestSenderUpdatesCPRoutesAndReleasesOldConnections(t *testing.T) {
	routes := []string{"cp-old:1234"}
	sender, err := NewSender(&testSource{}, "a", "b", "stream", 0, func() []string { return routes }, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	old := sender.clients["cp-old:1234"].conn
	routes = []string{"cp-first:1234", "cp-second:1234", "cp-first:1234"}
	clients, err := sender.resolveClients()
	if err != nil || len(clients) != 2 || len(sender.clients) != 2 {
		t.Fatal(clients, err)
	}
	if old.GetState() != connectivity.Shutdown {
		t.Fatal("obsolete CP connection was not closed")
	}
	first := sender.clients["cp-first:1234"].conn
	routes = []string{"cp-second:1234", "cp-first:1234"}
	if _, err := sender.resolveClients(); err != nil {
		t.Fatal(err)
	}
	if sender.clients["cp-first:1234"].conn != first {
		t.Fatal("unchanged address must reuse connection")
	}
	routes = nil
	clients, err = sender.resolveClients()
	if err != nil || len(clients) != 0 || len(sender.clients) != 0 {
		t.Fatal("missing CP route must pause delivery", clients, err)
	}
	if first.GetState() != connectivity.Shutdown {
		t.Fatal("removed CP connection was not closed")
	}
	sender.Close()
	if _, err := sender.resolveClients(); status.Code(err) != codes.Unavailable {
		t.Fatal("closed sender reopened connections", err)
	}
}

func (*testSource) Status() shardraft.Status {
	return shardraft.Status{Running: true, Ready: true, Role: "StateLeader"}
}
func (s *testSource) ReadOutbox(context.Context) (*pb.ReadOutboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &pb.ReadOutboxResponse{}
	if len(s.queue) != 0 {
		out.Entry = &pb.Entry{Id: s.queue[0], Key: "k", Versions: []*pb.Version{{Rev: 9, Data: []byte(s.value)}}}
	}
	return out, nil
}
func (s *testSource) AckOutbox(_ context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 || s.queue[0] != id {
		return fmt.Errorf("wrong ACK %d", id)
	}
	s.queue = s.queue[1:]
	s.ackCalls++
	if s.ackCalls == 1 {
		return status.Error(codes.DeadlineExceeded, "ACK committed, response lost")
	}
	return nil
}

type lossyReceiver struct {
	pb.UnimplementedReplicationServiceServer
	mu      sync.Mutex
	source  *testSource
	applied uint64
	value   string
	calls   []uint64
}

func (r *lossyReceiver) Replicate(_ context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := req.Entry.Id
	r.calls = append(r.calls, id)
	if id > r.applied+1 {
		return nil, status.Error(codes.FailedPrecondition, "gap")
	}
	if id == r.applied+1 {
		r.applied = id
		r.value = string(req.Entry.Versions[0].Data)
		if id == 1 {
			// Concurrent Put while the old key snapshot is being delivered.
			r.source.mu.Lock()
			r.source.queue = append(r.source.queue, 2)
			r.source.value = "new"
			r.source.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "destination applied, response lost")
		}
	}
	return &pb.ReplicateResponse{AppliedId: r.applied}, nil
}

func TestSenderLostDeliveryAndAckResponsesDoNotLoseConcurrentPut(t *testing.T) {
	source := &testSource{queue: []uint64{1}, value: "old"}
	receiver := &lossyReceiver{source: source}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterReplicationServiceServer(server, receiver)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	sender, err := NewSender(source, "a", "b", "a/0/1", 0, func() []string { return []string{listener.Addr().String()} }, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { sender.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		source.mu.Lock()
		empty := len(source.queue) == 0
		source.mu.Unlock()
		if empty {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("sender did not drain outbox")
		}
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.applied != 2 || receiver.value != "new" || len(receiver.calls) < 3 || receiver.calls[0] != 1 || receiver.calls[1] != 1 {
		t.Fatalf("lost update or no retry: applied=%d value=%s calls=%v", receiver.applied, receiver.value, receiver.calls)
	}
}
