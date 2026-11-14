package gateway_test

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/gateway"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeNode scripts the answer of one storage node address.
type fakeNode struct {
	pb.CacheGatewayClient
	mu    sync.Mutex
	calls int
	err   error
	rev   uint64
}

func (f *fakeNode) Put(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_OK, Rev: f.rev}, nil
}

func (f *fakeNode) Get(ctx context.Context, r *pb.GetRequest, o ...grpc.CallOption) (*pb.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: f.rev}, nil
}

func notLeader(t *testing.T, leaderAddr string) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&errdetails.ErrorInfo{
		Reason: "NOT_LEADER", Domain: "oracache.shardraft", Metadata: map[string]string{"address": leaderAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func newForwarder(t *testing.T, nodes map[string]*fakeNode) *gateway.Forwarder {
	t.Helper()
	cfg := &cpconfig.Config{
		Revision: 1, PartitionCount: 1,
		Regions: []cpconfig.Region{{ID: "r", Role: cpconfig.RegionRoleMaster}},
		Nodes: []cpconfig.Node{
			{ID: "n1", Region: "r", Address: "a1"}, {ID: "n2", Region: "r", Address: "a2"}, {ID: "n3", Region: "r", Address: "a3"},
		},
		Partitions: []cpconfig.PartitionAssignment{{PartitionID: 0, Region: "r", LeaderNodeID: "n1", ReplicaNodeIDs: []string{"n1", "n2", "n3"}}},
	}
	router, err := gateway.NewRouter("r")
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	return &gateway.Forwarder{
		Router: router, Backoff: time.Millisecond, Timeout: 5 * time.Second,
		Dial: func(addr string) (pb.CacheGatewayClient, error) { return nodes[addr], nil },
	}
}

func TestForwarderFollowsNotLeaderHintAndRemembersLeader(t *testing.T) {
	n1 := &fakeNode{err: notLeader(t, "a3")} // CP says n1, Raft says n3
	n2 := &fakeNode{}
	n3 := &fakeNode{rev: 7}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": n3})
	for i := 0; i < 3; i++ {
		r, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"})
		if err != nil || r.Rev != 7 || r.ServedBy.GetInstance() != "n3" {
			t.Fatalf("Put: %v %v", r, err)
		}
	}
	if n1.calls != 1 || n2.calls != 0 || n3.calls != 3 {
		t.Fatalf("hint must be followed once, then the learned leader reused: %d/%d/%d", n1.calls, n2.calls, n3.calls)
	}
}

func TestForwarderFailsOverFromUnavailableLeader(t *testing.T) {
	n1 := &fakeNode{err: status.Error(codes.Unavailable, "connection refused")}
	n2 := &fakeNode{err: notLeader(t, "")} // election still running: no hint
	n3 := &fakeNode{rev: 3}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": n3})
	g, err := f.Get(context.Background(), &pb.GetRequest{Key: "k"})
	if err != nil || g.Rev != 3 {
		t.Fatalf("Get: %v %v", g, err)
	}
}

func TestForwarderIsBounded(t *testing.T) {
	down := func() *fakeNode { return &fakeNode{err: status.Error(codes.Unavailable, "down")} }
	nodes := map[string]*fakeNode{"a1": down(), "a2": down(), "a3": down()}
	f := newForwarder(t, nodes)
	f.MaxAttempts = 5
	_, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
	total := nodes["a1"].calls + nodes["a2"].calls + nodes["a3"].calls
	if total != 5 {
		t.Fatalf("expected exactly MaxAttempts calls, got %d", total)
	}
}

func TestForwarderNeverRetriesAmbiguousWritesButRetriesReads(t *testing.T) {
	n1 := &fakeNode{err: status.Error(codes.DeadlineExceeded, "slow")}
	n2 := &fakeNode{rev: 1}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": &fakeNode{}})
	if _, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"}); status.Code(err) != codes.DeadlineExceeded || n2.calls != 0 {
		t.Fatalf("write must stop at DeadlineExceeded: %v (n2 calls %d)", err, n2.calls)
	}
	if g, err := f.Get(context.Background(), &pb.GetRequest{Key: "k"}); err != nil || g.Rev != 1 {
		t.Fatalf("read must fail over: %v %v", g, err)
	}
}

func TestForwarderReturnsApplicationErrorsUnchanged(t *testing.T) {
	n1 := &fakeNode{err: status.Error(codes.InvalidArgument, "bad key")}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": {}, "a3": {}})
	if _, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"}); status.Code(err) != codes.InvalidArgument || n1.calls != 1 {
		t.Fatalf("got %v after %d calls", err, n1.calls)
	}
}
