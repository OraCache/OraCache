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
		Reason: "NOT_LEADER", Domain: "oracache.shardraft", Metadata: map[string]string{"address": leaderAddr, "term": "7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func forwarderConfig() *cpconfig.Config {
	return &cpconfig.Config{
		Revision: 1, PartitionCount: 1,
		Regions: []cpconfig.Region{{ID: "r", Role: cpconfig.RegionRoleMaster}},
		Nodes: []cpconfig.Node{
			{ID: "n1", Region: "r", Address: "a1"}, {ID: "n2", Region: "r", Address: "a2"}, {ID: "n3", Region: "r", Address: "a3"},
		},
		Partitions: []cpconfig.PartitionAssignment{{PartitionID: 0, Region: "r", LeaderNodeID: "n1", ReplicaNodeIDs: []string{"n1", "n2", "n3"}}},
	}
}

func newForwarder(t *testing.T, nodes map[string]*fakeNode) *gateway.Forwarder {
	t.Helper()
	cfg := forwarderConfig()
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

// New hints win over old success, but an identical hint must not reset a more
// recent traffic-learned success on every request/refresh.
func TestForwarderNewCPHintSupersedesLearnedSuccess(t *testing.T) {
	n1, n2, n3 := &fakeNode{rev: 1}, &fakeNode{rev: 2}, &fakeNode{rev: 3}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": n3})
	put := func(want uint64) {
		t.Helper()
		out, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"})
		if err != nil || out.GetRev() != want {
			t.Fatalf("want %d: %v %v", want, out, err)
		}
	}
	hint := func(id string, term uint64) {
		t.Helper()
		cfg := forwarderConfig()
		a := &cfg.Partitions[0]
		a.LeaderNodeID, a.ObservedTerm = id, term
		a.Ready, a.Readable, a.Writable = true, true, true
		a.ReadyReplicaNodeIDs = a.ReplicaNodeIDs
		if err := f.Router.ApplyView(cfg); err != nil {
			t.Fatal(err)
		}
	}
	put(1) // cache a1 even though all replicas answer
	hint("n2", 2)
	put(2) // old a1 must not hide CP n2
	n2.err = notLeader(t, "a3")
	put(3) // learn n3 after stale CP hint
	hint("n2", 2)
	put(3) // identical CP hint must preserve learned success
	if n2.calls != 2 {
		t.Fatalf("unchanged hint reset success: %d calls", n2.calls)
	}
	n2.err = nil
	hint("n2", 3)
	put(2) // term change alone invalidates n3
	hint("n1", 2)
	if _, err := f.Put(t.Context(), &pb.PutRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("an older term must not become a serving route: %v", err)
	}
	hint("n2", 3)
	put(2)
	if f.Router.Revision() != 1 {
		t.Fatal("hint advanced config revision")
	}
}

func TestForwarderRejectsUnconfiguredCPAndNotLeaderDestinations(t *testing.T) {
	n1, n2 := &fakeNode{err: notLeader(t, "foreign")}, &fakeNode{rev: 2}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": {}})
	view := forwarderConfig()
	view.Partitions[0].ReadyReplicaNodeIDs = []string{"foreign"}
	if err := f.Router.ApplyView(view); err == nil {
		t.Fatal("accepted unknown ready replica")
	}
	original := f.Dial
	var addresses []string
	f.Dial = func(addr string) (pb.CacheGatewayClient, error) {
		addresses = append(addresses, addr)
		if addr == "foreign" {
			t.Fatal("dialed unconfigured NOT_LEADER destination")
		}
		return original(addr)
	}
	out, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"})
	if err != nil || out.GetRev() != 2 {
		t.Fatalf("fallback: %v %v", out, err)
	}
	if len(addresses) != 2 || addresses[0] != "a1" || addresses[1] != "a2" {
		t.Fatalf("unexpected attempts: %v", addresses)
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
	_, err := f.Get(t.Context(), &pb.GetRequest{Key: "k"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
	total := nodes["a1"].calls + nodes["a2"].calls + nodes["a3"].calls
	if total != 5 {
		t.Fatalf("expected exactly MaxAttempts calls, got %d", total)
	}
}

func TestForwarderNeverRetriesAmbiguousWritesButRetriesReads(t *testing.T) {
	for _, code := range []codes.Code{codes.DeadlineExceeded, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			n1 := &fakeNode{err: status.Error(code, "outcome unknown")}
			n2 := &fakeNode{rev: 1}
			f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": {}})
			if _, err := f.Put(t.Context(), &pb.PutRequest{Key: "k"}); status.Code(err) != code || n2.calls != 0 {
				t.Fatalf("write must stop after %s: %v (n2 calls %d)", code, err, n2.calls)
			}
			if g, err := f.Get(t.Context(), &pb.GetRequest{Key: "k"}); err != nil || g.Rev != 1 {
				t.Fatalf("read must fail over: %v %v", g, err)
			}
		})
	}
}

func TestForwarderOnlyTriesReadyReplicasFromLiveView(t *testing.T) {
	n1 := &fakeNode{err: notLeader(t, "a2")}
	n2, n3 := &fakeNode{rev: 2}, &fakeNode{rev: 3}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": n3})
	view := forwarderConfig()
	a := &view.Partitions[0]
	a.ObservedTerm, a.ReadyReplicaNodeIDs = 6, []string{"n1", "n3"}
	// No ready leader in this poll. A ready replica can serve only if its own
	// Raft group authorizes the request. The hint to n2 must be ignored.
	if err := f.Router.ApplyView(view); err != nil {
		t.Fatal(err)
	}
	out, err := f.Put(t.Context(), &pb.PutRequest{Key: "k"})
	if err != nil || out.GetRev() != 3 || n2.calls != 0 {
		t.Fatalf("ready fallback = %v, %v; not-ready replica calls = %d", out, err, n2.calls)
	}
	a.LeaderConflict = true
	if err := f.Router.ApplyView(view); err != nil {
		t.Fatal(err)
	}
	a.LeaderConflict = false
	a.LeaderNodeID, a.ReadyReplicaNodeIDs = "n3", []string{"n3"}
	a.Ready, a.Readable, a.Writable = true, true, true
	if err := f.Router.ApplyView(view); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Get(t.Context(), &pb.GetRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("same-term conflict should remain blocked: %v", err)
	}
	a.ObservedTerm++
	if err := f.Router.ApplyView(view); err != nil {
		t.Fatal(err)
	}
	if out, err := f.Get(t.Context(), &pb.GetRequest{Key: "k"}); err != nil || out.GetRev() != 3 {
		t.Fatalf("higher term should restore the route: %v, %v", out, err)
	}
}

func TestForwarderReturnsApplicationErrorsUnchanged(t *testing.T) {
	n1 := &fakeNode{err: status.Error(codes.InvalidArgument, "bad key")}
	f := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": {}, "a3": {}})
	if _, err := f.Put(context.Background(), &pb.PutRequest{Key: "k"}); status.Code(err) != codes.InvalidArgument || n1.calls != 1 {
		t.Fatalf("got %v after %d calls", err, n1.calls)
	}
}

func TestForwarderTimeoutIsHardCapAndCallerDeadlineStillApplies(t *testing.T) {
	down := func() *fakeNode { return &fakeNode{err: status.Error(codes.Unavailable, "down")} }
	run := func(cap, callerDeadline time.Duration) time.Duration {
		f := newForwarder(t, map[string]*fakeNode{"a1": down(), "a2": down(), "a3": down()})
		f.MaxAttempts, f.Backoff, f.Timeout = 1_000_000, 5*time.Millisecond, cap
		ctx, cancel := context.WithTimeout(context.Background(), callerDeadline)
		defer cancel()
		start := time.Now()
		if _, err := f.Get(ctx, &pb.GetRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
			t.Fatalf("want Unavailable, got %v", err)
		}
		return time.Since(start)
	}
	// Caller allows 30s but the gateway cap is 150ms: the cap wins.
	if d := run(150*time.Millisecond, 30*time.Second); d < 140*time.Millisecond || d > 2*time.Second {
		t.Fatalf("cap not honored, took %v", d)
	}
	// Caller deadline 100ms is earlier than the 30s cap: the caller wins.
	if d := run(30*time.Second, 100*time.Millisecond); d < 90*time.Millisecond || d > 2*time.Second {
		t.Fatalf("caller deadline not honored, took %v", d)
	}
}
