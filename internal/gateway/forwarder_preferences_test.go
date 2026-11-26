package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func preferenceConfig() *cpconfig.Config {
	return &cpconfig.Config{Revision: 1, PartitionCount: 1,
		Regions:    []cpconfig.Region{{ID: "r", Role: cpconfig.RegionRoleMaster}},
		Nodes:      []cpconfig.Node{{ID: "n1", Region: "r", Address: "a1"}, {ID: "n2", Region: "r", Address: "a2"}, {ID: "n3", Region: "r", Address: "a3"}},
		Partitions: []cpconfig.PartitionAssignment{{Region: "r", LeaderNodeID: "n1", ReplicaNodeIDs: []string{"n1", "n2", "n3"}}},
	}
}

func preferenceRouter(t *testing.T) *Router {
	t.Helper()
	r, _ := NewRouter("r")
	if err := r.Apply(preferenceConfig()); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestForwarderLateOldSuccessCannotEraseNewPreference(t *testing.T) {
	r := preferenceRouter(t)
	f := &Forwarder{Router: r, Backoff: time.Millisecond, Dial: func(string) (pb.CacheGatewayClient, error) { return nil, nil }}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	oldDone := make(chan error, 1)
	go func() {
		defer close(oldDone)
		oldDone <- f.forward(context.Background(), 0, true, "put", func(context.Context, pb.CacheGatewayClient, string) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	// Always release the old request, even if the assertion below fails.
	defer func() {
		unblock()
		if err := <-oldDone; err != nil {
			t.Error(err)
		}
	}()
	view := preferenceConfig()
	a := &view.Partitions[0]
	a.LeaderNodeID, a.ObservedTerm = "n2", 2
	a.Ready, a.Readable, a.Writable = true, true, true
	a.ReadyReplicaNodeIDs = a.ReplicaNodeIDs
	if err := r.ApplyView(view); err != nil {
		t.Fatal(err)
	}
	if err := f.forward(context.Background(), 0, true, "put", func(_ context.Context, _ pb.CacheGatewayClient, addr string) error {
		if addr == "a2" {
			st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(&errdetails.ErrorInfo{Reason: "NOT_LEADER", Domain: "oracache.shardraft", Metadata: map[string]string{"address": "a3", "term": "3"}})
			return st.Err()
		}
		if addr != "a3" {
			return status.Error(codes.Unavailable, "not serving")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Complete the old in-flight success after learning under the newer token.
	unblock()
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	var first string
	if err := f.forward(context.Background(), 0, true, "put", func(_ context.Context, _ pb.CacheGatewayClient, addr string) error { first = addr; return nil }); err != nil {
		t.Fatal(err)
	}
	if first != "a3" {
		t.Fatalf("late old success erased newer learned preference: first %q, want a3", first)
	}
}

func TestForwarderHintsCannotCrossConfiguredRegions(t *testing.T) {
	cfg := preferenceConfig()
	cfg.Regions = append(cfg.Regions, cpconfig.Region{ID: "s", Role: cpconfig.RegionRoleSlave})
	cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: "s1", Region: "s", Address: "b1"}, cpconfig.Node{ID: "s2", Region: "s", Address: "b2"})
	cfg.Partitions = append(cfg.Partitions, cpconfig.PartitionAssignment{Region: "s", LeaderNodeID: "s1", ReplicaNodeIDs: []string{"s1", "s2"}})
	r, _ := NewRouter("s")
	if err := r.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		write                  bool
		first, second, foreign string
	}{{true, "a1", "a2", "b1"}, {false, "b1", "b2", "a1"}} {
		f := &Forwarder{Router: r, Dial: func(addr string) (pb.CacheGatewayClient, error) {
			if addr == tc.foreign {
				t.Errorf("cross-region destination %s", addr)
			}
			return nil, nil
		}}
		var calls []string
		if err := f.forward(context.Background(), 0, tc.write, "test", func(_ context.Context, _ pb.CacheGatewayClient, addr string) error {
			calls = append(calls, addr)
			if len(calls) == 1 {
				st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(&errdetails.ErrorInfo{Reason: "NOT_LEADER", Domain: "oracache.shardraft", Metadata: map[string]string{"address": tc.foreign}})
				return st.Err()
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(calls) != 2 || calls[0] != tc.first || calls[1] != tc.second {
			t.Fatalf("write=%t: attempts %v", tc.write, calls)
		}
	}
}

func TestForwarderRejectsUnconfiguredLearnedPreference(t *testing.T) {
	r := preferenceRouter(t)
	route, token, err := r.forwardingRoute(0, true)
	if err != nil {
		t.Fatal(err)
	}
	f := &Forwarder{Router: r, preferred: map[forwardRoute]preference{{partition: 0, region: route.region}: {address: "foreign", token: token}}, Dial: func(addr string) (pb.CacheGatewayClient, error) {
		if addr == "foreign" {
			t.Error("dialed unconfigured preference")
		}
		return nil, nil
	}}
	var first string
	if err := f.forward(context.Background(), 0, true, "put", func(_ context.Context, _ pb.CacheGatewayClient, addr string) error { first = addr; return nil }); err != nil {
		t.Fatal(err)
	}
	if first != "a1" {
		t.Fatalf("first %q", first)
	}
}
