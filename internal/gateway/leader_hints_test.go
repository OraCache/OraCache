package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"

	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/leaderhint"
	"google.golang.org/grpc"
)

func TestLeaderHintsPreserveTermAcrossConfigAndCPRestart(t *testing.T) {
	r, _ := NewRouter("us")
	if err := r.Apply(cpconfigtest.Config(7)); err != nil {
		t.Fatal(err)
	}
	apply := func(id, region string, term uint64) error {
		return r.ApplyLeaderHints([]*pb.LeaderHint{{Region: region, PartitionId: 0, NodeId: id, Term: term}})
	}
	check := func(want string) {
		t.Helper()
		l, e := r.WriteLeader(0)
		if e != nil || l.NodeID != want {
			t.Fatalf("leader %v %v want %s", l, e, want)
		}
		rs, e := r.Replicas(0, true)
		if e != nil || len(rs) != 2 {
			t.Fatalf("fallback %v %v", rs, e)
		}
	}
	if err := apply("eu-2", "eu", 5); err != nil {
		t.Fatal(err)
	}
	check("eu-2")
	if err := apply("eu-1", "eu", 4); err != nil {
		t.Fatal(err)
	}
	check("eu-2")
	if err := r.ApplyLeaderHints(nil); err != nil {
		t.Fatal(err)
	}
	check("eu-2")
	if err := r.Apply(cpconfigtest.Config(8)); err != nil {
		t.Fatal(err)
	}
	check("eu-2")
	if err := apply("eu-1", "eu", 5); err != nil {
		t.Fatal(err)
	}
	check("eu-1") // static on conflict
	if err := apply("eu-2", "eu", 5); err != nil {
		t.Fatal(err)
	}
	check("eu-1") // sticky
	if err := apply("eu-2", "eu", 6); err != nil {
		t.Fatal(err)
	}
	check("eu-2")
	for _, h := range []*pb.LeaderHint{nil, {Region: "eu", PartitionId: 0, NodeId: "us-1", Term: 99}, {Region: "eu", PartitionId: 0, NodeId: "foreign", Term: 99}, {Region: "eu", PartitionId: 0, NodeId: "eu-1", Term: 0}} {
		if err := r.ApplyLeaderHints([]*pb.LeaderHint{h}); err == nil {
			t.Fatalf("accepted %v", h)
		}
		check("eu-2")
	}
	if err := apply("us-2", "us", 3); err != nil {
		t.Fatal(err)
	}
	l, e := r.GetLeader(0)
	if e != nil || l.NodeID != "us-2" {
		t.Fatalf("local %v %v", l, e)
	}
	check("eu-2")
}

func TestPollerInvalidHintsPreserveConfig(t *testing.T) {
	r, _ := NewRouter("eu")
	_ = r.Apply(cpconfigtest.Config(7))
	p := NewPoller(hintClient(func(context.Context, *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
		return &pb.GetConfigResponse{Revision: 8, Config: cpconfigtest.Config(8).ToProto(), LeaderHints: []*pb.LeaderHint{{Region: "eu", NodeId: "foreign", Term: 100}}}, nil
	}), r, "gw")
	if e := p.Refresh(context.Background()); e == nil {
		t.Fatal("accepted invalid hint")
	}
	if r.Revision() != 7 {
		t.Fatalf("failed refresh changed revision to %d", r.Revision())
	}
}

func TestHintsRetainStaticFallbackDuringCPFailure(t *testing.T) {
	r, _ := NewRouter("eu")
	_ = r.Apply(cpconfigtest.Config(7))
	_ = r.ApplyLeaderHints([]*pb.LeaderHint{{Region: "eu", NodeId: "eu-2", Term: 9}})
	p := NewPoller(hintClient(func(context.Context, *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
		return nil, errors.New("CP stopped")
	}), r, "gw")
	if e := p.Refresh(context.Background()); e == nil {
		t.Fatal("expected CP failure")
	}
	l, e := r.GetLeader(0)
	if e != nil || l.NodeID != "eu-2" {
		t.Fatalf("lost hint %v %v", l, e)
	}
	rs, e := r.Replicas(0, true)
	if e != nil || len(rs) != 2 || rs[0].NodeID != "eu-1" || rs[1].NodeID != "eu-2" {
		t.Fatalf("lost static fallback %v %v", rs, e)
	}
}

func TestHintsRejectUnassignedNodesAndRemovedReplicas(t *testing.T) {
	cfg := cpconfigtest.Config(7)
	cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: "eu-3", Region: "eu", Address: "eu-3:7000"})
	r, _ := NewRouter("eu")
	_ = r.Apply(cfg)
	if e := r.ApplyLeaderHints([]*pb.LeaderHint{{Region: "eu", NodeId: "eu-3", Term: 9}}); e == nil {
		t.Fatal("accepted unassigned configured node")
	}
	_ = r.ApplyLeaderHints([]*pb.LeaderHint{{Region: "eu", NodeId: "eu-2", Term: 9}})
	cfg.Revision = 8
	cfg.Partitions[0].ReplicaNodeIDs = []string{"eu-1"}
	if e := r.Apply(cfg); e != nil {
		t.Fatal(e)
	}
	l, e := r.GetLeader(0)
	if e != nil || l.NodeID != "eu-1" {
		t.Fatalf("removed replica retained %v %v", l, e)
	}
}

func TestHintsConcurrentMergeAndRead(t *testing.T) {
	r, _ := NewRouter("eu")
	_ = r.Apply(cpconfigtest.Config(7))
	var wg sync.WaitGroup
	for i := uint64(1); i <= 30; i++ {
		wg.Add(1)
		go func(term uint64) {
			defer wg.Done()
			if e := r.ApplyLeaderHints([]*pb.LeaderHint{{Region: "eu", NodeId: "eu-2", Term: term}}); e != nil {
				t.Error(e)
			}
			_, _ = r.GetLeader(0)
			_, _ = r.Replicas(0, true)
		}(i)
	}
	wg.Wait()
	if got := r.cur.Load().hints; got[leaderhint.Key{Region: "eu", Partition: 0}].Term != 30 {
		t.Fatal(got)
	}
}

type hintClient func(context.Context, *pb.GetConfigRequest) (*pb.GetConfigResponse, error)

func (f hintClient) GetConfig(c context.Context, r *pb.GetConfigRequest, _ ...grpc.CallOption) (*pb.GetConfigResponse, error) {
	return f(c, r)
}
func TestPollerAppliesHintsAtUnchangedConfigRevision(t *testing.T) {
	r, _ := NewRouter("eu")
	if e := r.Apply(cpconfigtest.Config(7)); e != nil {
		t.Fatal(e)
	}
	client := hintClient(func(_ context.Context, q *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
		if q.KnownRevision != 7 {
			t.Fatal(q)
		}
		return &pb.GetConfigResponse{Revision: 7, LeaderHints: []*pb.LeaderHint{{Region: "eu", PartitionId: 0, NodeId: "eu-2", Term: 9}}}, nil
	})
	p := NewPoller(client, r, "gw")
	if e := p.Refresh(context.Background()); e != nil {
		t.Fatal(e)
	}
	l, e := r.GetLeader(0)
	if e != nil || l.NodeID != "eu-2" || r.Revision() != 7 {
		t.Fatalf("route %v %v revision %d", l, e, r.Revision())
	}
}
