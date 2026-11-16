package cpapi

import (
	"context"
	"testing"

	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/statestore"
)

func TestLeaderHintsOrderingAndUnchangedConfig(t *testing.T) {
	ctx := context.Background()
	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(7)); err != nil {
		t.Fatal(err)
	}
	s := New(store)
	report := func(id, region string, term uint64, role pb.RaftRole, ready bool, leader string) {
		t.Helper()
		_, err := s.ReportHeartbeat(ctx, &pb.HeartbeatRequest{NodeId: id, Region: region, Partitions: []*pb.PartitionStatus{{PartitionId: 0, Term: term, Role: role, Ready: ready, LeaderId: leader}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	check := func(node string, term uint64) {
		t.Helper()
		r, err := s.GetConfig(ctx, &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY, KnownRevision: 7})
		if err != nil {
			t.Fatal(err)
		}
		if r.Revision != 7 || r.Config != nil {
			t.Fatalf("config changed: %v", r)
		}
		hs := r.GetLeaderHints()
		if len(hs) != 1 || hs[0].NodeId != node || hs[0].Term != term {
			t.Fatalf("hints: %v", hs)
		}
	}
	report("eu-2", "eu", 5, pb.RaftRole_RAFT_ROLE_LEADER, true, "eu-2")
	check("eu-2", 5)
	report("eu-1", "eu", 4, pb.RaftRole_RAFT_ROLE_LEADER, true, "eu-1")
	check("eu-2", 5)
	report("eu-1", "eu", 5, pb.RaftRole_RAFT_ROLE_LEADER, true, "eu-1")
	check("", 5)
	report("eu-2", "eu", 5, pb.RaftRole_RAFT_ROLE_LEADER, true, "eu-2")
	check("", 5)
	report("eu-1", "eu", 6, pb.RaftRole_RAFT_ROLE_LEADER, true, "eu-1")
	check("eu-1", 6)
	for _, x := range []struct {
		id, region, leader string
		term               uint64
		role               pb.RaftRole
		ready              bool
	}{
		{"unknown", "eu", "unknown", 99, pb.RaftRole_RAFT_ROLE_LEADER, true},
		{"us-1", "eu", "us-1", 99, pb.RaftRole_RAFT_ROLE_LEADER, true},
		{"eu-2", "eu", "eu-2", 99, pb.RaftRole_RAFT_ROLE_FOLLOWER, true},
		{"eu-2", "eu", "eu-2", 99, pb.RaftRole_RAFT_ROLE_LEADER, false},
		{"eu-2", "eu", "eu-1", 99, pb.RaftRole_RAFT_ROLE_LEADER, true},
		{"eu-2", "eu", "eu-2", 0, pb.RaftRole_RAFT_ROLE_LEADER, true},
	} {
		report(x.id, x.region, x.term, x.role, x.ready, x.leader)
		check("eu-1", 6)
	}
	restarted := New(store)
	r, err := restarted.GetConfig(ctx, &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY, KnownRevision: 7})
	if err != nil || len(r.GetLeaderHints()) != 0 {
		t.Fatalf("restart: %v %v", r, err)
	}
}
