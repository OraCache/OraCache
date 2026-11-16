package gateway

import (
	cp "github.com/OraCache/oracache/gen/controlplane/v1"
	"testing"
	"time"
)

func TestDiscoveryConflictingHighestTermProbesDeterministically(t *testing.T) {
	r, _ := NewRouter("a")
	view := discoveryView(10000, "n2", 2)
	// Both members self-report as leader at the same term. Neither report is
	// authority; conflict must discard the n2 preference and sort candidates.
	view.Observations[0].Heartbeat.Partitions[0].Role = cp.RaftRole_RAFT_ROLE_LEADER
	view.Observations[0].Heartbeat.Partitions[0].LeaderId = "n1"
	if err := r.ApplyDiscovery(view, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	rs, err := r.Replicas(0, true)
	if err != nil || len(rs) != 2 || rs[0].NodeID != "n1" || rs[1].NodeID != "n2" {
		t.Fatalf("candidates: %v %v", rs, err)
	}
}

func TestDiscoveryFullSnapshotRemovesStillLeasedNode(t *testing.T) {
	r, _ := NewRouter("a")
	now := time.Now()
	if err := r.ApplyDiscovery(discoveryView(10000, "n1", 1), 1, now); err != nil {
		t.Fatal(err)
	}
	next := discoveryView(10000, "n2", 2)
	next.Observations = next.Observations[1:]
	if err := r.ApplyDiscovery(next, 2, now); err != nil {
		t.Fatal(err)
	}
	rs, err := r.Replicas(0, true)
	if err != nil || len(rs) != 1 || rs[0].NodeID != "n2" {
		t.Fatalf("removed node retained: %v %v", rs, err)
	}
	if r.eligible(0, true, "n1:9001") {
		t.Fatal("old lease survived a newer full snapshot")
	}
	if err := r.ApplyDiscovery(&cp.DiscoverySnapshot{Region: "a", GroupId: "g"}, 3, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetLeader(0); err == nil {
		t.Fatal("empty snapshot did not clear routes")
	}
}
