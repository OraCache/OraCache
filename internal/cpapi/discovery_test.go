package cpapi

import (
	"context"
	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/discovery"
	"github.com/OraCache/oracache/internal/statestore"
	"testing"
	"time"
)

func TestDiscoveryEmptyRegistrationExpiry(t *testing.T) {
	c, _ := discovery.New("a", "g", time.Second)
	s := New(statestore.NewMemory(), WithDiscovery(c))
	ctx := context.Background()
	req := &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY}
	empty, err := s.GetConfig(ctx, req)
	if err != nil || empty.Discovery == nil || len(empty.Discovery.Observations) != 0 {
		t.Fatalf("%v %v", empty, err)
	}
	hb := &pb.HeartbeatRequest{NodeId: "n", Region: "a", ClientAddress: "n:9001", Sequence: 1, Partitions: []*pb.PartitionStatus{{GroupId: "g", Ready: true, Role: pb.RaftRole_RAFT_ROLE_LEADER, LeaderId: "n", Term: 1}}}
	if _, err := s.ReportHeartbeat(ctx, hb); err != nil {
		t.Fatal(err)
	}
	req.KnownRevision = empty.Revision
	full, err := s.GetConfig(ctx, req)
	if err != nil || len(full.Discovery.Observations) != 1 || full.Revision <= empty.Revision {
		t.Fatalf("%v %v", full, err)
	}
	// Receiver time, not sender timestamp; use the catalog clock directly for exact boundary.
	snap, _ := c.Snapshot(time.Now().Add(2 * time.Second))
	if len(snap.Observations) != 0 {
		t.Fatal("not expired")
	}
	hb.Sequence++
	if _, err := s.ReportHeartbeat(ctx, hb); err != nil {
		t.Fatal(err)
	}
	full, err = s.GetConfig(ctx, req)
	if err != nil || len(full.Discovery.Observations) != 1 {
		t.Fatalf("recovery: %v %v", full, err)
	}
}
