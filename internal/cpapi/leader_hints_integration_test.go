package cpapi

import (
	"context"
	"sync"
	"testing"

	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
)

type serverConfigClient struct{ s *Server }

func (c *serverConfigClient) GetConfig(ctx context.Context, r *pb.GetConfigRequest, _ ...grpc.CallOption) (*pb.GetConfigResponse, error) {
	return c.s.GetConfig(ctx, r)
}

func TestGatewayRetainsHighWaterAcrossCPRestart(t *testing.T) {
	ctx := context.Background()
	cfg := cpconfigtest.Config(7)
	restart := func() *Server {
		m := statestore.NewMemory()
		if e := m.ApplyConfig(ctx, cfg); e != nil {
			t.Fatal(e)
		}
		return New(m)
	}
	client := &serverConfigClient{s: restart()}
	r, _ := gateway.NewRouter("eu")
	p := gateway.NewPoller(client, r, "gw")
	report := func(id string, term uint64) {
		t.Helper()
		_, e := client.s.ReportHeartbeat(ctx, &pb.HeartbeatRequest{NodeId: id, Region: "eu", Partitions: []*pb.PartitionStatus{{PartitionId: 0, LeaderId: id, Role: pb.RaftRole_RAFT_ROLE_LEADER, Ready: true, Term: term}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	check := func(node string) {
		t.Helper()
		if e := p.Refresh(ctx); e != nil {
			t.Fatal(e)
		}
		l, e := r.WriteLeader(0)
		if e != nil || l.NodeID != node || r.Revision() != 7 {
			t.Fatalf("route %v %v rev %d", l, e, r.Revision())
		}
		rs, e := r.Replicas(0, true)
		if e != nil || len(rs) != 2 {
			t.Fatalf("fallback %v %v", rs, e)
		}
	}
	report("eu-2", 9)
	check("eu-2") // full config plus hints
	client.s = restart()
	check("eu-2") // empty restarted CP, unchanged config
	report("eu-1", 8)
	check("eu-2") // lower after CP loses high-water mark
	report("eu-1", 9)
	check("eu-1") // conflict at gateway: static fallback
	report("eu-2", 9)
	check("eu-1") // conflict tombstone from CP
	report("eu-2", 10)
	check("eu-2")
}

func TestLeaderHintsIgnoreConfiguredUnassignedNode(t *testing.T) {
	ctx := context.Background()
	cfg := cpconfigtest.Config(7)
	cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: "eu-3", Region: "eu", Address: "eu-3:7000"})
	store := statestore.NewMemory()
	if e := store.ApplyConfig(ctx, cfg); e != nil {
		t.Fatal(e)
	}
	s := New(store)
	_, e := s.ReportHeartbeat(ctx, &pb.HeartbeatRequest{NodeId: "eu-3", Region: "eu", Partitions: []*pb.PartitionStatus{{PartitionId: 0, LeaderId: "eu-3", Role: pb.RaftRole_RAFT_ROLE_LEADER, Ready: true, Term: 100}}})
	if e != nil {
		t.Fatal(e)
	}
	response, e := s.GetConfig(ctx, &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY, KnownRevision: 7})
	if e != nil || len(response.GetLeaderHints()) != 0 {
		t.Fatalf("unassigned node accepted: %v %v", response, e)
	}
}

func TestLeaderHintsConcurrentReportsAndPulls(t *testing.T) {
	ctx := context.Background()
	store := statestore.NewMemory()
	_ = store.ApplyConfig(ctx, cpconfigtest.Config(7))
	s := New(store)
	var wg sync.WaitGroup
	for i := uint64(1); i <= 30; i++ {
		wg.Add(1)
		go func(term uint64) {
			defer wg.Done()
			_, e := s.ReportHeartbeat(ctx, &pb.HeartbeatRequest{NodeId: "eu-2", Region: "eu", Partitions: []*pb.PartitionStatus{{PartitionId: 0, LeaderId: "eu-2", Role: pb.RaftRole_RAFT_ROLE_LEADER, Ready: true, Term: term}}})
			if e != nil {
				t.Error(e)
			}
			_, e = s.GetConfig(ctx, &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY, KnownRevision: 7})
			if e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	response, e := s.GetConfig(ctx, &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY, KnownRevision: 7})
	if e != nil || len(response.GetLeaderHints()) != 1 || response.LeaderHints[0].Term != 30 {
		t.Fatalf("hints %v %v", response, e)
	}
}
