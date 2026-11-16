package discovery

import (
	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func heartbeat(id string, seq, term uint64, leader string) *pb.HeartbeatRequest {
	role := pb.RaftRole_RAFT_ROLE_FOLLOWER
	if id == leader {
		role = pb.RaftRole_RAFT_ROLE_LEADER
	}
	return &pb.HeartbeatRequest{NodeId: id, Region: "a", ClientAddress: id + ":9001", Sequence: seq, Partitions: []*pb.PartitionStatus{{PartitionId: 0, GroupId: "g", Ready: true, Role: role, Term: term, LeaderId: leader}}}
}
func TestCatalogExpiryRecoveryOrdering(t *testing.T) {
	now := time.Now()
	c, err := New("a", "g", 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := c.Snapshot(now); len(s.Observations) != 0 {
		t.Fatal("not empty")
	}
	h := heartbeat("n1", 1, 1, "n1")
	if err := c.Report(h, now); err != nil {
		t.Fatal(err)
	}
	h.ClientAddress = "mutated:1"
	s, _ := c.Snapshot(now.Add(5 * time.Second))
	if len(s.Observations) != 1 || s.Observations[0].RemainingMs != 15000 || s.Observations[0].Heartbeat.ClientAddress != "n1:9001" {
		t.Fatal(s)
	}
	if err := c.Report(heartbeat("n1", 1, 1, "n1"), now.Add(19*time.Second)); err == nil {
		t.Fatal("duplicate refreshed")
	}
	if s, _ := c.Snapshot(now.Add(20 * time.Second)); len(s.Observations) != 0 {
		t.Fatal("expired survived")
	}
	if err := c.Report(heartbeat("n1", 2, 1, "n1"), now.Add(21*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.Report(heartbeat("n1", 3, 0, "n1"), now.Add(22*time.Second)); err == nil {
		t.Fatal("term regressed")
	}
	if err := c.Report(heartbeat("n1", 1, 1, "n1"), now.Add(23*time.Second)); err == nil {
		t.Fatal("old sequence accepted")
	}
	s, _ = c.Snapshot(now.Add(24 * time.Second))
	if len(s.Observations) != 1 || s.Observations[0].RemainingMs != 17000 {
		t.Fatal(s)
	}
}
func TestCatalogReadinessAndValidation(t *testing.T) {
	now := time.Now()
	c, _ := New("a", "g", time.Second)
	for _, drain := range []bool{false, true} {
		h := heartbeat("n1", 1, 1, "n1")
		h.Partitions[0].Ready = !drain
		h.Partitions[0].Draining = drain
		c, _ = New("a", "g", time.Second)
		if err := c.Report(h, now); err != nil {
			t.Fatal(err)
		}
		s, _ := c.Snapshot(now)
		if drain && len(s.Observations) != 0 {
			t.Fatal(s)
		}
	}
	h := heartbeat("n1", 2, 1, "n1")
	h.Partitions[0].Ready = false
	if err := c.Report(h, now); err != nil {
		t.Fatal(err)
	}
	s, _ := c.Snapshot(now)
	if len(s.Observations) != 0 {
		t.Fatal(s)
	}
	for _, mutate := range []func(*pb.HeartbeatRequest){func(h *pb.HeartbeatRequest) { h.Region = "b" }, func(h *pb.HeartbeatRequest) { h.ClientAddress = "bad" }, func(h *pb.HeartbeatRequest) { h.Sequence = 0 }, func(h *pb.HeartbeatRequest) { h.Partitions[0].GroupId = "other" }, func(h *pb.HeartbeatRequest) { h.Partitions = append(h.Partitions, h.Partitions[0]) }, func(h *pb.HeartbeatRequest) { h.Partitions[0].LeaderId = "other" }, func(h *pb.HeartbeatRequest) { h.NodeId = " " }, func(h *pb.HeartbeatRequest) { h.Partitions[0].Role = 99 }} {
		x := proto.Clone(heartbeat("n2", 1, 1, "n2")).(*pb.HeartbeatRequest)
		mutate(x)
		if err := c.Report(x, now); err == nil {
			t.Fatalf("accepted invalid %v", x)
		}
	}
}
