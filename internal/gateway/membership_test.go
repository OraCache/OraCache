package gateway_test

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "github.com/OraCache/oracache/gen/membership/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type adminStub struct {
	pb.UnimplementedMembershipAdminServer
	mu    sync.Mutex
	calls int
	err   error
}

func (s *adminStub) ChangeMembership(_ context.Context, req *pb.ChangeMembershipRequest) (*pb.ChangeMembershipResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &pb.ChangeMembershipResponse{OperationId: req.GetOperationId()}, nil
}

func (s *adminStub) count() int       { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }
func (s *adminStub) setErr(err error) { s.mu.Lock(); s.err = err; s.mu.Unlock() }

func serveAdminStub(t *testing.T, stub *adminStub) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterMembershipAdminServer(server, stub)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	return l.Addr().String()
}

func TestMembershipForwarderFollowsOnlyGroupHintsAndDoesNotReplayAmbiguousWrites(t *testing.T) {
	first, second, foreign := &adminStub{}, &adminStub{}, &adminStub{}
	a1, a2, outside := serveAdminStub(t, first), serveAdminStub(t, second), serveAdminStub(t, foreign)
	config := &cpconfig.Config{Revision: 1, PartitionCount: 1,
		Regions:    []cpconfig.Region{{ID: "r", Role: cpconfig.RegionRoleMaster}},
		Nodes:      []cpconfig.Node{{ID: "n1", Region: "r", Address: a1}, {ID: "n2", Region: "r", Address: a2}},
		Partitions: []cpconfig.PartitionAssignment{{Region: "r", GroupID: "g", LeaderNodeID: "n1", ReplicaNodeIDs: []string{"n1", "n2"}, Ready: true, Readable: true, Writable: true, ReadyReplicaNodeIDs: []string{"n1", "n2"}}},
		Memberships: []cpconfig.GroupMembership{{GroupID: "g", Region: "r", ReporterID: 1, Index: 2, Confirmed: true, Members: []cpconfig.GroupMember{
			{PeerID: 1, NodeID: "n1", Address: a1, RaftAddress: "http://n1/raft", Voter: true},
			{PeerID: 2, NodeID: "n2", Address: a2, RaftAddress: "http://n2/raft", Voter: true},
		}}},
	}
	router, _ := gateway.NewRouter("r")
	if err := router.ApplyView(config); err != nil {
		t.Fatal(err)
	}
	f := &gateway.MembershipForwarder{Router: router}
	req := &pb.ChangeMembershipRequest{GroupId: "g", Region: "r", OperationId: "change", Action: "remove", PeerId: 2}
	first.setErr(&shardraft.NotLeaderError{LeaderID: 2, Address: a2, Term: 3})
	if _, err := f.ChangeMembership(t.Context(), req); err != nil {
		t.Fatalf("follow leader hint: %v", err)
	}
	if first.count() != 1 || second.count() != 1 {
		t.Fatalf("calls first/second: %d/%d", first.count(), second.count())
	}
	first.setErr(&shardraft.NotLeaderError{LeaderID: 99, Address: outside, Term: 3})
	if _, err := f.ChangeMembership(t.Context(), req); err != nil {
		t.Fatalf("fallback to known voter: %v", err)
	}
	if foreign.count() != 0 {
		t.Fatal("followed hint outside requested group")
	}
	first.setErr(status.Error(codes.Unavailable, "outcome unknown"))
	if _, err := f.ChangeMembership(t.Context(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous mutation: %v", err)
	}
	if second.count() != 2 {
		t.Fatal("replayed ambiguous mutation")
	}
}
