package cli

import (
	"bytes"
	"context"
	"net"
	"testing"

	pb "github.com/OraCache/oracache/gen/membership/v1"
	"google.golang.org/grpc"
)

type membershipStub struct {
	pb.UnimplementedMembershipAdminServer
	changes []*pb.ChangeMembershipRequest
	gets    []*pb.GetMembershipRequest
}

func (s *membershipStub) ChangeMembership(_ context.Context, req *pb.ChangeMembershipRequest) (*pb.ChangeMembershipResponse, error) {
	s.changes = append(s.changes, req)
	return &pb.ChangeMembershipResponse{OperationId: req.GetOperationId(), ChangeIndex: uint64(len(s.changes)), Membership: &pb.Membership{GroupId: req.GetGroupId()}}, nil
}

func (s *membershipStub) GetMembership(_ context.Context, req *pb.GetMembershipRequest) (*pb.Membership, error) {
	s.gets = append(s.gets, req)
	return &pb.Membership{GroupId: req.GetGroupId()}, nil
}

func TestMembershipCommands(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	stub := &membershipStub{}
	pb.RegisterMembershipAdminServer(server, stub)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	t.Setenv("ORACACHE_ENDPOINT", l.Addr().String())
	call := func(args ...string) {
		t.Helper()
		var out, stderr bytes.Buffer
		if code := Run(args, forbiddenReader{}, &out, &stderr); code != 0 {
			t.Fatalf("%v: %s", args, stderr.String())
		}
		if !bytes.Contains(out.Bytes(), []byte(`"groupId":"group-a"`)) {
			t.Fatalf("response: %s", out.String())
		}
	}
	common := []string{"--group-id=group-a", "--region=eu", "--partition-id=2"}
	call(append([]string{"membership", "add-learner"}, append(common, "--operation-id=add-4", "--peer-id=4", "--node-id=node-4", "--address=127.0.0.1:80", "--raft-address=http://127.0.0.1:81/raft")...)...)
	call(append([]string{"membership", "promote"}, append(common, "--operation-id=promote-4", "--peer-id=4")...)...)
	call(append([]string{"membership", "remove"}, append(common, "--operation-id=remove-3", "--peer-id=3")...)...)
	call(append([]string{"membership", "replace"}, append(common, "--operation-id=replace-3", "--old-peer-id=3", "--learner-id=4")...)...)
	call(append([]string{"membership", "get"}, common...)...)
	if len(stub.changes) != 4 || len(stub.gets) != 1 {
		t.Fatalf("calls: %d changes %d gets", len(stub.changes), len(stub.gets))
	}
	if c := stub.changes[0]; c.GetAction() != "add-learner" || c.GetPeer().GetId() != 4 || c.GetPeer().GetNodeId() != "node-4" || c.GetGroupId() != "group-a" || c.GetPartitionId() != 2 {
		t.Fatalf("add request: %+v", c)
	}
	if c := stub.changes[3]; c.GetAction() != "replace" || c.GetPeerId() != 4 || c.GetRemoveId() != 3 {
		t.Fatalf("replace request: %+v", c)
	}
}
