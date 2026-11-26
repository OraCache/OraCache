package node

import (
	"context"

	pb "github.com/OraCache/oracache/gen/membership/v1"
	"github.com/OraCache/oracache/internal/membershipapi"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type membershipServer struct {
	pb.UnimplementedMembershipAdminServer
	n *Node
}

func (s *membershipServer) validate(group, region string, partition uint32) error {
	if group == "" || group != s.n.cfg.GroupID || region != s.n.cfg.Region || partition != s.n.cfg.ShardID {
		return status.Error(codes.InvalidArgument, "group_id, region or partition_id does not match this node")
	}
	return nil
}

func (s *membershipServer) ChangeMembership(ctx context.Context, req *pb.ChangeMembershipRequest) (*pb.ChangeMembershipResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validate(req.GetGroupId(), req.GetRegion(), req.GetPartitionId()); err != nil {
		return nil, err
	}
	result, err := s.n.ChangeMembership(ctx, membershipapi.ToChange(req))
	if err != nil {
		return nil, err
	}
	return membershipapi.Result(result), nil
}

func (s *membershipServer) GetMembership(_ context.Context, req *pb.GetMembershipRequest) (*pb.Membership, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := s.validate(req.GetGroupId(), req.GetRegion(), req.GetPartitionId()); err != nil {
		return nil, err
	}
	st := s.n.inst.Status()
	if st.Role != "StateLeader" {
		m := s.n.inst.Membership()
		var address string
		for _, peer := range m.Members {
			if peer.ID == st.LeaderID {
				address = peer.Address
				break
			}
		}
		return nil, &shardraft.NotLeaderError{LeaderID: st.LeaderID, Address: address, Term: st.Term}
	}
	return membershipapi.Membership(s.n.Membership()), nil
}
