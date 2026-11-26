package membershipapi

import (
	pb "github.com/OraCache/oracache/gen/membership/v1"
	"github.com/OraCache/oracache/shardraft"
)

func ToChange(req *pb.ChangeMembershipRequest) shardraft.MembershipChange {
	c := shardraft.MembershipChange{OperationID: req.GetOperationId(), Action: req.GetAction(), PeerID: req.GetPeerId(), RemoveID: req.GetRemoveId()}
	if p := req.GetPeer(); p != nil {
		c.Peer = shardraft.Peer{ID: p.GetId(), NodeID: p.GetNodeId(), Address: p.GetAddress(), RaftAddress: p.GetRaftAddress()}
	}
	return c
}

func Membership(m shardraft.Membership) *pb.Membership {
	out := &pb.Membership{GroupId: m.GroupID, LeaderId: m.LeaderID, Term: m.Term, Index: m.Index, Joint: m.Joint, RemovedIds: append([]uint64(nil), m.RemovedIDs...)}
	for _, member := range m.Members {
		out.Members = append(out.Members, &pb.Member{Peer: &pb.Peer{Id: member.ID, NodeId: member.NodeID, Address: member.Address, RaftAddress: member.RaftAddress}, Voter: member.Voter, Learner: member.Learner, OutgoingVoter: member.OutgoingVoter})
	}
	return out
}

func Result(r *shardraft.MembershipResult) *pb.ChangeMembershipResponse {
	return &pb.ChangeMembershipResponse{OperationId: r.OperationID, ChangeIndex: r.ChangeIndex, Membership: Membership(r.Membership)}
}
