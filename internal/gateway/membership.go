package gateway

import (
	"context"
	"fmt"
	"slices"
	"time"

	pb "github.com/OraCache/oracache/gen/membership/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// MembershipForwarder uses the CP live-view to locate a group and follows
// NotLeader hints only when the hinted address belongs to that group.
type MembershipForwarder struct {
	pb.UnimplementedMembershipAdminServer
	Router *Router
}

func (f *MembershipForwarder) call(ctx context.Context, group, region string, partition uint32, read bool, invoke func(context.Context, pb.MembershipAdminClient) (any, error)) (any, error) {
	if f.Router == nil {
		return nil, status.Error(codes.Unavailable, "membership router unavailable")
	}
	addresses, err := f.Router.AdminCandidates(group, region, partition)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("group route unavailable: %v", err))
	}
	seen := map[string]bool{}
	for len(addresses) > 0 {
		address := addresses[0]
		addresses = addresses[1:]
		if seen[address] {
			continue
		}
		seen[address] = true
		conn, dialErr := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
		if dialErr != nil {
			return nil, status.Error(codes.Unavailable, dialErr.Error())
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		result, callErr := invoke(attemptCtx, pb.NewMembershipAdminClient(conn))
		cancel()
		_ = conn.Close()
		if callErr == nil {
			return result, nil
		}
		if read && ctx.Err() == nil && (status.Code(callErr) == codes.Unavailable || status.Code(callErr) == codes.DeadlineExceeded) {
			continue
		}
		hint, _, notLeader := notLeaderHint(callErr)
		if !notLeader {
			return nil, callErr
		} // commit may already have happened
		if hint != "" {
			allowed, routeErr := f.Router.AdminCandidates(group, region, partition)
			if routeErr == nil && slices.Contains(allowed, hint) && !seen[hint] {
				addresses = append([]string{hint}, addresses...)
			}
		}
	}
	return nil, status.Error(codes.Unavailable, "no serving leader for group")
}

func (f *MembershipForwarder) ChangeMembership(ctx context.Context, req *pb.ChangeMembershipRequest) (*pb.ChangeMembershipResponse, error) {
	if req == nil || req.GetGroupId() == "" || req.GetRegion() == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and region are required")
	}
	out, err := f.call(ctx, req.GetGroupId(), req.GetRegion(), req.GetPartitionId(), false, func(ctx context.Context, c pb.MembershipAdminClient) (any, error) {
		return c.ChangeMembership(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return out.(*pb.ChangeMembershipResponse), nil
}

func (f *MembershipForwarder) GetMembership(ctx context.Context, req *pb.GetMembershipRequest) (*pb.Membership, error) {
	if req == nil || req.GetGroupId() == "" || req.GetRegion() == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and region are required")
	}
	out, err := f.call(ctx, req.GetGroupId(), req.GetRegion(), req.GetPartitionId(), true, func(ctx context.Context, c pb.MembershipAdminClient) (any, error) { return c.GetMembership(ctx, req) })
	if err != nil {
		return nil, err
	}
	return out.(*pb.Membership), nil
}
