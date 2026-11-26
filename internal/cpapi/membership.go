package cpapi

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
)

// RaftGroup identifies an incarnation of a regional partition.
type RaftGroup struct {
	Region      string
	PartitionID uint32
	GroupID     string
}

// MembershipObservation is a committed configuration observed on the Raft leader.
type MembershipObservation struct {
	Group           RaftGroup
	ResponderNodeID string
	LeaderNodeID    string
	Term            uint64
	Index           uint64
	Joint           bool
	VoterNodeIDs    []string
}

// MembershipReader must authenticate the responder and read a full configuration
// from the leader after a quorum ReadIndex and local apply in the same term.
// It must return an error on timeout or loss of leadership. A local diagnostic
// GET /membership does not meet this contract.
type MembershipReader interface {
	ReadConfirmedMembership(context.Context, RaftGroup) (MembershipObservation, error)
}

var ErrMembershipObservation = errors.New("invalid or stale Raft membership observation")

// ReconcileMembership publishes observed voters and their admission together.
// The operator's placement stays in the original assignment.
func (s *Server) ReconcileMembership(ctx context.Context, group RaftGroup, reader MembershipReader) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	snapshot, err := s.store.Config(ctx)
	if err != nil {
		return fmt.Errorf("load config for Raft membership: %w", err)
	}
	assignment, ok := snapshot.Config.Assignment(group.Region, group.PartitionID)
	if !ok || group.GroupID == "" || len(group.GroupID) > MaxIDBytes {
		return fmt.Errorf("%w: an existing regional partition and group ID are required", ErrMembershipObservation)
	}
	observation, err := reader.ReadConfirmedMembership(ctx, group)
	if err != nil {
		return fmt.Errorf("read confirmed Raft membership: %w", err)
	}
	voters, err := validateObservation(snapshot.Config, assignment.ConfirmedMembership, group, observation)
	if err != nil {
		return err
	}

	membership := &cpconfig.RaftMembership{
		GroupID: group.GroupID, Index: observation.Index, Term: observation.Term,
		LeaderNodeID: observation.LeaderNodeID, VoterNodeIDs: voters,
	}
	admitted := assignment.ReplicaNodeIDs
	if previous := assignment.ConfirmedMembership; previous != nil {
		admitted = previous.AdmittedNodeIDs
	}
	for _, nodeID := range voters {
		ready, err := s.membershipReadiness(ctx, group, observation, nodeID)
		if err != nil {
			return err
		}
		if ready || slices.Contains(admitted, nodeID) {
			membership.AdmittedNodeIDs = append(membership.AdmittedNodeIDs, nodeID)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish confirmed Raft membership: %w", err)
	}
	previous := assignment.ConfirmedMembership
	if previous != nil && previous.Index == membership.Index && previous.Term == membership.Term &&
		slices.Equal(previous.AdmittedNodeIDs, membership.AdmittedNodeIDs) {
		return nil
	}

	for i := range snapshot.Config.Partitions {
		current := &snapshot.Config.Partitions[i]
		if current.Region == group.Region && current.PartitionID == group.PartitionID {
			current.ConfirmedMembership = membership
			break
		}
	}
	snapshot.Config.Revision++
	if err := s.store.ApplyConfig(ctx, snapshot.Config); err != nil {
		return fmt.Errorf("publish confirmed Raft membership: %w", err)
	}
	return nil
}

func validateObservation(config *cpconfig.Config, previous *cpconfig.RaftMembership, group RaftGroup, observed MembershipObservation) ([]string, error) {
	if observed.Group != group || observed.Joint || observed.Index == 0 || observed.Term == 0 ||
		observed.LeaderNodeID == "" || observed.ResponderNodeID != observed.LeaderNodeID ||
		len(observed.VoterNodeIDs) == 0 || len(observed.VoterNodeIDs) > len(config.Nodes) {
		return nil, fmt.Errorf("%w: expected a complete configuration from the regional group leader", ErrMembershipObservation)
	}
	voters := slices.Clone(observed.VoterNodeIDs)
	slices.Sort(voters)
	if !slices.Contains(voters, observed.LeaderNodeID) {
		return nil, fmt.Errorf("%w: voter identities are incomplete or repeated", ErrMembershipObservation)
	}
	for i, id := range voters {
		node, ok := config.NodeByID(id)
		if !ok || node.Region != group.Region || (i > 0 && voters[i-1] == id) {
			return nil, fmt.Errorf("%w: voter %q is unknown, duplicated or outside the group region", ErrMembershipObservation, id)
		}
	}
	if previous != nil && (previous.GroupID != group.GroupID || observed.Term < previous.Term || observed.Index < previous.Index ||
		(observed.Term == previous.Term && observed.LeaderNodeID != previous.LeaderNodeID) ||
		(observed.Index == previous.Index && !slices.Equal(voters, previous.VoterNodeIDs))) {
		return nil, fmt.Errorf("%w: confirmation regresses or conflicts with the published group", ErrMembershipObservation)
	}
	return voters, nil
}

func (s *Server) membershipReadiness(ctx context.Context, group RaftGroup, observed MembershipObservation, nodeID string) (bool, error) {
	heartbeat, ok, err := s.store.Heartbeat(ctx, nodeID)
	if err != nil {
		return false, fmt.Errorf("read voter readiness: %w", err)
	}
	age := s.clock.Now().Sub(heartbeat.ReceivedAt)
	if !ok || age < 0 || age >= s.nodeTTL || heartbeat.Region != group.Region {
		return false, nil
	}
	for _, partition := range heartbeat.Partitions {
		if partition.PartitionID != group.PartitionID || partition.GroupID != group.GroupID {
			continue
		}
		if partition.Term > observed.Term || partition.MembershipIndex > observed.Index ||
			(partition.Term == observed.Term && partition.Role == statestore.RaftRoleLeader && nodeID != observed.LeaderNodeID) {
			return false, fmt.Errorf("%w: voter reports a newer or conflicting group state", ErrMembershipObservation)
		}
		return partition.MembershipIndex == observed.Index && partition.AppliedIndex >= observed.Index &&
			partition.CommitIndex >= partition.AppliedIndex && partition.Term == observed.Term &&
			partition.LeaderID == observed.LeaderNodeID && partition.Ready &&
			(partition.Role == statestore.RaftRoleFollower || partition.Role == statestore.RaftRoleLeader), nil
	}
	return false, nil
}
