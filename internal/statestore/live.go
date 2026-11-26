package statestore

import (
	"slices"

	"github.com/OraCache/oracache/internal/cpconfig"
)

type replicaHeartbeat struct {
	heartbeat Heartbeat
	received  bool
	live      bool
}

// projectLiveConfig derives routing readiness from the backend's liveness view.
func projectLiveConfig(snapshot Snapshot, lookup func(cpconfig.Node) replicaHeartbeat) Snapshot {
	if snapshot.Config == nil {
		return snapshot
	}
	config := snapshot.Config.Clone()
	assigned := make(map[string]bool, len(config.Nodes))
	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		admitted := assignment.ReplicaNodeIDs
		if membership := assignment.ConfirmedMembership; membership != nil {
			admitted = membership.AdmittedNodeIDs
			assignment.UseConfirmedVoters()
		}
		for _, nodeID := range admitted {
			assigned[nodeID] = true
		}
	}

	type replicaKey struct {
		nodeID      string
		partitionID uint32
	}
	statuses := make(map[replicaKey]PartitionStatus, len(config.Partitions))
	missingReplicaHeartbeats := false
	for i := range config.Nodes {
		node := &config.Nodes[i]
		if node.State == cpconfig.NodeStateDraining {
			continue
		}
		observed := lookup(*node)
		if !observed.live {
			missingReplicaHeartbeats = missingReplicaHeartbeats || (assigned[node.ID] && !observed.received)
			node.State = cpconfig.NodeStateDead
			continue
		}
		ready := false
		for _, partition := range observed.heartbeat.Partitions {
			statuses[replicaKey{node.ID, partition.PartitionID}] = partition
			ready = ready || partition.Ready
		}
		switch {
		case ready, !assigned[node.ID]:
			node.State = cpconfig.NodeStateReady
		default:
			node.State = cpconfig.NodeStateStarting
		}
	}

	masterRegion := config.MasterRegion()
	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		assignment.ObservedTerm = 0
		membership := assignment.ConfirmedMembership
		if membership != nil {
			assignment.ObservedTerm = membership.Term
		}
		assignment.ReadyReplicaNodeIDs = nil
		assignment.LeaderConflict = false
		for _, nodeID := range assignment.ReplicaNodeIDs {
			status := statuses[replicaKey{nodeID, assignment.PartitionID}]
			if membership == nil || status.matchesMembership(membership) {
				assignment.ObservedTerm = max(assignment.ObservedTerm, status.Term)
			}
		}
		var leaderID string
		for _, nodeID := range assignment.ReplicaNodeIDs {
			status, ok := statuses[replicaKey{nodeID, assignment.PartitionID}]
			if !ok || !status.Ready || status.Term != assignment.ObservedTerm {
				continue
			}
			if status.Role == RaftRoleLearner {
				continue
			}
			if membership != nil && (!slices.Contains(membership.AdmittedNodeIDs, nodeID) || !status.matchesMembership(membership) ||
				(status.Role != RaftRoleFollower && status.Role != RaftRoleLeader)) {
				continue
			}
			assignment.ReadyReplicaNodeIDs = append(assignment.ReadyReplicaNodeIDs, nodeID)
			// Followers cannot establish the readiness of the leader they report.
			if status.Role == RaftRoleLeader && (status.LeaderID == "" || status.LeaderID == nodeID) {
				assignment.LeaderConflict = assignment.LeaderConflict || leaderID != "" ||
					(membership != nil && status.Term == membership.Term && nodeID != membership.LeaderNodeID)
				leaderID = nodeID
			}
		}
		assignment.Ready = leaderID != "" && !assignment.LeaderConflict
		assignment.Readable = assignment.Ready
		assignment.Writable = assignment.Ready && assignment.Region == masterRegion
		if assignment.Ready {
			assignment.LeaderNodeID = leaderID
		}
	}

	return Snapshot{
		Revision:                 snapshot.Revision,
		Config:                   config,
		MissingReplicaHeartbeats: missingReplicaHeartbeats,
	}
}

func (p PartitionStatus) matchesMembership(m *cpconfig.RaftMembership) bool {
	return p.GroupID == m.GroupID && p.MembershipIndex == m.Index && p.AppliedIndex >= m.Index && p.CommitIndex >= p.AppliedIndex && p.LeaderID != ""
}
