package statestore

import "github.com/OraCache/oracache/internal/cpconfig"

type replicaHeartbeat struct {
	heartbeat Heartbeat
	received  bool
	live      bool
}

// projectLiveConfig derives routing readiness from the backend's liveness view.
func projectLiveConfig(snapshot Snapshot, memberships map[groupKey]cpconfig.GroupMembership, lookup func(cpconfig.Node) replicaHeartbeat) Snapshot {
	if snapshot.Config == nil {
		return snapshot
	}
	config := snapshot.Config.Clone()
	firstApplied := len(config.Memberships)
	for _, m := range memberships {
		copyOf := m
		copyOf.Members = append([]cpconfig.GroupMember(nil), m.Members...)
		copyOf.RemovedIDs = append([]uint64(nil), m.RemovedIDs...)
		copyOf.Confirmed = false
		config.Memberships = append(config.Memberships, copyOf)
		for _, peer := range m.Members {
			found := false
			for i := range config.Nodes {
				if config.Nodes[i].ID == peer.NodeID {
					config.Nodes[i].Address = peer.Address
					found = true
					break
				}
			}
			if !found {
				config.Nodes = append(config.Nodes, cpconfig.Node{ID: peer.NodeID, Region: m.Region, Address: peer.Address})
			}
		}
	}
	assigned := make(map[string]bool, len(config.Nodes))
	for i := range config.Partitions {
		a := config.Partitions[i]
		found := false
		for _, m := range config.Memberships {
			if m.Region == a.Region && m.PartitionID == a.PartitionID {
				for _, peer := range m.Members {
					assigned[peer.NodeID] = true
				}
				found = true
				break
			}
		}
		if !found {
			for _, nodeID := range a.ReplicaNodeIDs {
				assigned[nodeID] = true
			}
		}
	}

	type replicaKey struct {
		nodeID      string
		partitionID uint32
	}
	statuses := make(map[replicaKey]PartitionStatus, len(config.Partitions))
	for i := range config.Nodes {
		node := &config.Nodes[i]
		if node.State == cpconfig.NodeStateDraining {
			continue
		}
		observed := lookup(*node)
		if !observed.live {
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
	for i := range config.Memberships {
		m := &config.Memberships[i]
		var reporter string
		for _, peer := range m.Members {
			if peer.PeerID == m.ReporterID {
				reporter = peer.NodeID
				break
			}
		}
		state, ok := statuses[replicaKey{reporter, m.PartitionID}]
		m.Confirmed = ok && state.Role == RaftRoleLeader && state.Term == m.Term && state.Ready && state.Membership != nil && state.Membership.GroupID == m.GroupID && state.Membership.Index == m.Index
	}
	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		assignment.ObservedTerm = 0
		assignment.ReadyReplicaNodeIDs = nil
		assignment.LeaderConflict = false
		candidates := assignment.ReplicaNodeIDs
		for _, m := range config.Memberships {
			if m.Region == assignment.Region && m.PartitionID == assignment.PartitionID {
				candidates = nil
				for _, peer := range m.Members {
					if peer.Voter {
						candidates = append(candidates, peer.NodeID)
					}
				}
				break
			}
		}
		for _, nodeID := range candidates {
			status := statuses[replicaKey{nodeID, assignment.PartitionID}]
			assignment.ObservedTerm = max(assignment.ObservedTerm, status.Term)
		}
		var leaderID string
		for _, nodeID := range candidates {
			status, ok := statuses[replicaKey{nodeID, assignment.PartitionID}]
			if !ok || !status.Ready || status.Term != assignment.ObservedTerm {
				continue
			}
			assignment.ReadyReplicaNodeIDs = append(assignment.ReadyReplicaNodeIDs, nodeID)
			// Followers cannot establish the readiness of the leader they report.
			if status.Role == RaftRoleLeader && (status.LeaderID == "" || status.LeaderID == nodeID) {
				assignment.LeaderConflict = leaderID != ""
				leaderID = nodeID
			}
		}
		assignment.Ready = leaderID != "" && !assignment.LeaderConflict
		assignment.Readable = assignment.Ready
		assignment.Writable = assignment.Ready && assignment.Region == masterRegion
		if assignment.Ready {
			assignment.LeaderNodeID = leaderID
		}
		for j := range config.Memberships {
			m := &config.Memberships[j]
			if m.Region != assignment.Region || m.PartitionID != assignment.PartitionID {
				continue
			}
			var reporter string
			for _, peer := range m.Members {
				if peer.PeerID == m.ReporterID {
					reporter = peer.NodeID
					break
				}
			}
			m.Confirmed = m.Confirmed && assignment.Ready && assignment.ObservedTerm == m.Term && leaderID == reporter
			if !m.Confirmed {
				assignment.Ready, assignment.Readable, assignment.Writable = false, false, false
				assignment.ReadyReplicaNodeIDs = nil
			}
		}
	}

	missingReplicaHeartbeats := false
	hasConfirmedMembership := false
	for _, assignment := range config.Partitions {
		confirmed := false
		for _, m := range config.Memberships {
			if m.Region == assignment.Region && m.PartitionID == assignment.PartitionID && m.Confirmed {
				confirmed = true
				hasConfirmedMembership = true
				break
			}
		}
		if confirmed {
			continue
		}
		for _, nodeID := range assignment.ReplicaNodeIDs {
			node, ok := config.NodeByID(nodeID)
			if !ok || node.State == cpconfig.NodeStateDraining {
				continue
			}
			if !lookup(node).received {
				missingReplicaHeartbeats = true
			}
		}
	}
	// The legacy warm-up rule preserves an old routing map until every static
	// replica has reported. Once Raft has confirmed a group, publish the live
	// view so that its membership can recover after a CP restart.
	if hasConfirmedMembership {
		missingReplicaHeartbeats = false
	}
	return Snapshot{
		Revision:                 snapshot.Revision,
		Config:                   config,
		MissingReplicaHeartbeats: missingReplicaHeartbeats,
		AppliedMemberships:       config.Memberships[firstApplied:],
	}
}
