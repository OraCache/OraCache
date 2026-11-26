package cpconfig

import (
	"fmt"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
)

// FromProto преобразует и валидирует protobuf snapshot. Он не мутирует входные
// данные и никогда не возвращает частично валидную конфигурацию.
func FromProto(p *controlplanev1.ClusterConfig) (*Config, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil protobuf config", ErrInvalidConfig)
	}
	cfg := &Config{
		Revision:       p.GetRevision(),
		PartitionCount: p.GetPartitionCount(),
		Regions:        make([]Region, 0, len(p.GetRegions())),
		Nodes:          make([]Node, 0, len(p.GetNodes())),
		Partitions:     make([]PartitionAssignment, 0, len(p.GetPartitions())),
	}
	for i, r := range p.GetRegions() {
		if r == nil {
			return nil, fmt.Errorf("%w: region %d is nil", ErrInvalidConfig, i)
		}
		role, err := regionRoleFromProto(r.GetRole())
		if err != nil {
			return nil, fmt.Errorf("region %q: %w", r.GetRegionId(), err)
		}
		cfg.Regions = append(cfg.Regions, Region{
			ID:               r.GetRegionId(),
			Role:             role,
			GatewayEndpoints: append([]string(nil), r.GetGatewayEndpoints()...),
		})
	}
	for i, n := range p.GetNodes() {
		if n == nil {
			return nil, fmt.Errorf("%w: node %d is nil", ErrInvalidConfig, i)
		}
		cfg.Nodes = append(cfg.Nodes, Node{
			ID:            n.GetNodeId(),
			Region:        n.GetRegion(),
			Address:       n.GetAddress(),
			State:         n.GetState(),
			CapacityBytes: n.GetCapacityBytes(),
			Zone:          n.GetZone(),
		})
	}
	for i, a := range p.GetPartitions() {
		if a == nil {
			return nil, fmt.Errorf("%w: assignment %d is nil", ErrInvalidConfig, i)
		}
		cfg.Partitions = append(cfg.Partitions, PartitionAssignment{
			PartitionID:         a.GetPartitionId(),
			Region:              a.GetRegion(),
			GroupID:             a.GetGroupId(),
			LeaderNodeID:        a.GetLeaderNodeId(),
			ReplicaNodeIDs:      append([]string(nil), a.GetReplicaNodeIds()...),
			Ready:               a.GetReady(),
			Readable:            a.GetReadable(),
			Writable:            a.GetWritable(),
			ObservedTerm:        a.GetObservedTerm(),
			ReadyReplicaNodeIDs: append([]string(nil), a.GetReadyReplicaNodeIds()...),
			LeaderConflict:      a.GetLeaderConflict(),
		})
	}
	for _, m := range p.GetMemberships() {
		if m == nil {
			return nil, fmt.Errorf("%w: nil membership", ErrInvalidConfig)
		}
		item := GroupMembership{GroupID: m.GetGroupId(), Region: m.GetRegion(), PartitionID: m.GetPartitionId(), ReporterID: m.GetReporterPeerId(), Term: m.GetTerm(), Index: m.GetConfigIndex(), Joint: m.GetJoint(), RemovedIDs: append([]uint64(nil), m.GetRemovedIds()...), Confirmed: m.GetConfirmed()}
		for _, peer := range m.GetMembers() {
			if peer == nil {
				return nil, fmt.Errorf("%w: nil member", ErrInvalidConfig)
			}
			item.Members = append(item.Members, GroupMember{PeerID: peer.GetPeerId(), NodeID: peer.GetNodeId(), Address: peer.GetAddress(), RaftAddress: peer.GetRaftAddress(), Voter: peer.GetVoter(), Learner: peer.GetLearner(), OutgoingVoter: peer.GetOutgoingVoter()})
		}
		cfg.Memberships = append(cfg.Memberships, item)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ToProto возвращает независимую protobuf-проекцию snapshot. Вызывающий код
// должен передавать валидный Config.
func (c *Config) ToProto() *controlplanev1.ClusterConfig {
	if c == nil {
		return nil
	}
	out := &controlplanev1.ClusterConfig{
		Revision:       c.Revision,
		PartitionCount: c.PartitionCount,
		Regions:        make([]*controlplanev1.Region, 0, len(c.Regions)),
		Nodes:          make([]*controlplanev1.Node, 0, len(c.Nodes)),
		Partitions:     make([]*controlplanev1.PartitionAssignment, 0, len(c.Partitions)),
	}
	for i := range c.Regions {
		r := c.Regions[i]
		out.Regions = append(out.Regions, &controlplanev1.Region{
			RegionId:         r.ID,
			Role:             regionRoleToProto(r.Role),
			GatewayEndpoints: append([]string(nil), r.GatewayEndpoints...),
		})
	}
	for i := range c.Nodes {
		n := c.Nodes[i]
		out.Nodes = append(out.Nodes, &controlplanev1.Node{
			NodeId:        n.ID,
			Region:        n.Region,
			Address:       n.Address,
			State:         n.State,
			CapacityBytes: n.CapacityBytes,
			Zone:          n.Zone,
		})
	}
	for i := range c.Partitions {
		a := c.Partitions[i]
		out.Partitions = append(out.Partitions, &controlplanev1.PartitionAssignment{
			PartitionId:         a.PartitionID,
			Region:              a.Region,
			GroupId:             a.GroupID,
			LeaderNodeId:        a.LeaderNodeID,
			ReplicaNodeIds:      append([]string(nil), a.ReplicaNodeIDs...),
			Ready:               a.Ready,
			Readable:            a.Readable,
			Writable:            a.Writable,
			ObservedTerm:        a.ObservedTerm,
			ReadyReplicaNodeIds: append([]string(nil), a.ReadyReplicaNodeIDs...),
			LeaderConflict:      a.LeaderConflict,
		})
	}
	for _, m := range c.Memberships {
		item := &controlplanev1.GroupMembership{GroupId: m.GroupID, Region: m.Region, PartitionId: m.PartitionID, ReporterPeerId: m.ReporterID, Term: m.Term, ConfigIndex: m.Index, Joint: m.Joint, RemovedIds: append([]uint64(nil), m.RemovedIDs...), Confirmed: m.Confirmed}
		for _, peer := range m.Members {
			item.Members = append(item.Members, &controlplanev1.GroupMember{PeerId: peer.PeerID, NodeId: peer.NodeID, Address: peer.Address, RaftAddress: peer.RaftAddress, Voter: peer.Voter, Learner: peer.Learner, OutgoingVoter: peer.OutgoingVoter})
		}
		out.Memberships = append(out.Memberships, item)
	}
	return out
}

func regionRoleFromProto(role controlplanev1.RegionRole) (RegionRole, error) {
	switch role {
	case controlplanev1.RegionRole_REGION_ROLE_MASTER:
		return RegionRoleMaster, nil
	case controlplanev1.RegionRole_REGION_ROLE_SLAVE:
		return RegionRoleSlave, nil
	default:
		return RegionRoleUnspecified, fmt.Errorf("%w: unspecified or unknown region role %d", ErrInvalidConfig, role)
	}
}

func regionRoleToProto(role RegionRole) controlplanev1.RegionRole {
	switch role {
	case RegionRoleMaster:
		return controlplanev1.RegionRole_REGION_ROLE_MASTER
	case RegionRoleSlave:
		return controlplanev1.RegionRole_REGION_ROLE_SLAVE
	default:
		return controlplanev1.RegionRole_REGION_ROLE_UNSPECIFIED
	}
}
