package cpconfig

import (
	"fmt"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
)

// FromProto converts and validates a protobuf snapshot. It never mutates the
// input and never returns a partially valid config.
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
			PartitionID:    a.GetPartitionId(),
			Region:         a.GetRegion(),
			LeaderNodeID:   a.GetLeaderNodeId(),
			ReplicaNodeIDs: append([]string(nil), a.GetReplicaNodeIds()...),
		})
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ToProto returns an independent protobuf projection of the snapshot. The
// caller is expected to pass a validated Config.
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
			PartitionId:    a.PartitionID,
			Region:         a.Region,
			LeaderNodeId:   a.LeaderNodeID,
			ReplicaNodeIds: append([]string(nil), a.ReplicaNodeIDs...),
		})
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
