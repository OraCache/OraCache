// Package cpconfigtest provides deterministic valid configuration snapshots for
// Control Plane tests. It is not used by production code.
package cpconfigtest

import "github.com/OraCache/oracache/internal/cpconfig"

// PartitionCount is small enough for readable tests. Production uses
// cpconfig.DefaultPartitionCount.
const PartitionCount uint32 = 4

// Config returns a valid two-region snapshot: eu is master, us is slave.
// Leaders alternate between the two replicas so routing tests cover both.
func Config(revision uint64) *cpconfig.Config {
	return ConfigWithCount(revision, PartitionCount)
}

// ConfigWithCount returns a valid two-region snapshot with the given partition
// count. Production clusters use cpconfig.DefaultPartitionCount.
func ConfigWithCount(revision uint64, partitionCount uint32) *cpconfig.Config {
	cfg := &cpconfig.Config{
		Revision:       revision,
		PartitionCount: partitionCount,
		Regions: []cpconfig.Region{
			{ID: "eu", Role: cpconfig.RegionRoleMaster, GatewayEndpoints: []string{"gw-eu:50051"}},
			{ID: "us", Role: cpconfig.RegionRoleSlave, GatewayEndpoints: []string{"gw-us:50051"}},
		},
		Nodes: []cpconfig.Node{
			{ID: "eu-1", Region: "eu", Address: "10.0.0.1:7000", State: cpconfig.NodeStateReady, Zone: "eu-a"},
			{ID: "eu-2", Region: "eu", Address: "10.0.0.2:7000", State: cpconfig.NodeStateReady, Zone: "eu-b"},
			{ID: "us-1", Region: "us", Address: "10.0.1.1:7000", State: cpconfig.NodeStateReady, Zone: "us-a"},
			{ID: "us-2", Region: "us", Address: "10.0.1.2:7000", State: cpconfig.NodeStateReady, Zone: "us-b"},
		},
	}
	for p := uint32(0); p < partitionCount; p++ {
		euLeader, euReplica := "eu-1", "eu-2"
		usLeader, usReplica := "us-1", "us-2"
		if p%2 == 1 {
			euLeader, euReplica = euReplica, euLeader
			usLeader, usReplica = usReplica, usLeader
		}
		cfg.Partitions = append(cfg.Partitions,
			cpconfig.PartitionAssignment{
				PartitionID: p, Region: "eu", LeaderNodeID: euLeader,
				ReplicaNodeIDs: []string{euLeader, euReplica},
			},
			cpconfig.PartitionAssignment{
				PartitionID: p, Region: "us", LeaderNodeID: usLeader,
				ReplicaNodeIDs: []string{usLeader, usReplica},
			},
		)
	}
	return cfg
}
