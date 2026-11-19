// Package cpconfigtest предоставляет детерминированные валидные snapshot
// конфигурации для тестов Control Plane. В production-коде не используется.
package cpconfigtest

import "github.com/OraCache/oracache/internal/cpconfig"

// PartitionCount достаточно мал для читаемых тестов. В production используется
// cpconfig.DefaultPartitionCount.
const PartitionCount uint32 = 4

// Config возвращает валидный snapshot двух регионов: eu — master, us — slave.
// Лидеры чередуются между двумя репликами, чтобы routing-тесты покрывали оба случая.
func Config(revision uint64) *cpconfig.Config {
	return ConfigWithCount(revision, PartitionCount)
}

// ConfigWithCount возвращает валидный snapshot двух регионов с заданным числом
// партиций. В production-кластерах используется cpconfig.DefaultPartitionCount.
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
				Ready:          true, Readable: true, Writable: true,
			},
			cpconfig.PartitionAssignment{
				PartitionID: p, Region: "us", LeaderNodeID: usLeader,
				ReplicaNodeIDs: []string{usLeader, usReplica},
				Ready:          true, Readable: true,
			},
		)
	}
	return cfg
}
