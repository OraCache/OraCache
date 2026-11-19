package cpconfig

// SyntheticDevCluster строит детерминированный dev-кластер для локального
// стенда Control Plane: два региона (eu — master, us — slave), по две ноды в
// регионе, все партиции с RF=2 и чередованием лидеров.
//
// Функция предназначена только для разработки и тестов: production-снапшот
// приходит из StateStore (bootstrap JSON, позже Controller/Scheduler).
func SyntheticDevCluster() *Config {
	const (
		partitionCount = DefaultPartitionCount
		perRegion      = 2
	)

	regions := []Region{
		{ID: "eu", Role: RegionRoleMaster, GatewayEndpoints: []string{"127.0.0.1:50061"}},
		{ID: "us", Role: RegionRoleSlave, GatewayEndpoints: []string{"127.0.0.1:50062"}},
	}
	nodeAddresses := map[string]string{
		"eu-1": "127.0.0.1:50151",
		"eu-2": "127.0.0.1:50152",
		"us-1": "127.0.0.1:50153",
		"us-2": "127.0.0.1:50154",
	}
	zones := map[string]string{"eu-1": "eu-a", "eu-2": "eu-b", "us-1": "us-a", "us-2": "us-b"}

	nodes := make([]Node, 0, len(regions)*perRegion)
	for _, region := range regions {
		for i := 1; i <= perRegion; i++ {
			id := region.ID + "-" + string(rune('0'+i))
			nodes = append(nodes, Node{
				ID:      id,
				Region:  region.ID,
				Address: nodeAddresses[id],
				State:   NodeStateReady,
				Zone:    zones[id],
			})
		}
	}

	partitions := make([]PartitionAssignment, 0, int(partitionCount)*len(regions))
	for p := uint32(0); p < partitionCount; p++ {
		for _, region := range regions {
			first := region.ID + "-1"
			second := region.ID + "-2"
			leader, follower := first, second
			if p%2 == 1 {
				leader, follower = second, first
			}
			partitions = append(partitions, PartitionAssignment{
				PartitionID:    p,
				Region:         region.ID,
				LeaderNodeID:   leader,
				ReplicaNodeIDs: []string{leader, follower},
				Ready:          true,
				Readable:       true,
				Writable:       region.Role == RegionRoleMaster,
			})
		}
	}

	return &Config{
		Revision:       1,
		PartitionCount: partitionCount,
		Regions:        regions,
		Nodes:          nodes,
		Partitions:     partitions,
	}
}
