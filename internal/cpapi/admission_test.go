package cpapi

import (
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

func TestRestrictRegionalReadsClosesLeaderAndFallbackRoutes(t *testing.T) {
	config := cpconfigtest.Config(1)
	for i := range config.Partitions {
		config.Partitions[i].ReadyReplicaNodeIDs = config.Partitions[i].ReplicaNodeIDs
	}
	restrictRegionalReads(config)
	for _, group := range config.Partitions {
		if group.Region == "us" {
			if group.Readable || len(group.ReadyReplicaNodeIDs) != 0 {
				t.Fatalf("follower region routing allowed: %+v", group)
			}
		} else if !group.Readable || len(group.ReadyReplicaNodeIDs) == 0 {
			t.Fatalf("master region routing suppressed: %+v", group)
		}
	}
}
