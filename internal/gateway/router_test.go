package gateway_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
)

func TestRouterResolvesRegionScopedLeaders(t *testing.T) {
	t.Parallel()
	eu, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter(eu): %v", err)
	}
	if err := eu.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if eu.Region() != "eu" || eu.Revision() != 1 || eu.MasterRegion() != "eu" {
		t.Fatalf("router metadata = region %q revision %d master %q", eu.Region(), eu.Revision(), eu.MasterRegion())
	}
	if eu.PartitionCount() != cpconfigtest.PartitionCount {
		t.Fatalf("partition count = %d", eu.PartitionCount())
	}

	for p := uint32(0); p < cpconfigtest.PartitionCount; p++ {
		wantNode, wantAddr := "eu-1", "10.0.0.1:7000"
		if p%2 == 1 {
			wantNode, wantAddr = "eu-2", "10.0.0.2:7000"
		}
		leader, err := eu.GetLeader(p)
		if err != nil {
			t.Fatalf("GetLeader(%d): %v", p, err)
		}
		if leader.NodeID != wantNode || leader.Address != wantAddr || leader.Region != "eu" {
			t.Fatalf("GetLeader(%d) = %+v, want %s/%s", p, leader, wantNode, wantAddr)
		}
		writeLeader, err := eu.WriteLeader(p)
		if err != nil {
			t.Fatalf("WriteLeader(%d): %v", p, err)
		}
		if writeLeader != leader {
			t.Fatalf("eu WriteLeader(%d) = %+v, want local %+v", p, writeLeader, leader)
		}
	}

	us, err := gateway.NewRouter("us")
	if err != nil {
		t.Fatalf("NewRouter(us): %v", err)
	}
	if err := us.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	local, err := us.GetLeader(0)
	if err != nil {
		t.Fatalf("us GetLeader: %v", err)
	}
	if local.Region != "us" || local.NodeID != "us-1" {
		t.Fatalf("us local leader = %+v", local)
	}
	write, err := us.WriteLeader(0)
	if err != nil {
		t.Fatalf("us WriteLeader: %v", err)
	}
	if write.Region != "eu" || write.NodeID != "eu-1" {
		t.Fatalf("us write leader = %+v, want eu master", write)
	}
	if local == write {
		t.Fatal("slave local leader equals master write leader")
	}
}

func TestRouterKeyRoutingUsesStablePartition(t *testing.T) {
	t.Parallel()
	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	partitionID, err := router.Partition("profile:42")
	if err != nil {
		t.Fatalf("Partition: %v", err)
	}
	// xxhash64("profile:42") % 4 == 3; partition 3 has eu-2 as leader.
	if partitionID != 3 {
		t.Fatalf("partition = %d, want 3", partitionID)
	}
	leader, err := router.GetLeaderForKey("profile:42")
	if err != nil {
		t.Fatalf("GetLeaderForKey: %v", err)
	}
	if leader.NodeID != "eu-2" || leader.Address != "10.0.0.2:7000" {
		t.Fatalf("leader = %+v, want eu-2", leader)
	}
}

func TestRouterRejectsStaleRevisionAndPartitionCountChange(t *testing.T) {
	t.Parallel()
	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if err := router.Apply(cpconfigtest.Config(2)); err != nil {
		t.Fatalf("Apply(2): %v", err)
	}
	for _, revision := range []uint64{1, 2} {
		if err := router.Apply(cpconfigtest.Config(revision)); !errors.Is(err, gateway.ErrStaleRevision) {
			t.Fatalf("Apply(%d) error = %v, want ErrStaleRevision", revision, err)
		}
	}
	if err := router.Apply(cpconfigtest.ConfigWithCount(3, cpconfigtest.PartitionCount+1)); !errors.Is(err, gateway.ErrPartitionCountChanged) {
		t.Fatalf("partition count error = %v, want ErrPartitionCountChanged", err)
	}
	if router.Revision() != 2 {
		t.Fatalf("revision = %d, want 2", router.Revision())
	}
}

func TestRouterRejectsUnknownRegionAndInvalidConfig(t *testing.T) {
	t.Parallel()
	router, err := gateway.NewRouter("moon")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if err := router.Apply(cpconfigtest.Config(1)); !errors.Is(err, gateway.ErrUnknownRegion) {
		t.Fatalf("unknown region error = %v, want ErrUnknownRegion", err)
	}
	if _, err := gateway.NewRouter(""); !errors.Is(err, gateway.ErrUnknownRegion) {
		t.Fatalf("empty region error = %v, want ErrUnknownRegion", err)
	}

	router, _ = gateway.NewRouter("eu")
	bad := cpconfigtest.Config(1)
	bad.Regions[0].Role = cpconfig.RegionRoleUnspecified
	if err := router.Apply(bad); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v, want ErrInvalidConfig", err)
	}
	if err := router.Apply(nil); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("nil config error = %v, want ErrInvalidConfig", err)
	}
}

func TestRouterWithoutConfig(t *testing.T) {
	t.Parallel()
	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	if _, err := router.GetLeader(0); !errors.Is(err, gateway.ErrNoConfig) {
		t.Fatalf("GetLeader error = %v, want ErrNoConfig", err)
	}
	if _, err := router.WriteLeaderForKey("key"); !errors.Is(err, gateway.ErrNoConfig) {
		t.Fatalf("WriteLeaderForKey error = %v, want ErrNoConfig", err)
	}
	if _, err := router.Partition("key"); !errors.Is(err, gateway.ErrNoConfig) {
		t.Fatalf("Partition error = %v, want ErrNoConfig", err)
	}
	if router.Revision() != 0 || router.PartitionCount() != 0 || router.MasterRegion() != "" {
		t.Fatalf("empty router metadata = %d/%d/%q", router.Revision(), router.PartitionCount(), router.MasterRegion())
	}
}

func TestRouterRejectsOutOfRangePartition(t *testing.T) {
	t.Parallel()
	router, _ := gateway.NewRouter("eu")
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := router.GetLeader(cpconfigtest.PartitionCount); !errors.Is(err, gateway.ErrPartitionOutOfRange) {
		t.Fatalf("GetLeader error = %v, want ErrPartitionOutOfRange", err)
	}
}

func TestRouterDoesNotAliasAppliedConfig(t *testing.T) {
	t.Parallel()
	cfg := cpconfigtest.Config(1)
	router, _ := gateway.NewRouter("eu")
	if err := router.Apply(cfg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg.Nodes[0].Address = "mutated:1"
	leader, err := router.GetLeader(0)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if leader.Address == "mutated:1" {
		t.Fatal("router aliases the applied config")
	}
}

func TestRouterAtomicSwapUnderConcurrentReads(t *testing.T) {
	t.Parallel()
	router, _ := gateway.NewRouter("eu")
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	next := cpconfigtest.Config(2)
	for i := range next.Partitions {
		if next.Partitions[i].Region != "eu" {
			continue
		}
		replicas := next.Partitions[i].ReplicaNodeIDs
		next.Partitions[i].LeaderNodeID = replicas[1]
		next.Partitions[i].ReplicaNodeIDs = []string{replicas[1], replicas[0]}
	}

	errCh := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				leader, err := router.GetLeader(uint32(j % int(cpconfigtest.PartitionCount)))
				if err != nil {
					errCh <- err
					return
				}
				if leader.NodeID != "eu-1" && leader.NodeID != "eu-2" {
					errCh <- errors.New("unexpected leader during swap")
					return
				}
			}
		}()
	}
	if err := router.Apply(next); err != nil {
		wg.Wait()
		t.Fatalf("Apply(next): %v", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent read: %v", err)
	}
	if router.Revision() != 2 {
		t.Fatalf("revision = %d, want 2", router.Revision())
	}
}
