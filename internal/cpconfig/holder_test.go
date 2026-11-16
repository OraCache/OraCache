package cpconfig_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

func TestHolderApplyAndCurrent(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	if h.Current() != nil {
		t.Fatal("new holder must be empty")
	}
	if h.Revision() != 0 {
		t.Fatalf("empty Revision() = %d, want 0", h.Revision())
	}

	cfg := cpconfigtest.Config(1)
	if err := h.Apply(cfg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := h.Current(); got == nil || got.Revision != 1 {
		t.Fatalf("Current() = %+v", got)
	}
	if h.Revision() != 1 {
		t.Fatalf("Revision() = %d, want 1", h.Revision())
	}
}

func TestHolderRejectsStaleRevision(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	if err := h.Apply(cpconfigtest.Config(2)); err != nil {
		t.Fatalf("Apply(2): %v", err)
	}
	for _, revision := range []uint64{1, 2} {
		err := h.Apply(cpconfigtest.Config(revision))
		if !errors.Is(err, cpconfig.ErrStaleRevision) {
			t.Fatalf("Apply(%d) error = %v, want ErrStaleRevision", revision, err)
		}
		if h.Revision() != 2 {
			t.Fatalf("holder revision changed to %d", h.Revision())
		}
	}
}

func TestHolderRejectsPartitionCountChange(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	if err := h.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply(1): %v", err)
	}
	next := cpconfigtest.Config(2)
	next.PartitionCount = cpconfigtest.PartitionCount + 1
	next.Partitions = append(next.Partitions,
		cpconfig.PartitionAssignment{
			PartitionID: cpconfigtest.PartitionCount, Region: "eu",
			LeaderNodeID: "eu-1", ReplicaNodeIDs: []string{"eu-1", "eu-2"},
		},
		cpconfig.PartitionAssignment{
			PartitionID: cpconfigtest.PartitionCount, Region: "us",
			LeaderNodeID: "us-1", ReplicaNodeIDs: []string{"us-1", "us-2"},
		},
	)
	if err := next.Validate(); err != nil {
		t.Fatalf("test config is invalid: %v", err)
	}
	err := h.Apply(next)
	if !errors.Is(err, cpconfig.ErrPartitionCountChanged) {
		t.Fatalf("Apply(changed partition count) error = %v, want ErrPartitionCountChanged", err)
	}
	if h.Revision() != 1 {
		t.Fatalf("holder revision changed to %d", h.Revision())
	}
}

func TestHolderRejectsInvalidConfigAndKeepsLastKnownGood(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	good := cpconfigtest.Config(1)
	if err := h.Apply(good); err != nil {
		t.Fatalf("Apply(good): %v", err)
	}
	bad := cpconfigtest.Config(2)
	bad.Regions[0].Role = cpconfig.RegionRoleUnspecified
	if err := h.Apply(bad); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("Apply(bad) error = %v, want ErrInvalidConfig", err)
	}
	if h.Revision() != 1 {
		t.Fatalf("last-known-good replaced: revision = %d", h.Revision())
	}
}

func TestHolderCopiesAppliedConfig(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	cfg := cpconfigtest.Config(1)
	if err := h.Apply(cfg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg.Nodes[0].Address = "mutated:1"
	cfg.Partitions[0].ReplicaNodeIDs[0] = "mutated"
	got := h.Current()
	if got.Nodes[0].Address == "mutated:1" || got.Partitions[0].ReplicaNodeIDs[0] == "mutated" {
		t.Fatal("holder aliases the applied config")
	}
}

func TestHolderConcurrentApplyKeepsMonotonicRevision(t *testing.T) {
	t.Parallel()
	h := cpconfig.NewHolder()
	var wg sync.WaitGroup
	for i := uint64(1); i <= 32; i++ {
		wg.Add(1)
		go func(revision uint64) {
			defer wg.Done()
			_ = h.Apply(cpconfigtest.Config(revision))
		}(i)
	}
	wg.Wait()
	if h.Revision() < 32 {
		t.Fatalf("final revision = %d, want at least 32", h.Revision())
	}
}
