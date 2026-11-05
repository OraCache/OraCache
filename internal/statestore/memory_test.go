package statestore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/statestore"
)

func testHeartbeat() statestore.Heartbeat {
	return statestore.Heartbeat{
		NodeID:         "eu-1",
		Region:         "eu",
		Instance:       "node-eu-1",
		SentAt:         time.Unix(100, 0).UTC(),
		ReceivedAt:     time.Unix(200, 0).UTC(),
		ConfigRevision: 4,
		Partitions: []statestore.PartitionStatus{
			{
				PartitionID:  0,
				Role:         statestore.RaftRoleLeader,
				Term:         7,
				CommitIndex:  10,
				AppliedIndex: 10,
				LeaderID:     "eu-1",
				Ready:        true,
			},
			{
				PartitionID: 1,
				Role:        statestore.RaftRoleFollower,
				Term:        7,
				LeaderID:    "eu-2",
				Ready:       true,
			},
		},
		Resources: statestore.Resources{CPUMillis: 4000, MemoryBytes: 1 << 30, DiskFreeBytes: 1 << 40},
	}
}

func TestUpsertHeartbeatStoresFullSnapshot(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	hb := testHeartbeat()

	revision, err := store.UpsertHeartbeat(ctx, hb)
	if err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}
	if revision != 0 {
		t.Fatalf("revision = %d, want 0 before config", revision)
	}

	got, ok, err := store.Heartbeat(ctx, "eu-1")
	if err != nil || !ok {
		t.Fatalf("Heartbeat = %+v, %v, %v", got, ok, err)
	}
	if !got.ReceivedAt.Equal(hb.ReceivedAt) {
		t.Fatalf("ReceivedAt = %v, want %v", got.ReceivedAt, hb.ReceivedAt)
	}
	if len(got.Partitions) != 2 || got.Partitions[0].Role != statestore.RaftRoleLeader {
		t.Fatalf("partitions = %+v", got.Partitions)
	}
	if got.Resources.MemoryBytes != 1<<30 {
		t.Fatalf("resources = %+v", got.Resources)
	}

	if _, ok, err := store.Heartbeat(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing heartbeat = ok %v, err %v", ok, err)
	}
}

func TestHeartbeatDoesNotChangeConfigRevision(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(3)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}

	revision, err := store.UpsertHeartbeat(ctx, testHeartbeat())
	if err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}
	if revision != 3 {
		t.Fatalf("heartbeat revision = %d, want 3", revision)
	}
	snapshot, err := store.Config(ctx)
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if snapshot.Revision != 3 {
		t.Fatalf("config revision changed to %d by heartbeat", snapshot.Revision)
	}
}

func TestHeartbeatInputAndOutputAreCopied(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	hb := testHeartbeat()
	if _, err := store.UpsertHeartbeat(ctx, hb); err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}
	hb.Partitions[0].LeaderID = "mutated"
	got, _, _ := store.Heartbeat(ctx, "eu-1")
	if got.Partitions[0].LeaderID == "mutated" {
		t.Fatal("store aliases heartbeat input")
	}
	got.Partitions[0].LeaderID = "mutated-again"
	got2, _, _ := store.Heartbeat(ctx, "eu-1")
	if got2.Partitions[0].LeaderID == "mutated-again" {
		t.Fatal("store returns an aliased heartbeat")
	}
}

func TestHeartbeatValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*statestore.Heartbeat)
	}{
		{name: "empty_node", mutate: func(h *statestore.Heartbeat) { h.NodeID = "" }},
		{name: "empty_region", mutate: func(h *statestore.Heartbeat) { h.Region = "" }},
		{name: "partition_out_of_range", mutate: func(h *statestore.Heartbeat) {
			h.Partitions[0].PartitionID = cpconfig.DefaultPartitionCount
		}},
		{name: "duplicate_partition", mutate: func(h *statestore.Heartbeat) {
			h.Partitions[1].PartitionID = h.Partitions[0].PartitionID
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := statestore.NewMemory()
			hb := testHeartbeat()
			tt.mutate(&hb)
			if _, err := store.UpsertHeartbeat(context.Background(), hb); !errors.Is(err, statestore.ErrInvalidHeartbeat) {
				t.Fatalf("error = %v, want ErrInvalidHeartbeat", err)
			}
		})
	}
}

func TestStoreHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.UpsertHeartbeat(ctx, testHeartbeat()); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpsertHeartbeat error = %v, want context.Canceled", err)
	}
	if _, _, err := store.Heartbeat(ctx, "eu-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Heartbeat error = %v, want context.Canceled", err)
	}
	if _, err := store.Config(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Config error = %v, want context.Canceled", err)
	}
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("ApplyConfig error = %v, want context.Canceled", err)
	}
}

func TestApplyConfigRevisionRules(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(2)); err != nil {
		t.Fatalf("ApplyConfig(2): %v", err)
	}
	for _, revision := range []uint64{1, 2} {
		err := store.ApplyConfig(ctx, cpconfigtest.Config(revision))
		if !errors.Is(err, cpconfig.ErrStaleRevision) {
			t.Fatalf("ApplyConfig(%d) error = %v, want ErrStaleRevision", revision, err)
		}
	}
	snapshot, err := store.Config(ctx)
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if snapshot.Revision != 2 || snapshot.Config == nil || snapshot.Config.Revision != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestApplyConfigRejectsPartitionCountChange(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig(1): %v", err)
	}
	err := store.ApplyConfig(ctx, cpconfigtest.ConfigWithCount(2, cpconfigtest.PartitionCount+1))
	if !errors.Is(err, cpconfig.ErrPartitionCountChanged) {
		t.Fatalf("error = %v, want ErrPartitionCountChanged", err)
	}
	if snapshot, _ := store.Config(ctx); snapshot.Revision != 1 {
		t.Fatalf("revision changed to %d", snapshot.Revision)
	}
}

func TestApplyConfigRejectsInvalidAndKeepsLastKnownGood(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig(1): %v", err)
	}
	bad := cpconfigtest.Config(2)
	bad.Regions[0].Role = cpconfig.RegionRoleUnspecified
	if err := store.ApplyConfig(ctx, bad); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("error = %v, want ErrInvalidConfig", err)
	}
	snapshot, _ := store.Config(ctx)
	if snapshot.Revision != 1 {
		t.Fatalf("revision = %d, want last-known-good 1", snapshot.Revision)
	}
}

func TestConfigCopiesAreIndependent(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	cfg := cpconfigtest.Config(1)
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	cfg.Nodes[0].Address = "mutated:1"
	first, _ := store.Config(ctx)
	first.Config.Nodes[0].Address = "mutated:2"
	second, _ := store.Config(ctx)
	if second.Config.Nodes[0].Address == "mutated:1" || second.Config.Nodes[0].Address == "mutated:2" {
		t.Fatalf("store config aliased: %q", second.Config.Nodes[0].Address)
	}
}

func TestStoreConcurrentHeartbeatsAndSnapshots(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hb := testHeartbeat()
			hb.NodeID = "node-" + string(rune('a'+i))
			_, _ = store.UpsertHeartbeat(ctx, hb)
		}(i)
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.Config(ctx)
		}()
	}
	wg.Wait()
}
