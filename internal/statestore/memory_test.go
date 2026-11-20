package statestore_test

import (
	"context"
	"errors"
	"slices"
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

func TestHeartbeatReceiptsTrackOnlyConfiguredNodeRegions(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	const nodeID = "extra"
	first := time.Unix(200, 0)
	apply := func(revision uint64, region string) {
		t.Helper()
		cfg := cpconfigtest.Config(revision)
		if region != "" {
			cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: nodeID, Region: region, Address: "10.0.0.3:7000"})
		}
		if err := store.ApplyConfig(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	beat := func(region string, receivedAt time.Time) {
		t.Helper()
		hb := statestore.Heartbeat{NodeID: nodeID, Region: region, ReceivedAt: receivedAt}
		if _, err := store.UpsertHeartbeat(ctx, hb); err != nil {
			t.Fatal(err)
		}
	}
	assertReceipts := func(want ...statestore.HeartbeatReceipt) {
		t.Helper()
		got, err := store.HeartbeatReceipts(ctx)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("HeartbeatReceipts = %v, %v; want %v", got, err, want)
		}
	}

	beat("eu", first)
	apply(1, "eu")
	assertReceipts()

	second := first.Add(time.Second)
	beat("eu", second)
	beat("us", second.Add(time.Second))
	apply(2, "eu")
	assertReceipts(statestore.HeartbeatReceipt{NodeID: nodeID, Region: "eu", ReceivedAt: second})

	apply(3, "us")
	assertReceipts()
	apply(4, "eu")
	assertReceipts()

	beat("eu", first.Add(4*time.Second))
	apply(5, "")
	assertReceipts()
	apply(6, "eu")
	assertReceipts()
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

func TestLiveConfigMarksStaleNodesDead(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	now := time.Unix(1000, 0).UTC()
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
		NodeID: "eu-1", Region: "eu", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 0, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
			{PartitionID: 2, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat(eu-1): %v", err)
	}
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
		NodeID: "eu-2", Region: "eu", ReceivedAt: now.Add(-16 * time.Second),
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 1, Role: statestore.RaftRoleLeader, LeaderID: "eu-2", Ready: true},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat(eu-2): %v", err)
	}

	view, err := store.LiveConfig(ctx, now, 15*time.Second)
	if err != nil {
		t.Fatalf("LiveConfig: %v", err)
	}
	if view.Revision != 1 || view.Config == nil {
		t.Fatalf("view = %+v", view)
	}
	for nodeID, want := range map[string]string{
		"eu-1": cpconfig.NodeStateReady,
		"eu-2": cpconfig.NodeStateDead,
		"us-1": cpconfig.NodeStateDead,
		"us-2": cpconfig.NodeStateDead,
	} {
		node, ok := view.Config.NodeByID(nodeID)
		if !ok {
			t.Fatalf("node %q missing", nodeID)
		}
		if node.State != want {
			t.Errorf("node %q state = %q, want %q", nodeID, node.State, want)
		}
	}
}

func TestLiveConfigProjectsLeaderFromHeartbeat(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	now := time.Unix(1000, 0).UTC()
	// По конфигурации лидер partition 0 — eu-1, но heartbeat говорит, что
	// лидер сейчас eu-2.
	for _, heartbeat := range []statestore.Heartbeat{
		{
			NodeID: "eu-1", Region: "eu", ReceivedAt: now,
			Partitions: []statestore.PartitionStatus{
				{PartitionID: 0, Role: statestore.RaftRoleFollower, LeaderID: "eu-2", Ready: true},
			},
		},
		{
			NodeID: "eu-2", Region: "eu", ReceivedAt: now,
			Partitions: []statestore.PartitionStatus{
				{PartitionID: 0, Role: statestore.RaftRoleLeader, LeaderID: "eu-2", Ready: true},
			},
		},
	} {
		if _, err := store.UpsertHeartbeat(ctx, heartbeat); err != nil {
			t.Fatalf("UpsertHeartbeat(%s): %v", heartbeat.NodeID, err)
		}
	}

	view, err := store.LiveConfig(ctx, now, 15*time.Second)
	if err != nil {
		t.Fatalf("LiveConfig: %v", err)
	}
	assignment, ok := view.Config.Assignment("eu", 0)
	if !ok {
		t.Fatal("assignment eu/0 missing")
	}
	if assignment.LeaderNodeID != "eu-2" {
		t.Fatalf("live leader = %q, want eu-2", assignment.LeaderNodeID)
	}
}

func TestLiveConfigChoosesLeaderAmongAssignedReplicas(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		nodeID     string
		term       uint64
		wantLeader string
		wantReady  bool
	}{
		{name: "higher term from an unassigned node", nodeID: "eu-3", term: 3, wantLeader: "eu-2", wantReady: true},
		{name: "lower term from an assigned replica", nodeID: "eu-1", term: 1, wantLeader: "eu-2", wantReady: true},
		{name: "higher term from an assigned replica", nodeID: "eu-1", term: 3, wantLeader: "eu-1", wantReady: true},
		{name: "conflicting leaders in the same term", nodeID: "eu-1", term: 2, wantLeader: "eu-1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := statestore.NewMemory()
			cfg := cpconfigtest.Config(1)
			cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: "eu-3", Region: "eu", Address: "10.0.0.3:7000"})
			if err := store.ApplyConfig(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			now := time.Unix(1000, 0).UTC()
			for _, report := range []struct {
				nodeID string
				term   uint64
			}{{"eu-2", 2}, {tt.nodeID, tt.term}} {
				if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
					NodeID: report.nodeID, Region: "eu", ReceivedAt: now,
					Partitions: []statestore.PartitionStatus{{
						PartitionID: 0, Role: statestore.RaftRoleLeader, Ready: true,
						LeaderID: report.nodeID, Term: report.term,
					}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			view, err := store.LiveConfig(ctx, now, 15*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			assignment, ok := view.Config.Assignment("eu", 0)
			if !ok || assignment.LeaderNodeID != tt.wantLeader || assignment.Ready != tt.wantReady ||
				assignment.Readable != tt.wantReady || assignment.Writable != tt.wantReady {
				t.Fatalf("assignment = %+v, want leader %q with readiness %t", assignment, tt.wantLeader, tt.wantReady)
			}
		})
	}
}

func TestLiveConfigUsesOnlyReadyReplicasInObservedTerm(t *testing.T) {
	store := statestore.NewMemory()
	if err := store.ApplyConfig(t.Context(), cpconfigtest.Config(1)); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0).UTC()
	report := func(nodeID string, term uint64, role statestore.RaftRole) {
		t.Helper()
		if _, err := store.UpsertHeartbeat(t.Context(), statestore.Heartbeat{
			NodeID: nodeID, Region: "eu", ReceivedAt: now,
			Partitions: []statestore.PartitionStatus{{PartitionID: 0, Term: term, Role: role, Ready: true}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	report("eu-1", 5, statestore.RaftRoleLeader)
	report("eu-2", 6, statestore.RaftRoleFollower)
	view, err := store.LiveConfig(t.Context(), now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := view.Config.Assignment("eu", 0)
	if !ok || a.Ready || a.Readable || a.Writable || a.ObservedTerm != 6 ||
		len(a.ReadyReplicaNodeIDs) != 1 || a.ReadyReplicaNodeIDs[0] != "eu-2" {
		t.Fatalf("expected only the term-6 follower to be eligible: %+v", a)
	}
}

func TestLiveConfigMarksNodeStartingWithoutReadyPartitions(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	now := time.Unix(1000, 0).UTC()
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
		NodeID: "eu-1", Region: "eu", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 0, Role: statestore.RaftRoleLeader, Ready: false},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}
	view, err := store.LiveConfig(ctx, now, 15*time.Second)
	if err != nil {
		t.Fatalf("LiveConfig: %v", err)
	}
	node, _ := view.Config.NodeByID("eu-1")
	if node.State != cpconfig.NodeStateStarting {
		t.Fatalf("eu-1 state = %q, want starting", node.State)
	}
}

func TestLiveConfigWithoutConfig(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	view, err := store.LiveConfig(context.Background(), time.Unix(1000, 0), 15*time.Second)
	if err != nil {
		t.Fatalf("LiveConfig: %v", err)
	}
	if view.Revision != 0 || view.Config != nil {
		t.Fatalf("view = %+v, want empty", view)
	}
}

func TestLiveConfigRejectsZeroTTL(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	if _, err := store.LiveConfig(context.Background(), time.Unix(1000, 0), 0); err == nil {
		t.Fatal("LiveConfig accepted zero TTL")
	}
}

func TestLiveConfigProjectsPartitionReadiness(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	ctx := context.Background()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	now := time.Unix(1000, 0).UTC()
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
		NodeID: "eu-1", Region: "eu", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 0, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
			{PartitionID: 2, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: false},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}

	view, err := store.LiveConfig(ctx, now, 15*time.Second)
	if err != nil {
		t.Fatalf("LiveConfig: %v", err)
	}
	ready, ok := view.Config.Assignment("eu", 0)
	if !ok || !ready.Ready || !ready.Readable || !ready.Writable {
		t.Fatalf("eu/0 = %+v, want ready/readable/writable", ready)
	}
	notReady, ok := view.Config.Assignment("eu", 2)
	if !ok || notReady.Ready || notReady.Readable || notReady.Writable {
		t.Fatalf("eu/2 = %+v, want not ready/readable/writable", notReady)
	}
	slave, ok := view.Config.Assignment("us", 0)
	if !ok || slave.Writable {
		t.Fatalf("us/0 = %+v, want non-writable", slave)
	}
}
