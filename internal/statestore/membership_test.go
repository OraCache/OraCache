package statestore_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/statestore"
)

func TestAppliedMembershipProjectionAndOrdering(t *testing.T) {
	ctx := t.Context()
	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, cpconfigtest.ConfigWithCount(1, 1)); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0).UTC()
	member := func(id uint64, node, address string, voter, learner, outgoing bool) cpconfig.GroupMember {
		return cpconfig.GroupMember{PeerID: id, NodeID: node, Address: address, RaftAddress: "http://" + address + "/raft", Voter: voter, Learner: learner, OutgoingVoter: outgoing}
	}
	m := cpconfig.GroupMembership{GroupID: "eu/0/epoch-1", Region: "eu", PartitionID: 0, ReporterID: 1, Index: 10, Members: []cpconfig.GroupMember{
		member(1, "eu-1", "10.0.0.1:7000", true, false, false),
		member(2, "eu-2", "10.0.0.2:7000", true, false, false),
		member(3, "eu-3", "10.0.0.3:7000", false, true, false),
	}}
	beat := func(node string, role statestore.RaftRole, term uint64, ready bool, report *cpconfig.GroupMembership) {
		t.Helper()
		_, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{NodeID: node, Region: "eu", ReceivedAt: now, Partitions: []statestore.PartitionStatus{{PartitionID: 0, Role: role, Term: term, LeaderID: "eu-1", Ready: ready, Membership: report}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	// An unknown node is observable but has no route before a leader reports it.
	beat("eu-3", statestore.RaftRoleLearner, 3, false, nil)
	before, err := store.LiveConfig(ctx, now, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := before.Config.NodeByID("eu-3"); ok {
		t.Fatal("unknown heartbeat granted membership")
	}
	beat("eu-1", statestore.RaftRoleLeader, 3, true, &m)
	beat("eu-2", statestore.RaftRoleFollower, 3, true, nil)
	view, err := store.LiveConfig(ctx, now, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Config.Memberships) != 1 || !view.Config.Memberships[0].Confirmed {
		t.Fatalf("membership: %+v", view.Config.Memberships)
	}
	if err := view.Config.Validate(); err != nil {
		t.Fatal(err)
	}
	if node, ok := view.Config.NodeByID("eu-3"); !ok || node.State != cpconfig.NodeStateStarting {
		t.Fatalf("learner: %+v %t", node, ok)
	}
	a, _ := view.Config.Assignment("eu", 0)
	if slices.Contains(a.ReadyReplicaNodeIDs, "eu-3") {
		t.Fatalf("learner routed: %+v", a)
	}
	if !slices.Equal(a.ReplicaNodeIDs, []string{"eu-1", "eu-2"}) {
		t.Fatalf("static placement changed: %+v", a)
	}

	router, _ := gateway.NewRouter("eu")
	if err := router.ApplyView(view.Config); err != nil {
		t.Fatal(err)
	}
	addresses, err := router.AdminCandidates(m.GroupID, "eu", 0)
	if err != nil || slices.Contains(addresses, "10.0.0.3:7000") {
		t.Fatalf("admin voters: %v %v", addresses, err)
	}

	stale := m
	stale.Index = 9
	stale.Members = append([]cpconfig.GroupMember(nil), m.Members[:2]...)
	beat("eu-1", statestore.RaftRoleLeader, 3, true, &stale)
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	if view.Config.Memberships[0].Index != 10 {
		t.Fatalf("stale report rolled back: %+v", view.Config.Memberships)
	}
	conflict := m
	conflict.Members = append([]cpconfig.GroupMember(nil), m.Members[:2]...)
	_, err = store.UpsertHeartbeat(ctx, statestore.Heartbeat{NodeID: "eu-1", Region: "eu", ReceivedAt: now, Partitions: []statestore.PartitionStatus{{PartitionID: 0, Role: statestore.RaftRoleLeader, Term: 3, Ready: true, Membership: &conflict}}})
	if !errors.Is(err, statestore.ErrMembershipConflict) {
		t.Fatalf("equal-index conflict: %v", err)
	}

	m.Index = 11
	m.Members[2].Voter, m.Members[2].Learner = true, false
	beat("eu-1", statestore.RaftRoleLeader, 3, true, &m)
	beat("eu-3", statestore.RaftRoleFollower, 3, true, nil)
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	a, _ = view.Config.Assignment("eu", 0)
	if !slices.Contains(a.ReadyReplicaNodeIDs, "eu-3") {
		t.Fatalf("promoted voter missing: %+v", a)
	}
	if err := router.ApplyView(view.Config); err != nil {
		t.Fatal(err)
	}
	addresses, _ = router.AdminCandidates(m.GroupID, "eu", 0)
	if !slices.Contains(addresses, "10.0.0.3:7000") {
		t.Fatalf("new voter absent from admin route: %v", addresses)
	}

	m.Index = 12
	m.Joint = true
	m.Members[1].Voter, m.Members[1].OutgoingVoter = false, true
	m.RemovedIDs = []uint64{2}
	beat("eu-1", statestore.RaftRoleLeader, 3, true, &m)
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	if !view.Config.Memberships[0].Joint {
		t.Fatal("joint state hidden")
	}
	m.Index, m.Joint = 13, false
	m.Members = append(m.Members[:1], m.Members[2])
	m.RemovedIDs = []uint64{2}
	beat("eu-1", statestore.RaftRoleLeader, 3, true, &m)
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	a, _ = view.Config.Assignment("eu", 0)
	if slices.Contains(a.ReadyReplicaNodeIDs, "eu-2") || len(view.Config.Memberships[0].RemovedIDs) != 1 {
		t.Fatalf("removed peer still routed: %+v %+v", a, view.Config.Memberships)
	}
	if err := view.Config.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectInvalidMembershipBeforePublishing(t *testing.T) {
	ctx := t.Context()
	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, cpconfigtest.ConfigWithCount(1, 1)); err != nil {
		t.Fatal(err)
	}
	base := cpconfig.GroupMembership{GroupID: "eu/0/epoch-1", Region: "eu", Index: 10, ReporterID: 1, Members: []cpconfig.GroupMember{
		{PeerID: 1, NodeID: "eu-1", Address: "10.0.0.1:7000", RaftAddress: "http://10.0.0.1:7001/raft", Voter: true},
		{PeerID: 2, NodeID: "eu-2", Address: "10.0.0.2:7000", RaftAddress: "http://10.0.0.2:7001/raft", Learner: true},
	}}
	for _, mutate := range []func(*cpconfig.GroupMembership){
		func(m *cpconfig.GroupMembership) { m.Members[1].Voter = true },
		func(m *cpconfig.GroupMembership) { m.Members[1].Learner = false },
		func(m *cpconfig.GroupMembership) { m.RemovedIDs = []uint64{2} },
		func(m *cpconfig.GroupMembership) { m.RemovedIDs = []uint64{3, 3} },
	} {
		m := base
		m.Members = append([]cpconfig.GroupMember(nil), base.Members...)
		mutate(&m)
		_, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{NodeID: "eu-1", Region: "eu", ReceivedAt: time.Now(), Partitions: []statestore.PartitionStatus{{Role: statestore.RaftRoleLeader, Term: 2, Ready: true, Membership: &m}}})
		if !errors.Is(err, statestore.ErrInvalidHeartbeat) {
			t.Fatalf("invalid membership accepted: %+v: %v", m, err)
		}
		view, err := store.LiveConfig(ctx, time.Now(), time.Second)
		if err != nil || len(view.Config.Memberships) != 0 {
			t.Fatalf("invalid membership was published: %+v: %v", view.Config.Memberships, err)
		}
	}
}

func TestMembershipRecoveryAndLeaderOrdering(t *testing.T) {
	ctx := t.Context()
	config := cpconfigtest.ConfigWithCount(1, 1)
	config.Partitions[0].GroupID = "eu/0/first"
	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, config); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2000, 0).UTC()
	m := cpconfig.GroupMembership{GroupID: config.Partitions[0].GroupID, Region: "eu", ReporterID: 1, Index: 3, Members: []cpconfig.GroupMember{
		{PeerID: 1, NodeID: "eu-1", Address: "10.0.0.1:7000", RaftAddress: "http://10.0.0.1:7001/raft", Voter: true},
		{PeerID: 2, NodeID: "eu-2", Address: "10.0.0.2:7000", RaftAddress: "http://10.0.0.2:7001/raft", Voter: true},
	}}
	report := func(node string, term uint64, report *cpconfig.GroupMembership) error {
		_, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{NodeID: node, Region: "eu", ReceivedAt: now, Partitions: []statestore.PartitionStatus{{Role: statestore.RaftRoleLeader, Term: term, LeaderID: node, Ready: true, Membership: report}}})
		return err
	}
	if err := report("eu-1", 2, &m); err != nil {
		t.Fatal(err)
	}
	view, _ := store.LiveConfig(ctx, now, 10*time.Second)
	if !view.Config.Memberships[0].Confirmed || view.MissingReplicaHeartbeats {
		t.Fatalf("leader report should restore route without follower heartbeat: %+v", view)
	}

	newLeader := m
	newLeader.ReporterID = 2
	if err := report("eu-2", 3, &newLeader); err != nil {
		t.Fatal(err)
	}
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	a, _ := view.Config.Assignment("eu", 0)
	if view.Config.Memberships[0].ReporterID != 2 || !view.Config.Memberships[0].Confirmed || a.LeaderNodeID != "eu-2" {
		t.Fatalf("new leader not projected: %+v %+v", view.Config.Memberships, a)
	}
	if err := report("eu-1", 2, &m); err != nil {
		t.Fatal(err)
	}
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	if view.Config.Memberships[0].ReporterID != 2 {
		t.Fatal("late old leader rolled back reporter")
	}

	newConfig := config.Clone()
	newConfig.Revision = 2
	newConfig.Partitions[0].GroupID = "eu/0/second"
	if err := store.ApplyConfig(ctx, newConfig); err != nil {
		t.Fatal(err)
	}
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	if len(view.Config.Memberships) != 0 {
		t.Fatal("old incarnation survived explicit topology change")
	}
	m.GroupID = newConfig.Partitions[0].GroupID
	if err := report("eu-1", 4, &m); err != nil {
		t.Fatal(err)
	}
	view, _ = store.LiveConfig(ctx, now, 10*time.Second)
	if len(view.Config.Memberships) != 1 || view.Config.Memberships[0].GroupID != m.GroupID {
		t.Fatal("new incarnation not bound")
	}
}
