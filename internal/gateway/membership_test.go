package gateway_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
)

type membershipReader struct {
	observation cpapi.MembershipObservation
	err         error
}

func (r membershipReader) ReadConfirmedMembership(context.Context, cpapi.RaftGroup) (cpapi.MembershipObservation, error) {
	return r.observation, r.err
}

func TestGatewayAdmitsVoterAfterConfirmedMembershipAndFreshReadiness(t *testing.T) {
	ctx := t.Context()
	store := statestore.NewMemory()
	config := forwarderConfig()
	config.Nodes = append(config.Nodes, cpconfig.Node{ID: "n4", Region: "r", Address: "a4"})
	if err := store.ApplyConfig(ctx, config); err != nil {
		t.Fatal(err)
	}
	api := cpapi.New(store)
	group := cpapi.RaftGroup{Region: "r", GroupID: "r/0/incarnation"}
	core := []string{"n1", "n2", "n3"}
	n1 := &fakeNode{err: notLeader(t, "a4")}
	n2 := &fakeNode{err: notLeader(t, "a4")}
	n3 := &fakeNode{rev: 3}
	n4 := &fakeNode{rev: 4}
	forwarder := newForwarder(t, map[string]*fakeNode{"a1": n1, "a2": n2, "a3": n3, "a4": n4})
	heartbeat := func(nodeID, groupID string, term, index uint64, role controlplanev1.RaftRole, leader string) {
		t.Helper()
		_, err := api.ReportHeartbeat(ctx, &controlplanev1.HeartbeatRequest{
			NodeId: nodeID, Region: "r", Partitions: []*controlplanev1.PartitionStatus{{
				GroupId: groupID, MembershipIndex: index, Term: term, CommitIndex: index,
				AppliedIndex: index, Role: role, LeaderId: leader, Ready: true,
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	refresh := func() *cpconfig.Config {
		t.Helper()
		resp, err := api.GetConfig(ctx, &controlplanev1.GetConfigRequest{
			NodeId: "gateway", Kind: controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
			KnownRevision: forwarder.Router.Revision(),
		})
		if err != nil {
			t.Fatal(err)
		}
		config, err := cpconfig.FromProto(resp.GetConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := forwarder.Router.ApplyView(config); err != nil {
			t.Fatal(err)
		}
		return config
	}
	wantPut := func(nodeID string) {
		t.Helper()
		result, err := forwarder.Put(ctx, &pb.PutRequest{Key: "test"})
		if err != nil || result.GetStatus() != pb.PutStatus_PUT_STATUS_OK || result.GetServedBy().GetInstance() != nodeID {
			t.Fatalf("PUT served by %q: result %v, error %v", nodeID, result, err)
		}
	}
	reconcile := func(index, term uint64, leader string, voters []string) error {
		return api.ReconcileMembership(ctx, group, membershipReader{observation: cpapi.MembershipObservation{
			Group: group, ResponderNodeID: leader, LeaderNodeID: leader,
			Index: index, Term: term, VoterNodeIDs: voters,
		}})
	}

	leader := controlplanev1.RaftRole_RAFT_ROLE_LEADER
	follower := controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER
	learner := controlplanev1.RaftRole_RAFT_ROLE_LEARNER
	heartbeat("n1", group.GroupID, 7, 40, leader, "n1")
	heartbeat("n2", group.GroupID, 7, 40, follower, "n1")
	heartbeat("n3", group.GroupID, 7, 40, follower, "n1")
	heartbeat("n4", group.GroupID, 7, 40, learner, "n1")
	if err := reconcile(40, 7, "n1", core); err != nil {
		t.Fatal(err)
	}
	refresh()
	wantPut("n3")
	if n4.calls != 0 {
		t.Fatalf("Gateway contacted learner %d times", n4.calls)
	}

	voters := append(slices.Clone(core), "n4")
	heartbeat("n1", group.GroupID, 7, 42, leader, "n1")
	heartbeat("n3", group.GroupID, 7, 42, follower, "n1")
	heartbeat("n4", "", 7, 42, follower, "n1")
	if err := reconcile(42, 7, "n1", voters); err != nil {
		t.Fatal(err)
	}
	missing := refresh().Partitions[0]
	if slices.Contains(missing.ReadyReplicaNodeIDs, "n4") {
		t.Fatal("heartbeat missing group identity admitted n4")
	}

	heartbeat("n4", group.GroupID, 7, 42, follower, "n1")
	err := api.ReconcileMembership(ctx, group, membershipReader{err: context.DeadlineExceeded})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed confirmation: %v", err)
	}
	timedOut := refresh().Partitions[0]
	if slices.Contains(timedOut.ReadyReplicaNodeIDs, "n4") {
		t.Fatal("heartbeat alone admitted n4 after confirmation timed out")
	}

	heartbeat("n1", group.GroupID, 8, 42, follower, "n4")
	heartbeat("n3", group.GroupID, 8, 42, follower, "n4")
	heartbeat("n4", group.GroupID, 8, 42, leader, "n4")
	if err := reconcile(42, 8, "n4", voters); err != nil {
		t.Fatal(err)
	}
	published := refresh()
	agent, err := api.GetConfig(ctx, &controlplanev1.GetConfigRequest{
		NodeId: "n4", Kind: controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE,
	})
	partitions := agent.GetConfig().GetPartitions()
	if err != nil || len(partitions) != 1 || !slices.Contains(partitions[0].GetReplicaNodeIds(), "n4") {
		t.Fatalf("Storage Node did not receive confirmed placement: %v, %v", agent, err)
	}
	desired, err := store.Config(ctx)
	if err != nil || slices.Contains(desired.Config.Partitions[0].ReplicaNodeIDs, "n4") {
		t.Fatalf("observation changed desired placement: %v, %v", desired.Config, err)
	}
	wantPut("n4")
	get, err := forwarder.Get(ctx, &pb.GetRequest{Key: "test"})
	if err != nil || get.GetRev() != 4 || get.GetServedBy().GetInstance() != "n4" {
		t.Fatalf("GET through n4 after leadership change: %v, %v", get, err)
	}

	api = cpapi.New(store)
	if err := reconcile(40, 7, "n1", core); !errors.Is(err, cpapi.ErrMembershipObservation) {
		t.Fatalf("stale observation after observer restart: %v", err)
	}
	if next := refresh(); next.Revision != published.Revision || next.Partitions[0].LeaderNodeID != "n4" {
		t.Fatalf("stale observation changed published route: %+v", next.Partitions[0])
	}
	wantPut("n4")
}
