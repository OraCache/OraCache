package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	gatewayv1 "github.com/OraCache/oracache/gen/gateway/v1"
	membershipv1 "github.com/OraCache/oracache/gen/membership/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMembershipThroughGatewayAndCPProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := statestore.NewMemory()
	cpListener := raftTestListener(t)
	cpServer := grpc.NewServer()
	controlplanev1.RegisterControlPlaneAPIServer(cpServer, cpapi.New(store, cpapi.WithLogger(logger), cpapi.WithNodeTTL(2*time.Second)))
	go cpServer.Serve(cpListener)
	t.Cleanup(cpServer.Stop)

	const region, group = "region-a", "membership-gateway/epoch-1"
	var peers []node.Peer
	var clients, rafts []net.Listener
	for i := range 5 {
		client, raft := raftTestListener(t), raftTestListener(t)
		clients, rafts = append(clients, client), append(rafts, raft)
		peers = append(peers, node.Peer{ID: uint64(i + 1), NodeID: fmt.Sprintf("node-%d", i+1), ClientAddr: client.Addr().String(), RaftURL: "http://" + raft.Addr().String() + "/raft"})
	}
	topology := &cpconfig.Config{Revision: 1, PartitionCount: 1, Regions: []cpconfig.Region{{ID: region, Role: cpconfig.RegionRoleMaster}}, Partitions: []cpconfig.PartitionAssignment{{Region: region, GroupID: group, LeaderNodeID: peers[0].NodeID}}}
	for _, peer := range peers[:3] {
		topology.Nodes = append(topology.Nodes, cpconfig.Node{ID: peer.NodeID, Region: region, Address: peer.ClientAddr})
		topology.Partitions[0].ReplicaNodeIDs = append(topology.Partitions[0].ReplicaNodeIDs, peer.NodeID)
	}
	if err := store.ApplyConfig(ctx, topology); err != nil {
		t.Fatal(err)
	}

	members := map[string]*node.Node{}
	stops := map[string]func(){}
	start := func(i int, join bool) {
		t.Helper()
		peer := peers[i]
		seed := peers[:3]
		if join {
			seed = peers
		}
		n, err := node.New(node.Config{NodeID: peer.NodeID, Region: region, GroupID: group, PeerID: peer.ID, Peers: seed, Join: join, GRPCListener: clients[i], RaftListener: rafts[i], CPAddr: cpListener.Addr().String(), HeartbeatInterval: 100 * time.Millisecond, ConfigPullInterval: 100 * time.Millisecond, TickInterval: 20 * time.Millisecond, ElectionTicks: 10, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		memberCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- n.Run(memberCtx) }()
		members[peer.NodeID] = n
		stops[peer.NodeID] = sync.OnceFunc(func() {
			stop()
			if err := <-done; err != nil {
				t.Errorf("node %s: %v", peer.NodeID, err)
			}
		})
		t.Cleanup(stops[peer.NodeID])
	}
	for i := range 3 {
		start(i, false)
	}
	router, err := gateway.NewRouter(region)
	if err != nil {
		t.Fatal(err)
	}
	poller := gateway.NewPoller(controlplanev1.NewControlPlaneAPIClient(raftTestClient(t, cpListener)), router, "membership-test-gateway")
	poller.Logger = logger
	gwListener := raftTestListener(t)
	_, stopGateway := serveGateway(gwListener, poller, logger)
	t.Cleanup(stopGateway)
	conn := raftTestClient(t, gwListener)
	admin := membershipv1.NewMembershipAdminClient(conn)
	cache := gatewayv1.NewCacheGatewayClient(conn)
	waitRaftScenario(t, ctx, "CP membership and admin route", func() bool {
		if err := poller.Refresh(ctx); err != nil {
			return false
		}
		addresses, err := router.AdminCandidates(group, region, 0)
		return err == nil && len(addresses) == 3
	})
	initial, err := admin.GetMembership(ctx, &membershipv1.GetMembershipRequest{GroupId: group, Region: region})
	if err != nil || len(initial.GetMembers()) != 3 {
		t.Fatalf("initial membership: %v %v", initial, err)
	}
	if _, err := admin.GetMembership(ctx, &membershipv1.GetMembershipRequest{GroupId: "other", Region: region}); status.Code(err) != codes.Unavailable {
		t.Fatalf("unknown group route: %v", err)
	}
	if _, err := cache.Put(ctx, &gatewayv1.PutRequest{Key: "membership-key", Data: []byte("before")}); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	start(3, true)
	waitRaftScenario(t, ctx, "unknown node heartbeat", func() bool { _, ok, _ := store.Heartbeat(ctx, peers[3].NodeID); return ok })
	view, err := store.LiveConfig(ctx, time.Now(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Config.NodeByID(peers[3].NodeID); ok {
		t.Fatal("unknown node heartbeat changed membership")
	}
	add := &membershipv1.ChangeMembershipRequest{GroupId: group, Region: region, OperationId: "add-four", Action: "add-learner", Peer: &membershipv1.Peer{Id: peers[3].ID, NodeId: peers[3].NodeID, Address: peers[3].ClientAddr, RaftAddress: peers[3].RaftURL}}
	added, err := admin.ChangeMembership(ctx, add)
	if err != nil {
		t.Fatalf("add learner through gateway: %v", err)
	}
	repeated, err := admin.ChangeMembership(ctx, add)
	if err != nil || repeated.GetChangeIndex() != added.GetChangeIndex() {
		t.Fatalf("idempotent add: %v %v", repeated, err)
	}
	waitRaftScenario(t, ctx, "learner projected but not routed", func() bool {
		view, err := store.LiveConfig(ctx, time.Now(), 2*time.Second)
		if err != nil || len(view.Config.Memberships) != 1 || len(view.Config.Memberships[0].Members) != 4 {
			return false
		}
		a := view.Config.Partitions[0]
		return !slices.Contains(a.ReadyReplicaNodeIDs, peers[3].NodeID) && members[peers[3].NodeID].Status().IsLearner
	})
	promote := &membershipv1.ChangeMembershipRequest{GroupId: group, Region: region, OperationId: "promote-four", Action: "promote", PeerId: peers[3].ID}
	waitRaftScenario(t, ctx, "promote learner through gateway", func() bool {
		_, err := admin.ChangeMembership(ctx, promote)
		return err == nil
	})
	waitRaftScenario(t, ctx, "promoted voter in CP route", func() bool {
		view, err := store.LiveConfig(ctx, time.Now(), 2*time.Second)
		if err != nil || !slices.Contains(view.Config.Partitions[0].ReadyReplicaNodeIDs, peers[3].NodeID) {
			return false
		}
		return poller.Refresh(ctx) == nil
	})
	if _, err := cache.Put(ctx, &gatewayv1.PutRequest{Key: "membership-key", Data: []byte("after-promotion")}); err != nil {
		t.Fatalf("write after promotion: %v", err)
	}
	var oldFollower node.Peer
	for _, peer := range peers[:3] {
		if members[peer.NodeID].Status().Role != "StateLeader" {
			oldFollower = peer
			break
		}
	}
	remove := &membershipv1.ChangeMembershipRequest{GroupId: group, Region: region, OperationId: "remove-old", Action: "remove", PeerId: oldFollower.ID}
	if _, err := admin.ChangeMembership(ctx, remove); err != nil {
		t.Fatalf("remove through gateway: %v", err)
	}
	waitRaftScenario(t, ctx, "removed voter excluded from CP route", func() bool {
		view, err := store.LiveConfig(ctx, time.Now(), 2*time.Second)
		if err != nil || len(view.Config.Memberships) != 1 || view.Config.Memberships[0].Joint {
			return false
		}
		if !slices.Contains(view.Config.Memberships[0].RemovedIDs, oldFollower.ID) {
			return false
		}
		return !slices.Contains(view.Config.Partitions[0].ReadyReplicaNodeIDs, oldFollower.NodeID)
	})
	if err := poller.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	addresses, err := router.AdminCandidates(group, region, 0)
	if err != nil || slices.Contains(addresses, oldFollower.ClientAddr) {
		t.Fatalf("removed peer in admin route: %v %v", addresses, err)
	}
	if _, err := cache.Put(ctx, &gatewayv1.PutRequest{Key: "membership-key", Data: []byte("after-removal")}); err != nil {
		t.Fatalf("write after removal: %v", err)
	}
	stops[oldFollower.NodeID]()
	start(4, true)
	waitRaftScenario(t, ctx, "fifth node heartbeat before membership", func() bool { _, ok, _ := store.Heartbeat(ctx, peers[4].NodeID); return ok })
	addFive := &membershipv1.ChangeMembershipRequest{GroupId: group, Region: region, OperationId: "add-five", Action: "add-learner", Peer: &membershipv1.Peer{Id: peers[4].ID, NodeId: peers[4].NodeID, Address: peers[4].ClientAddr, RaftAddress: peers[4].RaftURL}}
	if _, err := admin.ChangeMembership(ctx, addFive); err != nil {
		t.Fatalf("add replacement learner: %v", err)
	}
	var oldLeader, replaceTarget node.Peer
	for _, peer := range peers[:3] {
		if peer.ID == oldFollower.ID {
			continue
		}
		if members[peer.NodeID].Status().Role == "StateLeader" {
			oldLeader = peer
		} else {
			replaceTarget = peer
		}
	}
	if oldLeader.ID == 0 || replaceTarget.ID == 0 {
		t.Fatalf("cannot select old leader and replacement target: %v %v", oldLeader, replaceTarget)
	}
	replace := &membershipv1.ChangeMembershipRequest{GroupId: group, Region: region, OperationId: "replace-with-five", Action: "replace", PeerId: peers[4].ID, RemoveId: replaceTarget.ID}
	waitRaftScenario(t, ctx, "replace old voter through gateway", func() bool { _, err := admin.ChangeMembership(ctx, replace); return err == nil })
	waitRaftScenario(t, ctx, "replacement projected by CP", func() bool {
		view, err := store.LiveConfig(ctx, time.Now(), 2*time.Second)
		if err != nil || len(view.Config.Memberships) != 1 || view.Config.Memberships[0].Joint {
			return false
		}
		a := view.Config.Partitions[0]
		return slices.Contains(a.ReadyReplicaNodeIDs, peers[4].NodeID) && !slices.Contains(a.ReadyReplicaNodeIDs, replaceTarget.NodeID)
	})
	stops[replaceTarget.NodeID]()
	if err := poller.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	stops[oldLeader.NodeID]()
	waitRaftScenario(t, ctx, "new voter elected after old leader stopped", func() bool {
		for _, peer := range peers[3:] {
			if s := members[peer.NodeID].Status(); s.Ready && s.Role == "StateLeader" {
				return true
			}
		}
		return false
	})
	// A read can safely try another voter while the gateway still has an old route.
	if observed, err := admin.GetMembership(ctx, &membershipv1.GetMembershipRequest{GroupId: group, Region: region}); err != nil || observed.GetJoint() || len(observed.GetMembers()) != 3 {
		t.Fatalf("membership after failover: %v %v", observed, err)
	}
	waitRaftScenario(t, ctx, "CP and gateway observe the new voter leader", func() bool {
		if err := poller.Refresh(ctx); err != nil {
			return false
		}
		leader, err := router.WriteLeader(0)
		return err == nil && (leader.NodeID == peers[3].NodeID || leader.NodeID == peers[4].NodeID)
	})
	if repeated, err := admin.ChangeMembership(ctx, replace); err != nil || repeated.GetOperationId() != replace.GetOperationId() {
		t.Fatalf("retry after leader change: %v %v", repeated, err)
	}
	if _, err := cache.Put(ctx, &gatewayv1.PutRequest{Key: "membership-key", Data: []byte("written-by-new-voter")}); err != nil {
		t.Fatalf("write through new voter: %v", err)
	}
}
