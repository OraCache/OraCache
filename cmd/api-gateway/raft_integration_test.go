package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	gatewayv1 "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Exercise the executable's client endpoint, actual CP heartbeats and Raft
// over independent loopback listeners. No synthetic leader statuses.
func TestGatewayRoutesRealRaftThroughLeaderAndControlPlaneLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := statestore.NewMemory()

	var cpUnavailable atomic.Bool
	cpListener := raftTestListener(t)
	cpServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handle grpc.UnaryHandler) (any, error) {
		if cpUnavailable.Load() {
			return nil, status.Error(codes.Unavailable, "test Control Plane outage")
		}
		return handle(ctx, req)
	}))
	controlplanev1.RegisterControlPlaneAPIServer(cpServer, cpapi.New(store, cpapi.WithLogger(logger)))
	go cpServer.Serve(cpListener)
	t.Cleanup(cpServer.Stop)

	const region = "region-a"
	topology := &cpconfig.Config{
		Revision: 1, PartitionCount: 1,
		Regions: []cpconfig.Region{{ID: region, Role: cpconfig.RegionRoleMaster}},
	}
	assignment := cpconfig.PartitionAssignment{Region: region, LeaderNodeID: "node-1"}
	var peers []node.Peer
	var clientListeners, raftListeners []net.Listener
	for i := range 3 {
		client, raft := raftTestListener(t), raftTestListener(t)
		clientListeners, raftListeners = append(clientListeners, client), append(raftListeners, raft)
		id := fmt.Sprintf("node-%d", i+1)
		peers = append(peers, node.Peer{ID: uint64(i + 1), NodeID: id, ClientAddr: client.Addr().String(), RaftURL: "http://" + raft.Addr().String() + "/raft"})
		topology.Nodes = append(topology.Nodes, cpconfig.Node{ID: id, Region: region, Address: client.Addr().String()})
		assignment.ReplicaNodeIDs = append(assignment.ReplicaNodeIDs, id)
	}
	topology.Partitions = []cpconfig.PartitionAssignment{assignment}
	if err := store.ApplyConfig(ctx, topology); err != nil {
		t.Fatal(err)
	}

	members := make(map[string]*node.Node, len(peers))
	stopMember := make(map[string]func(), len(peers))
	for i, peer := range peers {
		member, err := node.New(node.Config{
			NodeID: peer.NodeID, Region: region, GroupID: "integration/group", PeerID: peer.ID, Peers: peers,
			GRPCListener: clientListeners[i], RaftListener: raftListeners[i], CPAddr: cpListener.Addr().String(),
			HeartbeatInterval: time.Second, ConfigPullInterval: time.Second, TickInterval: 25 * time.Millisecond,
			ElectionTicks: 10, Logger: logger,
		})
		if err != nil {
			t.Fatal(err)
		}
		memberContext, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- member.Run(memberContext) }()
		members[peer.NodeID] = member
		stopMember[peer.NodeID] = sync.OnceFunc(func() {
			stop()
			if err := <-done; err != nil {
				t.Errorf("storage %s: %v", peer.NodeID, err)
			}
		})
		t.Cleanup(stopMember[peer.NodeID])
	}

	cpConn := raftTestClient(t, cpListener)
	router := testRouter(t, region)
	poller := gateway.NewPoller(controlplanev1.NewControlPlaneAPIClient(cpConn), router, "test-gateway")
	poller.Logger, poller.RPCTimeout = logger, 300*time.Millisecond
	listener := raftTestListener(t)
	_, stopGateway := serveGateway(listener, poller, cpmetrics.NewRegistry(), logger)
	t.Cleanup(stopGateway)
	client := gatewayv1.NewCacheGatewayClient(raftTestClient(t, listener))

	var oldLeader string
	waitRaftScenario(t, ctx, "all replicas ready and a confirmed leader", func() bool {
		view, err := store.LiveConfig(ctx, time.Now(), cpapi.DefaultNodeTTL)
		if err != nil || len(view.Config.Partitions[0].ReadyReplicaNodeIDs) != len(peers) || poller.Refresh(ctx) != nil {
			return false
		}
		leader, err := router.WriteLeader(0)
		oldLeader = leader.NodeID
		return err == nil
	})
	const key = "confirmed-through-gateway"
	seed, err := client.Put(ctx, &gatewayv1.PutRequest{Key: key, Data: []byte("before")})
	if err != nil || seed.GetStatus() != gatewayv1.PutStatus_PUT_STATUS_OK {
		t.Fatalf("initial PUT: %v, %v", seed, err)
	}

	cpUnavailable.Store(true)
	oldTerm := members[oldLeader].Status().Term
	stopMember[oldLeader]()
	var newLeader string
	waitRaftScenario(t, ctx, "Raft elects a new leader without Control Plane", func() bool {
		for id, member := range members {
			current := member.Status()
			if id != oldLeader && current.Ready && current.Role == "StateLeader" && current.Term > oldTerm {
				newLeader = id
				return true
			}
		}
		return false
	})
	if err := poller.Refresh(ctx); status.Code(err) != codes.Unavailable {
		t.Fatalf("Control Plane outage returned %v", err)
	}
	if route, err := router.WriteLeader(0); err != nil || route.NodeID != oldLeader {
		t.Fatalf("outage discarded the last accepted route: %v, %v", route, err)
	}
	if _, err := client.Put(ctx, &gatewayv1.PutRequest{Key: key, Data: []byte("ambiguous")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("an ambiguous transport error must not redirect a write: %v", err)
	}
	read, err := client.Get(ctx, &gatewayv1.GetRequest{Key: key})
	if err != nil || string(read.GetData()) != "before" || read.GetServedBy().GetInstance() != newLeader {
		t.Fatalf("strict read after failover: %v, %v; want %s", read, err, newLeader)
	}
	next, err := client.Put(ctx, &gatewayv1.PutRequest{Key: key, Data: []byte("after"), ExpectedRev: &seed.Rev})
	if err != nil || next.GetStatus() != gatewayv1.PutStatus_PUT_STATUS_OK || next.GetRev() <= seed.GetRev() || next.GetServedBy().GetInstance() != newLeader {
		t.Fatalf("PUT through the recovered route during CP outage: %v, %v; want %s", next, err, newLeader)
	}
	t.Logf("leader %s → %s during Control Plane outage, store revision %d → %d", oldLeader, newLeader, seed.GetRev(), next.GetRev())

	cpUnavailable.Store(false)
	waitRaftScenario(t, ctx, "same topology revision reports the new Raft leader", func() bool {
		if poller.Refresh(ctx) != nil {
			return false
		}
		leader, err := router.WriteLeader(0)
		return err == nil && leader.NodeID == newLeader && router.Revision() == topology.Revision
	})
}

func raftTestListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func raftTestClient(t *testing.T, listener net.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func waitRaftScenario(t *testing.T, ctx context.Context, what string, check func() bool) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-ticker.C:
		}
	}
}
