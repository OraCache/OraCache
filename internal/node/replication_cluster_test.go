package node_test

import (
	"context"
	"fmt"
	"github.com/OraCache/oracache/internal/replication"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	cpb "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/statestore"
	"github.com/OraCache/oracache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type regionTestGroup struct {
	t          *testing.T
	ctx        context.Context
	nodes      [3]*node.Node
	clients    [3]pb.CacheGatewayClient
	grpc, raft [3]net.Listener
	done       [3]chan error
	stopped    [3]bool
	cpAddr     string
}

func newRegionGroup(t *testing.T, ctx context.Context) *regionTestGroup {
	g := &regionTestGroup{t: t, ctx: ctx}
	for i := range g.nodes {
		g.grpc[i], g.raft[i] = storageListen(t), storageListen(t)
	}
	return g
}

func (g *regionTestGroup) configure(region string, cfg replication.Config) {
	g.t.Helper()
	var peers []node.Peer
	for i := range g.nodes {
		peers = append(peers, node.Peer{ID: uint64(i + 1), NodeID: fmt.Sprintf("%s-%d", region, i), ClientAddr: g.grpc[i].Addr().String(), RaftURL: "http://" + g.raft[i].Addr().String() + "/raft"})
	}
	for i := range g.nodes {
		n, err := node.New(node.Config{NodeID: peers[i].NodeID, Region: region, GroupID: region + "/0/test", PeerID: peers[i].ID, Peers: peers, GRPCListener: g.grpc[i], RaftListener: g.raft[i], Replication: cfg, CPAddr: g.cpAddr, ConfigPullInterval: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, SnapshotEntries: 4, TickInterval: 20 * time.Millisecond, RequestTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			g.t.Fatal(err)
		}
		g.nodes[i] = n
		conn, err := grpc.NewClient(peers[i].ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			g.t.Fatal(err)
		}
		g.clients[i] = pb.NewCacheGatewayClient(conn)
		g.t.Cleanup(func() { _ = conn.Close() })
	}
	g.t.Cleanup(func() {
		for i := range g.nodes {
			g.stop(i)
		}
	})
}

func TestCrossRegionReplicationDiscoversAddressesFromGetConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b := newRegionGroup(t, ctx), newRegionGroup(t, ctx)
	cpListener := storageListen(t)
	cpStore := statestore.NewMemory()
	config := &cpconfig.Config{Revision: 1, PartitionCount: 1, Regions: []cpconfig.Region{{ID: "a", Role: cpconfig.RegionRoleMaster}, {ID: "b", Role: cpconfig.RegionRoleSlave}}}
	for _, group := range []struct {
		name string
		g    *regionTestGroup
	}{{"a", a}, {"b", b}} {
		assignment := cpconfig.PartitionAssignment{Region: group.name, PartitionID: 0, LeaderNodeID: group.name + "-0"}
		for i := range group.g.nodes {
			id := fmt.Sprintf("%s-%d", group.name, i)
			address := group.g.grpc[i].Addr().String()
			if group.name == "b" {
				address = "127.0.0.1:1"
			}
			config.Nodes = append(config.Nodes, cpconfig.Node{ID: id, Region: group.name, Address: address})
			assignment.ReplicaNodeIDs = append(assignment.ReplicaNodeIDs, id)
		}
		config.Partitions = append(config.Partitions, assignment)
	}
	if err := cpStore.ApplyConfig(ctx, config); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	cpb.RegisterControlPlaneAPIServer(server, cpapi.New(cpStore, cpapi.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))))
	go func() { _ = server.Serve(cpListener) }()
	defer server.Stop()
	a.cpAddr, b.cpAddr = cpListener.Addr().String(), cpListener.Addr().String()
	a.configure("a", replication.Config{TargetRegion: "b"})
	b.configure("b", replication.Config{SourceRegion: "a", SourceGroupID: "a/0/test"})
	for i := range a.nodes {
		a.start(i)
		b.start(i)
	}
	source, dest := a.leader(), b.leader()
	eventuallyReplication(t, ctx, func() bool {
		h, exists, err := cpStore.Heartbeat(ctx, fmt.Sprintf("a-%d", source))
		return err == nil && exists && h.ConfigRevision == 1
	})
	put, err := a.clients[source].Put(ctx, &pb.PutRequest{Key: "from-cp", Data: []byte("first")})
	if err != nil || put.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
		t.Fatal(put, err)
	}
	missing, err := b.clients[dest].Get(ctx, &pb.GetRequest{Key: "from-cp"})
	if err != nil || missing.GetStatus() != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("sender used an address not supplied by CP", missing, err)
	}
	updated := config.Clone()
	updated.Revision++
	for i := range b.nodes {
		updated.Nodes[3+i].Address = b.grpc[i].Addr().String()
	}
	if err := cpStore.ApplyConfig(ctx, updated); err != nil {
		t.Fatal(err)
	}
	eventuallyReplication(t, ctx, func() bool {
		got, err := b.clients[dest].Get(ctx, &pb.GetRequest{Key: "from-cp", Rev: put.Rev})
		return err == nil && got.GetRev() == put.Rev && string(got.Data) == "first"
	})
	b.stop(dest)
	dest = b.leader()
	put, err = a.clients[source].Put(ctx, &pb.PutRequest{Key: "from-cp", Data: []byte("second")})
	if err != nil || put.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
		t.Fatal(put, err)
	}
	eventuallyReplication(t, ctx, func() bool {
		got, err := b.clients[dest].Get(ctx, &pb.GetRequest{Key: "from-cp", Rev: put.Rev})
		return err == nil && got.GetRev() == put.Rev && string(got.Data) == "second"
	})
	server.Stop()
	deleted, err := a.clients[source].Delete(ctx, &pb.DeleteRequest{Key: "from-cp"})
	if err != nil || deleted.GetStatus() != pb.DeleteStatus_DELETE_STATUS_DELETED {
		t.Fatal(deleted, err)
	}
	eventuallyReplication(t, ctx, func() bool {
		got, err := b.clients[dest].Get(ctx, &pb.GetRequest{Key: "from-cp"})
		return err == nil && got.GetStatus() == pb.GetStatus_GET_STATUS_MISS
	})
}

func (g *regionTestGroup) start(i int) {
	g.done[i] = make(chan error, 1)
	go func() { g.done[i] <- g.nodes[i].Run(g.ctx) }()
}

func (g *regionTestGroup) stop(i int) {
	if g.stopped[i] {
		return
	}
	g.stopped[i] = true
	g.nodes[i].Close()
	if g.done[i] != nil {
		if err := <-g.done[i]; err != nil {
			g.t.Error(err)
		}
	}
}

func (g *regionTestGroup) leader() int {
	g.t.Helper()
	found := -1
	eventuallyReplication(g.t, g.ctx, func() bool {
		for i, n := range g.nodes {
			s := n.Status()
			if !g.stopped[i] && s.Running && s.Ready && s.Role == "StateLeader" {
				found = i
				return true
			}
		}
		return false
	})
	return found
}

func eventuallyReplication(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("replication condition not reached", ctx.Err())
		}
	}
}

func TestCrossRegionReplicationBacklogSnapshotsAndBothLeaderChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b := newRegionGroup(t, ctx), newRegionGroup(t, ctx)
	cpListener := storageListen(t)
	cpStore := statestore.NewMemory()
	config := &cpconfig.Config{Revision: 1, PartitionCount: 1, Regions: []cpconfig.Region{{ID: "a", Role: cpconfig.RegionRoleMaster}, {ID: "b", Role: cpconfig.RegionRoleSlave}}}
	for _, group := range []struct {
		name string
		g    *regionTestGroup
	}{{"a", a}, {"b", b}} {
		assignment := cpconfig.PartitionAssignment{Region: group.name, PartitionID: 0, LeaderNodeID: group.name + "-0"}
		for i := range group.g.nodes {
			id := fmt.Sprintf("%s-%d", group.name, i)
			config.Nodes = append(config.Nodes, cpconfig.Node{ID: id, Region: group.name, Address: group.g.grpc[i].Addr().String()})
			assignment.ReplicaNodeIDs = append(assignment.ReplicaNodeIDs, id)
		}
		config.Partitions = append(config.Partitions, assignment)
	}
	if err := cpStore.ApplyConfig(ctx, config); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	cpb.RegisterControlPlaneAPIServer(server, cpapi.New(cpStore, cpapi.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))))
	go func() { _ = server.Serve(cpListener) }()
	defer server.Stop()
	a.cpAddr = cpListener.Addr().String()
	a.configure("a", replication.Config{TargetRegion: "b"})
	b.configure("b", replication.Config{SourceRegion: "a", SourceGroupID: "a/0/test"})
	a.start(0)
	a.start(1)
	first := a.leader()
	eventuallyReplication(t, ctx, func() bool {
		for i := 0; i < 2; i++ {
			h, exists, err := cpStore.Heartbeat(ctx, fmt.Sprintf("a-%d", i))
			if err != nil || !exists || h.ConfigRevision != 1 {
				return false
			}
		}
		return true
	})
	var revisions []uint64
	for i := 0; i < 6; i++ {
		out, err := a.clients[first].Put(ctx, &pb.PutRequest{Key: "k", Data: fmt.Append(nil, i)})
		if err != nil || out.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
			t.Fatal(out, err)
		}
		revisions = append(revisions, out.Rev)
	}
	a.start(2)
	eventuallyReplication(t, ctx, func() bool {
		return a.nodes[2].Status().SnapshotIndex > 0 && a.nodes[2].Store().Peek("k", 0).LatestRev == revisions[5]
	})
	a.stop(first)
	next := a.leader()
	for i := range b.nodes {
		b.start(i)
	}
	dest := b.leader()
	eventuallyReplication(t, ctx, func() bool {
		for _, n := range b.nodes {
			for _, rev := range revisions[3:] {
				if n.Store().Peek("k", rev).Status != store.GetHit {
					return false
				}
			}
		}
		return true
	})
	if _, err := b.clients[dest].Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("illegal")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("destination accepted client write", err)
	}
	if _, err := a.clients[next].Delete(ctx, &pb.DeleteRequest{Key: "k", Rev: revisions[4]}); err != nil {
		t.Fatal(err)
	}
	eventuallyReplication(t, ctx, func() bool {
		for _, n := range b.nodes {
			if n.Store().Peek("k", revisions[4]).Status != store.GetVersionNotFound {
				return false
			}
		}
		return true
	})
	b.stop(dest)
	_ = b.leader()
	out, err := a.clients[next].Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("after-destination-election")})
	if err != nil {
		t.Fatal(err)
	}
	eventuallyReplication(t, ctx, func() bool {
		for i, n := range b.nodes {
			if !b.stopped[i] && n.Store().Peek("k", 0).LatestRev != out.Rev {
				return false
			}
		}
		return true
	})
	if _, err := a.clients[next].Delete(ctx, &pb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatal(err)
	}
	eventuallyReplication(t, ctx, func() bool {
		for i, n := range b.nodes {
			if !b.stopped[i] && n.Store().Peek("k", 0).Status != store.GetMiss {
				return false
			}
		}
		return true
	})
}
