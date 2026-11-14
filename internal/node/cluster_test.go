package node_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/statestore"
	"github.com/OraCache/oracache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

type poc struct {
	nodes    []*node.Node
	runDone  []chan error
	cpStore  statestore.Store
	client   pb.CacheGatewayClient
	forward  *gateway.Forwarder
	nodeAddr []string
}

// startPoC brings up the whole PoC over real loopback transports: three
// storage nodes (HTTP Raft transport, gRPC API, CP agent), a CP gRPC server, a
// CP-polled router and the forwarding gateway served over gRPC.
func startPoC(t *testing.T) *poc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &poc{}

	grpcLis := []net.Listener{listen(t), listen(t), listen(t)}
	raftLis := []net.Listener{listen(t), listen(t), listen(t)}
	var peers []node.Peer
	cfg := &cpconfig.Config{
		Revision: 1, PartitionCount: 1,
		Regions: []cpconfig.Region{{ID: "region-a", Role: cpconfig.RegionRoleMaster}},
	}
	assignment := cpconfig.PartitionAssignment{PartitionID: 0, Region: "region-a", LeaderNodeID: "node-1"}
	for i := range grpcLis {
		id := fmt.Sprintf("node-%d", i+1)
		peers = append(peers, node.Peer{ID: uint64(i + 1), NodeID: id, ClientAddr: grpcLis[i].Addr().String(), RaftURL: "http://" + raftLis[i].Addr().String() + "/raft"})
		cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: id, Region: "region-a", Address: grpcLis[i].Addr().String()})
		assignment.ReplicaNodeIDs = append(assignment.ReplicaNodeIDs, id)
		p.nodeAddr = append(p.nodeAddr, grpcLis[i].Addr().String())
	}
	cfg.Partitions = []cpconfig.PartitionAssignment{assignment}

	// Control plane over real gRPC. Its static assignment names node-1 as the
	// leader no matter who wins the election, so routing must follow Raft hints.
	p.cpStore = statestore.NewMemory()
	if err := p.cpStore.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	cpLis := listen(t)
	cpSrv := grpc.NewServer()
	controlplanev1.RegisterControlPlaneAPIServer(cpSrv, cpapi.New(p.cpStore, cpapi.WithLogger(quiet), cpapi.WithHeartbeatInterval(time.Second)))
	go cpSrv.Serve(cpLis)
	t.Cleanup(cpSrv.Stop)

	for i := range grpcLis {
		n, err := node.New(node.Config{
			NodeID: peers[i].NodeID, Region: "region-a", ShardID: 0, GroupID: "region-a/shard-0/test",
			PeerID: peers[i].ID, Peers: peers, GRPCListener: grpcLis[i], RaftListener: raftLis[i],
			CPAddr: cpLis.Addr().String(), HeartbeatInterval: time.Second, ConfigPullInterval: time.Second,
			Store:        store.Config{MaxVersions: 3},
			TickInterval: 20 * time.Millisecond, ElectionTicks: 10, RequestTimeout: 3 * time.Second,
			Logger: quiet,
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- n.Run(ctx); close(done) }() // closed, so a second receive in Cleanup cannot block
		t.Cleanup(func() { n.Close(); <-done })
		p.nodes, p.runDone = append(p.nodes, n), append(p.runDone, done)
	}

	cpConn, err := grpc.NewClient(cpLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cpConn.Close() })
	router, err := gateway.NewRouter("region-a")
	if err != nil {
		t.Fatal(err)
	}
	poller := gateway.NewPoller(controlplanev1.NewControlPlaneAPIClient(cpConn), router, "gateway-1")
	if err := poller.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	p.forward = &gateway.Forwarder{Router: router, Logger: quiet, Timeout: 15 * time.Second}
	t.Cleanup(p.forward.Close)
	gwLis := listen(t)
	gwSrv := grpc.NewServer()
	pb.RegisterCacheGatewayServer(gwSrv, p.forward)
	go gwSrv.Serve(gwLis)
	t.Cleanup(gwSrv.Stop)
	gwConn, err := grpc.NewClient(gwLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gwConn.Close() })
	p.client = pb.NewCacheGatewayClient(gwConn)
	return p
}

func (p *poc) leaderIndex(t *testing.T, excluded int) int {
	t.Helper()
	idx := -1
	eventually(t, "a ready leader", func() bool {
		for i, n := range p.nodes {
			if i == excluded {
				continue
			}
			if s := n.Status(); s.Running && s.Ready && s.Role == "StateLeader" {
				idx = i
				return true
			}
		}
		return false
	})
	return idx
}

func TestPoCClusterKillLeaderOverRealTransports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p := startPoC(t)
	leader := p.leaderIndex(t, -1)

	// Put/Get/conflict through the single gateway endpoint.
	put, err := p.client.Put(ctx, &pb.PutRequest{Key: "profile:1", Data: []byte("v1")})
	if err != nil || put.Status != pb.PutStatus_PUT_STATUS_OK || put.Rev == 0 {
		t.Fatalf("Put: %v %v", put, err)
	}
	if put.ServedBy.GetInstance() != fmt.Sprintf("node-%d", leader+1) {
		t.Fatalf("served by %v, leader is node-%d", put.ServedBy, leader+1)
	}
	stale := uint64(0)
	conflict, err := p.client.Put(ctx, &pb.PutRequest{Key: "profile:1", Data: []byte("x"), ExpectedRev: &stale})
	if err != nil || conflict.Status != pb.PutStatus_PUT_STATUS_CONFLICT || conflict.Rev != put.Rev {
		t.Fatalf("conflict: %v %v", conflict, err)
	}
	put2, err := p.client.Put(ctx, &pb.PutRequest{Key: "profile:1", Data: []byte("v2"), ExpectedRev: &put.Rev})
	if err != nil || put2.Status != pb.PutStatus_PUT_STATUS_OK || put2.Rev <= put.Rev {
		t.Fatalf("conditional put: %v %v", put2, err)
	}
	got, err := p.client.Get(ctx, &pb.GetRequest{Key: "profile:1"})
	if err != nil || got.Status != pb.GetStatus_GET_STATUS_HIT || string(got.Data) != "v2" || got.Rev != put2.Rev {
		t.Fatalf("Get: %v %v", got, err)
	}
	old, err := p.client.Get(ctx, &pb.GetRequest{Key: "profile:1", Rev: put.Rev})
	if err != nil || string(old.Data) != "v1" || old.LatestRev != put2.Rev {
		t.Fatalf("Get old rev: %v %v", old, err)
	}

	// All three independent stores converged to the same replicated state.
	want, _ := p.nodes[leader].Store().Checkpoint()
	eventually(t, "stores converge", func() bool {
		for _, n := range p.nodes {
			if got, _ := n.Store().Checkpoint(); !bytes.Equal(got, want) {
				return false
			}
		}
		return true
	})

	// Heartbeats carry the actual Raft status (soft state in the CP).
	eventually(t, "CP sees the real leader via heartbeats", func() bool {
		hb, ok, err := p.cpStore.Heartbeat(ctx, fmt.Sprintf("node-%d", leader+1))
		return err == nil && ok && len(hb.Partitions) == 1 &&
			hb.Partitions[0].Role == statestore.RaftRoleLeader && hb.Partitions[0].Ready &&
			hb.Partitions[0].LeaderID == fmt.Sprintf("node-%d", leader+1) && hb.Partitions[0].AppliedIndex > 0
	})
	snap, _ := p.cpStore.Config(ctx)
	if snap.Revision != 1 || snap.Config.Partitions[0].LeaderNodeID != "node-1" {
		t.Fatal("heartbeats must not rewrite CP assignments")
	}

	// Kill the elected leader process-equivalent: its gRPC and Raft ports go away.
	p.nodes[leader].Close()
	if err := <-p.runDone[leader]; err != nil {
		t.Fatalf("leader Run: %v", err)
	}
	next := p.leaderIndex(t, leader)
	if next == leader {
		t.Fatal("same leader")
	}

	// Same gateway endpoint: reads see confirmed data, writes succeed on the new leader.
	got, err = p.client.Get(ctx, &pb.GetRequest{Key: "profile:1"})
	if err != nil || string(got.Data) != "v2" {
		t.Fatalf("post-failover Get: %v %v", got, err)
	}
	put3, err := p.client.Put(ctx, &pb.PutRequest{Key: "profile:1", Data: []byte("v3"), ExpectedRev: &put2.Rev})
	if err != nil || put3.Status != pb.PutStatus_PUT_STATUS_OK || put3.Rev <= put2.Rev {
		t.Fatalf("post-failover Put: %v %v", put3, err)
	}
	if put3.ServedBy.GetInstance() != fmt.Sprintf("node-%d", next+1) {
		t.Fatalf("served by %v, want node-%d", put3.ServedBy, next+1)
	}
	del, err := p.client.Delete(ctx, &pb.DeleteRequest{Key: "profile:1"})
	if err != nil || del.Status != pb.DeleteStatus_DELETE_STATUS_DELETED {
		t.Fatalf("Delete: %v %v", del, err)
	}
	if miss, err := p.client.Get(ctx, &pb.GetRequest{Key: "profile:1"}); err != nil || miss.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatalf("after delete: %v %v", miss, err)
	}
	batch, err := p.client.BatchPut(ctx, &pb.BatchPutRequest{Items: []*pb.PutRequest{{Key: "a", Data: []byte("1")}, {Key: "b", Data: []byte("2")}}})
	if err != nil || len(batch.Items) != 2 {
		t.Fatalf("BatchPut: %v %v", batch, err)
	}
	bg, err := p.client.BatchGet(ctx, &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "a"}, {Key: "b"}, {Key: "zz"}}})
	if err != nil || bg.Items[0].Status != pb.GetStatus_GET_STATUS_HIT || bg.Items[2].Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatalf("BatchGet: %v %v", bg, err)
	}
}
