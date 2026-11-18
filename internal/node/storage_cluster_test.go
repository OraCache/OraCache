package node_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func storageListen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestStandaloneStorageCluster(t *testing.T) {
	t.Run("linearizable", func(t *testing.T) { testStandaloneStorageCluster(t, shardraft.Linearizable) })
	t.Run("local", func(t *testing.T) { testStandaloneStorageCluster(t, shardraft.Local) })
}

func testStandaloneStorageCluster(t *testing.T, mode shardraft.ReadMode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var grpcLis, raftLis [3]net.Listener
	var nodes [3]*node.Node
	var clients [3]pb.CacheGatewayClient
	var done [3]chan error
	var peers []node.Peer
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := range nodes {
		grpcLis[i], raftLis[i] = storageListen(t), storageListen(t)
		peers = append(peers, node.Peer{ID: uint64(i + 1), NodeID: fmt.Sprintf("node-%d", i+1), ClientAddr: grpcLis[i].Addr().String(), RaftURL: "http://" + raftLis[i].Addr().String() + "/raft"})
	}
	for i := range nodes {
		n, err := node.New(node.Config{NodeID: peers[i].NodeID, Region: "region-a", GroupID: "standalone-test", PeerID: peers[i].ID, Peers: peers, GRPCListener: grpcLis[i], RaftListener: raftLis[i], ReadMode: mode, Logger: quiet, TickInterval: 20 * time.Millisecond, ElectionTicks: 10})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		done[i] = make(chan error, 1)
		go func() { done[i] <- n.Run(ctx); close(done[i]) }()
		t.Cleanup(func() {
			n.Close()
			if err := <-done[i]; err != nil {
				t.Error(err)
			}
		})
		conn, err := grpc.NewClient(peers[i].ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		clients[i] = pb.NewCacheGatewayClient(conn)
	}
	leader := func(excluded int) int {
		t.Helper()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			for i, n := range nodes {
				s := n.Status()
				if i != excluded && s.Running && s.Ready && s.Role == "StateLeader" {
					return i
				}
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("no ready leader", ctx.Err())
			}
		}
	}
	write := func(i int, value string) {
		t.Helper()
		got, err := clients[i].Put(ctx, &pb.PutRequest{Key: "k", Data: []byte(value)})
		if err != nil || got.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
			t.Fatalf("Put: %v %v", got, err)
		}
	}
	assertHint := func(err error, addr string) {
		t.Helper()
		st := status.Convert(err)
		for _, detail := range st.Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && st.Code() == codes.FailedPrecondition && info.Reason == "NOT_LEADER" && info.Metadata["address"] == addr {
				return
			}
		}
		t.Fatalf("expected NOT_LEADER with address %s: %v (details %v)", addr, err, st.Details())
	}
	first := leader(-1)
	write(first, "v1")
	for i, client := range clients {
		if i == first {
			continue
		}
		// Wait until this follower has applied the write via Raft, without a gateway.
		ticker := time.NewTicker(10 * time.Millisecond)
		for {
			applied := nodes[i].Status().AppliedIndex >= nodes[first].Status().AppliedIndex
			if applied {
				break
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				ticker.Stop()
				t.Fatal("write did not replicate", ctx.Err())
			}
		}
		ticker.Stop()
		_, err := client.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("must not write")})
		assertHint(err, peers[first].ClientAddr)
		got, err := client.Get(ctx, &pb.GetRequest{Key: "k"})
		if mode == shardraft.Linearizable {
			assertHint(err, peers[first].ClientAddr)
			_, err = client.BatchGet(ctx, &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "k"}}})
			assertHint(err, peers[first].ClientAddr)
			continue
		}
		if err != nil || string(got.GetData()) != "v1" {
			t.Fatalf("local Get: %v %v", got, err)
		}
		batch, err := client.BatchGet(ctx, &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "k"}}})
		if err != nil || len(batch.GetItems()) != 1 || string(batch.GetItems()[0].GetData()) != "v1" {
			t.Fatalf("local BatchGet: %v %v", batch, err)
		}
	}
	nodes[first].Close()
	if err := <-done[first]; err != nil {
		t.Fatal(err)
	}
	next := leader(first)
	got, err := clients[next].Get(ctx, &pb.GetRequest{Key: "k"})
	if err != nil || string(got.GetData()) != "v1" {
		t.Fatalf("Get after election: %v %v", got, err)
	}
	write(next, "v2")
}
