package shardraft_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func grpcClient(t *testing.T, n *shardraft.Instance) pb.CacheGatewayClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterCacheGatewayServer(server, shardraft.GatewayServer(n))
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-finished; err != nil {
			t.Error(err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///raft-cache", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewCacheGatewayClient(conn)
}

func TestHTTPClusterAndGRPCAdapter(t *testing.T) {
	listeners := make([]net.Listener, 3)
	addresses := make(map[uint64]string)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		t.Cleanup(func() { listener.Close() })
		addresses[uint64(i+1)] = "http://" + listener.Addr().String() + "/raft"
	}
	// Each node gets its own HTTP client, transport, Raft state and cache.
	c := &cluster{}
	for i, listener := range listeners {
		clientTransport := http.DefaultTransport.(*http.Transport).Clone()
		t.Cleanup(clientTransport.CloseIdleConnections)
		transport, err := shardraft.NewHTTPTransport(addresses, &http.Client{Transport: clientTransport})
		if err != nil {
			t.Fatal(err)
		}
		cfg := testConfig(uint64(i+1), 3)
		cfg.ElectionTicks, cfg.RequestTimeout, cfg.SendTimeout = 100, 10*time.Second, 3*time.Second
		backend := &observedCache{Server: cache.New()}
		n, err := shardraft.NewInstance(cfg, shardraft.AdaptCache(backend), transport)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: n.RaftHandler(), ReadHeaderTimeout: time.Second}
		finished := make(chan error, 1)
		go func() { finished <- server.Serve(listener) }()
		t.Cleanup(func() {
			server.Close()
			if err := <-finished; err != http.ErrServerClosed {
				t.Errorf("HTTP server: %v", err)
			}
		})
		c.nodes = append(c.nodes, n)
		c.caches = append(c.caches, backend)
		runNode(t, n)
	}
	leader := c.leader(t, 0)
	client := grpcClient(t, leader)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Exercise the full public maximum batch, larger than the default 4 MiB
	// gRPC receive limit, through both gRPC and the real Raft HTTP transport.
	puts := make([]*pb.PutRequest, 1000)
	gets := make([]*pb.GetRequest, 1000)
	for i := range puts {
		puts[i] = &pb.PutRequest{Key: fmt.Sprintf("profile:%d", i), Data: make([]byte, 10240)}
		puts[i].Data[0] = byte(i)
		gets[i] = &pb.GetRequest{Key: puts[i].Key}
	}
	written, err := client.BatchPut(ctx, &pb.BatchPutRequest{Items: puts})
	if err != nil || len(written.Items) != len(puts) {
		t.Fatalf("large BatchPut: %v", err)
	}
	read, err := client.BatchGet(ctx, &pb.BatchGetRequest{Items: gets})
	if err != nil || len(read.Items) != len(gets) {
		t.Fatalf("large BatchGet: %v", err)
	}
	for i, item := range read.Items {
		if item.Status != pb.GetStatus_GET_STATUS_HIT || item.Rev != 1 || len(item.Data) != 10240 || item.Data[0] != byte(i) {
			t.Fatalf("invalid response at item %d", i)
		}
	}
	eventually(t, "HTTP followers applied batch", func() bool {
		for _, backend := range c.caches {
			g, _ := backend.Get(context.Background(), gets[999])
			if g.Status != pb.GetStatus_GET_STATUS_HIT {
				return false
			}
		}
		return true
	})
	for _, n := range c.nodes {
		if n == leader {
			continue
		}
		follower := grpcClient(t, n)
		_, err := follower.Put(ctx, &pb.PutRequest{Key: "wrong"})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("follower status: %v", err)
		}
		found := false
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "NOT_LEADER" && info.Metadata["leader_id"] == fmt.Sprint(leader.Status().PeerID) && info.Metadata["address"] != "" {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing wire leader hint: %v", err)
		}
		break
	}
}
