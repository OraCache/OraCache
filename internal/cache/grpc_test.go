package cache_test

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise generated batch client/server interfaces and optional field presence
// across serialization, rather than only calling implementation methods.
func TestBatchOverGRPC(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterCacheGatewayServer(server, cache.New())
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///cache", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := pb.NewCacheGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	zero := uint64(0)
	p, err := client.BatchPut(ctx, &pb.BatchPutRequest{Items: []*pb.PutRequest{{Key: "k", ExpectedRev: &zero}, {Key: "k", ExpectedRev: &zero}, {Key: "k", Data: []byte("last")}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 3 || p.Items[0].Status != pb.PutStatus_PUT_STATUS_OK || p.Items[0].Rev != 1 || p.Items[1].Status != pb.PutStatus_PUT_STATUS_CONFLICT || p.Items[2].Rev != 2 {
		t.Fatal(p)
	}
	g, err := client.BatchGet(ctx, &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "k", Rev: 1}, {Key: "absent"}, {Key: "k"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Items) != 3 || g.Items[0].Status != pb.GetStatus_GET_STATUS_HIT || g.Items[0].Rev != 1 || g.Items[1].Status != pb.GetStatus_GET_STATUS_MISS || string(g.Items[2].Data) != "last" {
		t.Fatal(g)
	}
	// Both size boundaries travel through real gRPC.
	items := make([]*pb.GetRequest, 1000)
	for i := range items {
		items[i] = &pb.GetRequest{Key: "k"}
	}
	if r, e := client.BatchGet(ctx, &pb.BatchGetRequest{Items: items}); e != nil || len(r.Items) != 1000 {
		t.Fatal(r, e)
	}
	_, err = client.BatchGet(ctx, &pb.BatchGetRequest{Items: append(items, &pb.GetRequest{Key: "k"})})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = client.BatchPut(ctx, &pb.BatchPutRequest{Items: make([]*pb.PutRequest, 1001)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}
