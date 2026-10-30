package main

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// A legal batch of 1000 maximum-size payloads exceeds gRPC's default 4 MiB
// receive limit. Exercise the actual executable's server configuration.
func TestMaximumPayloadBatch(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := newServer()
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///cache", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := pb.NewCacheGatewayClient(conn)
	puts := make([]*pb.PutRequest, 1000)
	gets := make([]*pb.GetRequest, 1000)
	for i := range puts {
		puts[i] = &pb.PutRequest{Key: "k", Data: make([]byte, 10240)}
		gets[i] = &pb.GetRequest{Key: "k", Rev: uint64(i + 1)}
	}
	p, err := client.BatchPut(ctx, &pb.BatchPutRequest{Items: puts})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1000 || p.Items[999].Rev != 1000 {
		t.Fatal("incomplete batch")
	}
	g, err := client.BatchGet(ctx, &pb.BatchGetRequest{Items: gets})
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Items) != 1000 || len(g.Items[999].Data) != 10240 {
		t.Fatal("incomplete payload")
	}
}
