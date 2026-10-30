//go:build e2e

package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Ordinary tests exclude this package. Explicit e2e runs require Docker and
// fail rather than skip when it is unavailable.
func TestGateway(t *testing.T) {
	// Fail promptly before Compose's constructor starts its reaper retries.
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelPing()
	if _, err := docker.Ping(pingCtx); err != nil {
		t.Fatalf("Docker required for e2e: %v", err)
	}
	manifest, err := filepath.Abs("../../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackFiles(manifest),
		compose.StackIdentifier("oracache-e2e-"+uuid.NewString()),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Register before Up so partial startup failures are cleaned up as well.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true), compose.RemoveImagesLocal); err != nil {
			t.Errorf("compose down: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// Unique project, no image override: Up builds this manifest's Dockerfile.
	if err := stack.WithEnv(map[string]string{"ORACACHE_PORT": "0"}).
		WaitForService("gateway", wait.ForLog("listening ").WithStartupTimeout(30*time.Second)).
		Up(ctx, compose.Wait(true)); err != nil {
		t.Fatalf("compose up (Docker required): %v", err)
	}
	gateway, err := stack.ServiceContainer(ctx, "gateway")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.PortEndpoint(ctx, "50051/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewCacheGatewayClient(conn)
	readyCtx, cancelReady := context.WithTimeout(ctx, 15*time.Second)
	defer cancelReady()
	// A real bounded RPC verifies readiness after the listening log.
	ready, err := client.Get(readyCtx, &pb.GetRequest{Key: "readiness"}, grpc.WaitForReady(true))
	if err != nil || ready.GetStatus() != pb.GetStatus_GET_STATUS_MISS {
		t.Fatalf("gRPC readiness: response=%v error=%v", ready, err)
	}

	check := func(got, want proto.Message, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	}
	get := func(rev uint64, want *pb.GetResponse) {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		got, err := client.Get(ctx, &pb.GetRequest{Key: "key", Rev: rev})
		check(got, want, err)
	}
	put := func(data []byte, expected *uint64, status pb.PutStatus, rev uint64) {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		got, err := client.Put(ctx, &pb.PutRequest{Key: "key", Data: data, ExpectedRev: expected})
		check(got, &pb.PutResponse{Status: status, Rev: rev}, err)
	}
	del := func(rev uint64, status pb.DeleteStatus) {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		got, err := client.Delete(ctx, &pb.DeleteRequest{Key: "key", Rev: rev})
		check(got, &pb.DeleteResponse{Status: status}, err)
	}
	miss := &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}
	payload := []byte("hello\x00cache\xff")
	get(0, miss)
	put(payload, proto.Uint64(0), pb.PutStatus_PUT_STATUS_OK, 1)
	get(0, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 1, LatestRev: 1, Data: payload})
	put([]byte("stale"), proto.Uint64(0), pb.PutStatus_PUT_STATUS_CONFLICT, 1)
	put([]byte("new\x00\xfe"), proto.Uint64(1), pb.PutStatus_PUT_STATUS_OK, 2)
	get(0, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 2, LatestRev: 2, Data: []byte("new\x00\xfe")})
	get(1, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 1, LatestRev: 2, Data: payload})
	get(99, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND, LatestRev: 2})
	del(99, pb.DeleteStatus_DELETE_STATUS_NOT_FOUND)
	del(2, pb.DeleteStatus_DELETE_STATUS_DELETED)
	get(0, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 1, LatestRev: 1, Data: payload})
	del(0, pb.DeleteStatus_DELETE_STATUS_DELETED)
	get(0, miss)
	del(0, pb.DeleteStatus_DELETE_STATUS_NOT_FOUND)
	put([]byte("after delete"), proto.Uint64(0), pb.PutStatus_PUT_STATUS_OK, 3)
	put(nil, nil, pb.PutStatus_PUT_STATUS_OK, 4)
	get(0, &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 4, LatestRev: 4})
	batchCtx, cancelBatch := context.WithTimeout(ctx, 5*time.Second)
	defer cancelBatch()
	puts, err := client.BatchPut(batchCtx, &pb.BatchPutRequest{Items: []*pb.PutRequest{
		{Key: "key", Data: payload, ExpectedRev: proto.Uint64(4)},
		{Key: "key", ExpectedRev: proto.Uint64(4)},
	}})
	check(puts, &pb.BatchPutResponse{Items: []*pb.PutResponse{
		{Status: pb.PutStatus_PUT_STATUS_OK, Rev: 5},
		{Status: pb.PutStatus_PUT_STATUS_CONFLICT, Rev: 5},
	}}, err)
	gets, err := client.BatchGet(batchCtx, &pb.BatchGetRequest{Items: []*pb.GetRequest{
		{Key: "key"}, {Key: "absent"}, {Key: "key", Rev: 4},
	}})
	check(gets, &pb.BatchGetResponse{Items: []*pb.GetResponse{
		{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 5, LatestRev: 5, Data: payload},
		{Status: pb.GetStatus_GET_STATUS_MISS},
		{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 4, LatestRev: 5},
	}}, err)
	del(0, pb.DeleteStatus_DELETE_STATUS_DELETED)
	get(0, miss)
}
