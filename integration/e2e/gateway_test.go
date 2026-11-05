//go:build e2e

package e2e

import (
	"context"
	"os"
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
	project := "oracache-e2e-" + uuid.NewString()
	image := os.Getenv("ORACACHE_GATEWAY_IMAGE")
	removeImages := compose.RemoveImagesLocal
	if image == "" {
		// A unique tag forces a build of this checkout, not a stale local image.
		image = project + ":local"
		removeImages = compose.RemoveImagesAll
	} else {
		// CI loads this exact image before e2e; never silently rebuild a missing one.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, _, err := docker.ImageInspectWithRaw(ctx, image); err != nil {
			t.Fatalf("prebuilt ORACACHE_GATEWAY_IMAGE %q required: %v", image, err)
		}
	}
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackFiles(manifest),
		compose.StackIdentifier(project),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Register before Up so partial startup failures are cleaned up as well.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := stack.Down(ctx, compose.RemoveOrphans(true), compose.RemoveVolumes(true), removeImages); err != nil {
			t.Errorf("compose down: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// Compose reuses an existing explicit image; otherwise it builds the unique tag.
	if err := stack.WithEnv(map[string]string{"ORACACHE_PORT": "0", "ORACACHE_GATEWAY_IMAGE": image}).
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
	rpcCtx, cancelRPC := context.WithTimeout(ctx, 15*time.Second)
	defer cancelRPC()
	get := func(want *pb.GetResponse) {
		t.Helper()
		got, err := client.Get(rpcCtx, &pb.GetRequest{Key: "key"})
		check(got, want, err)
	}
	put := func(data []byte, expected *uint64, status pb.PutStatus, rev uint64) {
		t.Helper()
		got, err := client.Put(rpcCtx, &pb.PutRequest{Key: "key", Data: data, ExpectedRev: expected})
		check(got, &pb.PutResponse{Status: status, Rev: rev}, err)
	}
	miss := &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}
	payload := []byte("hello\x00cache\xff")
	get(miss)
	put(payload, proto.Uint64(0), pb.PutStatus_PUT_STATUS_OK, 1)
	get(&pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 1, LatestRev: 1, Data: payload})
	put([]byte("stale"), proto.Uint64(0), pb.PutStatus_PUT_STATUS_CONFLICT, 1)
	// Missing expected_rev must stay unconditional across protobuf serialization.
	put(payload, nil, pb.PutStatus_PUT_STATUS_OK, 2)
	puts, err := client.BatchPut(rpcCtx, &pb.BatchPutRequest{Items: []*pb.PutRequest{
		{Key: "key", Data: payload, ExpectedRev: proto.Uint64(2)},
		{Key: "key", ExpectedRev: proto.Uint64(2)},
	}})
	check(puts, &pb.BatchPutResponse{Items: []*pb.PutResponse{
		{Status: pb.PutStatus_PUT_STATUS_OK, Rev: 3},
		{Status: pb.PutStatus_PUT_STATUS_CONFLICT, Rev: 3},
	}}, err)
	gets, err := client.BatchGet(rpcCtx, &pb.BatchGetRequest{Items: []*pb.GetRequest{
		{Key: "key"}, {Key: "absent"}, {Key: "key", Rev: 1},
	}})
	check(gets, &pb.BatchGetResponse{Items: []*pb.GetResponse{
		{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 3, LatestRev: 3, Data: payload},
		{Status: pb.GetStatus_GET_STATUS_MISS},
		{Status: pb.GetStatus_GET_STATUS_HIT, Rev: 1, LatestRev: 3, Data: payload},
	}}, err)
	deleted, err := client.Delete(rpcCtx, &pb.DeleteRequest{Key: "key"})
	check(deleted, &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_DELETED}, err)
	get(miss)
}
