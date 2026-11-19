//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestControlPlaneGatewayFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	_, err = docker.Ping(pingCtx)
	cancelPing()
	if err != nil {
		t.Fatalf("Docker required for e2e: %v", err)
	}
	manifest, err := filepath.Abs("../../compose.cp.yaml")
	if err != nil {
		t.Fatal(err)
	}
	project := "oracache-cp-e2e-" + uuid.NewString()
	image := os.Getenv("ORACACHE_CP_IMAGE")
	removeImages := compose.RemoveImagesLocal
	if image == "" {
		image = project + ":local"
		removeImages = compose.RemoveImagesAll
	} else if _, _, err := docker.ImageInspectWithRaw(ctx, image); err != nil {
		t.Fatalf("prebuilt ORACACHE_CP_IMAGE %q required: %v", image, err)
	}
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackFiles(manifest),
		compose.StackIdentifier(project),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := stack.Down(cleanup, compose.RemoveVolumes(true), removeImages); err != nil {
			t.Errorf("compose down: %v", err)
		}
	})
	if err := stack.WithEnv(map[string]string{
		"ORACACHE_CP_IMAGE": image, "ORACACHE_CP_PORT": "0", "ORACACHE_CP_STATUS_PORT": "0",
	}).
		Up(ctx, compose.Wait(true)); err != nil {
		t.Fatalf("compose up: %v", err)
	}
	gateway, err := stack.ServiceContainer(ctx, "api-gateway")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.PortEndpoint(ctx, "28080/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	routesURL, err := gateway.PortEndpoint(ctx, "9466/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cache := pb.NewCacheGatewayClient(conn)

	before := waitCPRoute(t, ctx, routesURL+"/routes", "")
	const key = "confirmed-before-failover"
	payload := []byte("hello\x00raft\xff")
	put := func(data []byte, expected uint64) *pb.PutResponse {
		t.Helper()
		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		out, err := cache.Put(rpcCtx, &pb.PutRequest{Key: key, Data: data, ExpectedRev: proto.Uint64(expected)})
		if err != nil || out.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
			t.Fatalf("Put (expected rev %d): response=%v error=%v", expected, out, err)
		}
		return out
	}
	seed := put(payload, 0)
	leader := seed.GetServedBy().GetInstance()
	if leader != before.Write[0].NodeID || seed.GetRev() == 0 {
		t.Fatalf("initial Put: response=%v; want a revision and leader %s", seed, before.Write[0].NodeID)
	}
	switch leader {
	case "node-1", "node-2", "node-3":
	default:
		t.Fatalf("unexpected storage node %q", leader)
	}
	container, err := stack.ServiceContainer(ctx, leader)
	if err != nil {
		t.Fatal(err)
	}
	if err := docker.ContainerKill(ctx, container.GetContainerID(), "SIGKILL"); err != nil {
		t.Fatalf("kill leader %s: %v", leader, err)
	}

	after := waitCPRoute(t, ctx, routesURL+"/routes", leader)
	nextLeader := after.Write[0].NodeID
	if after.Revision != before.Revision {
		t.Fatalf("topology revision changed on leader loss: %d → %d", before.Revision, after.Revision)
	}
	readCtx, cancelRead := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRead()
	read, err := cache.Get(readCtx, &pb.GetRequest{Key: key})
	if err != nil || read.GetStatus() != pb.GetStatus_GET_STATUS_HIT ||
		read.GetRev() != seed.GetRev() || !bytes.Equal(read.GetData(), payload) ||
		read.GetServedBy().GetInstance() != nextLeader {
		t.Fatalf("Get after failover: response=%v error=%v; want rev %d and original bytes from %s", read, err, seed.GetRev(), nextLeader)
	}
	updated := put([]byte("after failover"), seed.GetRev())
	if updated.GetServedBy().GetInstance() != nextLeader || updated.GetRev() <= seed.GetRev() {
		t.Fatalf("Put after failover: response=%v; want rev > %d from %s", updated, seed.GetRev(), nextLeader)
	}
	t.Logf("leader %s → %s through the same gateway; topology revision %d, cache revision %d → %d",
		leader, nextLeader, after.Revision, seed.GetRev(), updated.GetRev())
}

type cpRoute struct{ NodeID string }

type cpRoutes struct {
	Revision uint64             `json:"revision"`
	Read     map[uint32]cpRoute `json:"read_routes"`
	Write    map[uint32]cpRoute `json:"write_routes"`
}

func waitCPRoute(t *testing.T, ctx context.Context, url, previous string) cpRoutes {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		routes, err := readCPRoutes(ctx, client, url)
		node := routes.Write[0].NodeID
		if err == nil && routes.Revision != 0 && node != "" && node != previous && routes.Read[0].NodeID == node {
			return routes
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for read/write route different from %q: last routes=%+v error=%v: %v", previous, routes, err, ctx.Err())
		case <-ticker.C:
		}
	}
}

func readCPRoutes(ctx context.Context, client *http.Client, url string) (cpRoutes, error) {
	var routes cpRoutes
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return routes, err
	}
	res, err := client.Do(req)
	if err != nil {
		return routes, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return routes, fmt.Errorf("GET /routes: %s", res.Status)
	}
	err = json.NewDecoder(res.Body).Decode(&routes)
	return routes, err
}
