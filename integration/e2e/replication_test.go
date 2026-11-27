//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
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
)

// Keep both scenarios on one Compose stack: separate stacks in the same Go
// process share a Ryuk session, which can remove the next stack during cleanup.
func TestRegionReplication(t *testing.T) {
	stack := newReplicationStack(t)
	t.Run("VersionsAndDeletion", func(t *testing.T) {
		s := *stack
		s.t = t
		first := s.put("versions", []byte("first\x00\xff"))
		second := s.put("versions", []byte("second"))
		s.waitVersion("versions", first, []byte("first\x00\xff"), second)
		s.waitVersion("versions", second, []byte("second"), second)
		s.delete("versions")
		s.waitMissing("versions")
	})

	t.Run("AfterSlavePause", func(t *testing.T) {
		s := *stack
		s.t = t
		seed := s.put("delete-while-paused", []byte("present in B"))
		s.waitVersion("delete-while-paused", seed, []byte("present in B"), seed)

		resume := s.pauseSlave()
		first := s.put("written-while-paused", []byte("first"))
		second := s.put("written-while-paused", []byte("second"))
		s.delete("delete-while-paused")
		resume()

		s.waitVersion("written-while-paused", first, []byte("first"), second)
		s.waitVersion("written-while-paused", second, []byte("second"), second)
		s.waitMissing("delete-while-paused")
	})
}

type replicationStack struct {
	t      *testing.T
	ctx    context.Context
	docker *client.Client
	stack  compose.ComposeStack
	a, b   []pb.CacheGatewayClient
}

func newReplicationStack(t *testing.T) *replicationStack {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	docker, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	_, err = docker.Ping(pingCtx)
	cancelPing()
	if err != nil {
		t.Fatalf("Docker required for e2e: %v", err)
	}
	manifest, err := filepath.Abs("../../compose.replication.yaml")
	if err != nil {
		t.Fatal(err)
	}
	project := "oracache-replication-e2e-" + uuid.NewString()
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
		if err := stack.Down(cleanup, compose.RemoveOrphans(true), compose.RemoveVolumes(true), removeImages); err != nil {
			t.Errorf("compose down: %v", err)
		}
	})
	if err := stack.WithEnv(map[string]string{
		"ORACACHE_CP_IMAGE": image, "ORACACHE_REPLICATION_PORT": "0",
	}).Up(ctx, compose.Wait(true), compose.RunServices("cp-api", "a-1", "a-2", "a-3", "b-1", "b-2", "b-3")); err != nil {
		t.Fatalf("compose up: %v", err)
	}
	s := &replicationStack{t: t, ctx: ctx, docker: docker, stack: stack}
	for _, group := range []struct {
		region string
		out    *[]pb.CacheGatewayClient
	}{{"a", &s.a}, {"b", &s.b}} {
		for i := 1; i <= 3; i++ {
			container, err := stack.ServiceContainer(ctx, fmt.Sprintf("%s-%d", group.region, i))
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := container.PortEndpoint(ctx, "28091/tcp", "")
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			*group.out = append(*group.out, pb.NewCacheGatewayClient(conn))
		}
	}
	s.waitRead(s.b, &pb.GetRequest{Key: "readiness"}, func(r *pb.GetResponse) bool {
		return r.Status == pb.GetStatus_GET_STATUS_MISS
	})
	return s
}

func (s *replicationStack) sourceLeader() pb.CacheGatewayClient {
	s.t.Helper()
	return s.waitRead(s.a, &pb.GetRequest{Key: "readiness"}, func(r *pb.GetResponse) bool {
		return r.Status == pb.GetStatus_GET_STATUS_MISS
	})
}

func (s *replicationStack) put(key string, data []byte) uint64 {
	s.t.Helper()
	leader := s.sourceLeader()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	out, err := leader.Put(ctx, &pb.PutRequest{Key: key, Data: data})
	if err != nil || out.GetStatus() != pb.PutStatus_PUT_STATUS_OK || out.GetRev() == 0 {
		s.t.Fatalf("Put %q: response=%v error=%v", key, out, err)
	}
	return out.Rev
}

func (s *replicationStack) delete(key string) {
	s.t.Helper()
	leader := s.sourceLeader()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	out, err := leader.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil || out.GetStatus() != pb.DeleteStatus_DELETE_STATUS_DELETED {
		s.t.Fatalf("Delete %q: response=%v error=%v", key, out, err)
	}
}

func (s *replicationStack) waitVersion(key string, rev uint64, data []byte, latest uint64) {
	s.t.Helper()
	s.waitRead(s.b, &pb.GetRequest{Key: key, Rev: rev}, func(r *pb.GetResponse) bool {
		return r.Status == pb.GetStatus_GET_STATUS_HIT && r.Rev == rev &&
			r.LatestRev == latest && bytes.Equal(r.Data, data)
	})
}

func (s *replicationStack) waitMissing(key string) {
	s.t.Helper()
	s.waitRead(s.b, &pb.GetRequest{Key: key}, func(r *pb.GetResponse) bool {
		return r.Status == pb.GetStatus_GET_STATUS_MISS
	})
}

func (s *replicationStack) waitRead(clients []pb.CacheGatewayClient, req *pb.GetRequest, matches func(*pb.GetResponse) bool) pb.CacheGatewayClient {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last *pb.GetResponse
	var lastErr error
	for {
		for _, c := range clients {
			attempt, cancel := context.WithTimeout(ctx, time.Second)
			got, err := c.Get(attempt, req)
			cancel()
			if err == nil {
				last = got
				if matches(got) {
					return c
				}
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			s.t.Fatalf("waiting for Get %v: last response=%v error=%v: %v", req, last, lastErr, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *replicationStack) pauseSlave() func() {
	s.t.Helper()
	var paused []string
	resume := func() {
		s.t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var remaining []string
		for _, id := range paused {
			if err := s.docker.ContainerUnpause(ctx, id); err != nil {
				s.t.Errorf("unpause slave %s: %v", id, err)
				remaining = append(remaining, id)
			}
		}
		paused = remaining
	}
	s.t.Cleanup(resume)
	for i := 1; i <= 3; i++ {
		container, err := s.stack.ServiceContainer(s.ctx, fmt.Sprintf("b-%d", i))
		if err != nil {
			s.t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		err = s.docker.ContainerPause(ctx, container.GetContainerID())
		cancel()
		if err != nil {
			s.t.Fatalf("pause slave b-%d: %v", i, err)
		}
		paused = append(paused, container.GetContainerID())
	}
	return resume
}
