//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const integrationNodeTTL = 4 * time.Second

func TestEtcdControlPlane(t *testing.T) {
	container := docker(t, "run", "--detach", "--publish", "127.0.0.1::2379",
		"quay.io/coreos/etcd:v3.6.16", "/usr/local/bin/etcd",
		"--listen-client-urls", "http://0.0.0.0:2379",
		"--advertise-client-urls", "http://127.0.0.1:2379", "--data-dir", "/integration-etcd")
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(docker(t, "logs", "--tail", "40", container))
		}
		docker(t, "rm", "--force", container)
	})
	options := storeOptions{
		backend:   "etcd",
		endpoints: "http://" + docker(t, "port", container, "2379/tcp"),
		prefix:    "/readiness",
		nodeTTL:   integrationNodeTTL,
	}
	eventually(t, 10*time.Second, func() bool {
		_, _, closeStore, err := openStore(t.Context(), options)
		if err != nil {
			return false
		}
		closeStore()
		return true
	})

	t.Run("snapshot_survives_restart_and_concurrent_publishers", func(t *testing.T) {
		options.prefix = "/restart"
		testEtcdSnapshot(t, options)
	})
	t.Run("lease_expiry_and_receipt_retention", func(t *testing.T) {
		options.prefix = "/leases"
		testEtcdLeases(t, options)
	})
	t.Run("live_heartbeat_belongs_to_current_membership", func(t *testing.T) {
		options.prefix = "/membership"
		options.nodeTTL = 30 * time.Second
		testEtcdHeartbeatMembership(t, options)
	})
	t.Run("live_view_uses_highest_observed_raft_term", func(t *testing.T) {
		options.prefix = "/raft-term"
		options.nodeTTL = 30 * time.Second
		store, _, _ := openTestStore(t, options)
		publishConfig(t, store, cpconfigtest.Config(1))
		report := func(node string, term uint64, role statestore.RaftRole) {
			t.Helper()
			if _, err := store.UpsertHeartbeat(t.Context(), statestore.Heartbeat{
				NodeID: node, Region: "eu", ReceivedAt: time.Now(),
				Partitions: []statestore.PartitionStatus{{PartitionID: 0, Term: term, Role: role, Ready: true}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		report("eu-1", 1, statestore.RaftRoleLeader)
		report("eu-2", 2, statestore.RaftRoleFollower)

		view, err := store.LiveConfig(t.Context(), time.Now(), options.nodeTTL)
		if err != nil {
			t.Fatal(err)
		}
		assignment, ok := view.Config.Assignment("eu", 0)
		if !ok || view.Revision != 1 || assignment.ObservedTerm != 2 ||
			assignment.Readable || assignment.Writable ||
			!reflect.DeepEqual(assignment.ReadyReplicaNodeIDs, []string{"eu-2"}) {
			t.Fatalf("revision %d: expected only the term-2 follower to be eligible: %+v", view.Revision, assignment)
		}
	})
	t.Run("backend_outage_keeps_gateway_map", func(t *testing.T) {
		options.prefix = "/outage"
		store, _, _ := openTestStore(t, options)
		publishConfig(t, store, cpconfigtest.Config(1))
		client, api, _ := startTestCP(t, store)
		reportLeader(t, client)
		poller, router := attachTestGateway(t, client)
		before := router.Routes()
		if before.Revision != 1 || before.ReadRoutes[0].NodeID != "eu-1" {
			t.Fatalf("initial routes = %+v", before)
		}

		docker(t, "stop", "--time", "0", container)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := client.ReportHeartbeat(ctx, &controlplanev1.HeartbeatRequest{NodeId: "eu-1", Region: "eu"})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("heartbeat during etcd outage = %v, want Unavailable", err)
		}
		if err := poller.Refresh(t.Context()); status.Code(err) != codes.Unavailable {
			t.Fatalf("refresh during etcd outage = %v, want Unavailable", err)
		}
		if after := router.Routes(); !reflect.DeepEqual(after, before) {
			t.Fatalf("routes changed during etcd outage: before %+v, after %+v", before, after)
		}
		var metrics bytes.Buffer
		if err := api.Metrics().Render(&metrics); err != nil {
			t.Fatalf("cannot collect CP metrics during etcd outage: %v", err)
		}
		if !strings.Contains(metrics.String(), cpmetrics.ConfigPullTotal+`{result="error"} 1`) {
			t.Fatalf("outage not visible in CP metrics:\n%s", metrics.String())
		}
		t.Logf("gRPC Unavailable preserved route %s at revision %d", before.ReadRoutes[0].NodeID, before.Revision)
	})
}

func testEtcdSnapshot(t *testing.T, options storeOptions) {
	options.devCluster = true
	first, revision, closeStore := openTestStore(t, options)
	base := cpconfig.SyntheticDevCluster()
	if revision != base.Revision {
		t.Fatalf("bootstrap revision = %d, want %d", revision, base.Revision)
	}
	client, _, stopCP := startTestCP(t, first)
	assertConfig(t, client, base)

	second, _, _ := openTestStore(t, options)
	const highest = 12
	start := make(chan struct{})
	results := make(chan error, highest-1)
	for i := uint64(2); i <= highest; i++ {
		config := base.Clone()
		config.Revision = i
		config.Nodes[0].Address = fmt.Sprintf("publisher-%d:7000", i)
		store := first
		if i%2 == 0 {
			store = second
		}
		go func() {
			<-start
			results <- store.ApplyConfig(t.Context(), config)
		}()
	}
	close(start)
	var last uint64
	for range highest - 1 {
		if err := <-results; err != nil && !errors.Is(err, cpconfig.ErrStaleRevision) {
			t.Fatalf("concurrent publish: %v", err)
		}
		snapshot, err := second.Config(t.Context())
		if err != nil || snapshot.Config == nil || snapshot.Revision < last ||
			snapshot.Revision != snapshot.Config.Revision ||
			snapshot.Config.Nodes[0].Address != fmt.Sprintf("publisher-%d:7000", snapshot.Revision) {
			t.Fatalf("publish readback: last=%d, snapshot=%+v, err=%v", last, snapshot, err)
		}
		last = snapshot.Revision
	}
	want := base.Clone()
	want.Revision = highest
	want.Nodes[0].Address = fmt.Sprintf("publisher-%d:7000", highest)
	assertConfig(t, client, want)
	if err := second.ApplyConfig(t.Context(), want); !errors.Is(err, cpconfig.ErrStaleRevision) {
		t.Fatalf("duplicate revision = %v, want ErrStaleRevision", err)
	}
	wrongCount := cpconfigtest.Config(highest + 1)
	if err := second.ApplyConfig(t.Context(), wrongCount); !errors.Is(err, cpconfig.ErrPartitionCountChanged) {
		t.Fatalf("changed partition count = %v, want ErrPartitionCountChanged", err)
	}
	bad := want.Clone()
	bad.Revision++
	bad.Nodes[0].Address = ""
	if err := second.ApplyConfig(t.Context(), bad); !errors.Is(err, cpconfig.ErrInvalidConfig) {
		t.Fatalf("invalid config = %v, want ErrInvalidConfig", err)
	}

	stopCP()
	closeStore()
	options.devCluster = false
	options.configPath = filepath.Join(t.TempDir(), "newer-bootstrap.json")
	newerBootstrap := base.Clone()
	newerBootstrap.Revision = highest + 100
	if err := second.(*statestore.EtcdStore).BootstrapConfig(t.Context(), newerBootstrap); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(newerBootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(options.configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	restarted, revision, _ := openTestStore(t, options)
	client, _, _ = startTestCP(t, restarted)
	assertConfig(t, client, want)
	if revision != highest {
		t.Fatalf("startup revision after restart = %d, want %d", revision, highest)
	}
	t.Logf("persisted revision %d and its snapshot survived CP restart with bootstrap revision %d", revision, newerBootstrap.Revision)
}

func testEtcdLeases(t *testing.T, options storeOptions) {
	store, _, closeStore := openTestStore(t, options)
	config := cpconfigtest.Config(1)
	publishConfig(t, store, config)
	client, _, stopCP := startTestCP(t, store)
	reportLeader(t, client)

	// This view would be expired if CP wall-clock time were used for liveness.
	view, err := store.LiveConfig(t.Context(), time.Now().Add(time.Hour), integrationNodeTTL)
	if err != nil || view.Config == nil || !view.Config.Partitions[0].Writable {
		t.Fatalf("live lease with skewed CP clock: view = %+v, error = %v", view, err)
	}

	stopCP()
	closeStore()
	store, _, _ = openTestStore(t, options)
	client, api, _ := startTestCP(t, store)
	poller, router := attachTestGateway(t, client)
	if routes := router.Routes(); routes.Revision != 1 || routes.ReadRoutes[0].NodeID != "eu-1" {
		t.Fatalf("routes after restart with active lease = %+v", routes)
	}

	eventually(t, 12*time.Second, func() bool {
		_, ok, err := store.Heartbeat(t.Context(), "eu-1")
		return err == nil && !ok
	})
	if err := poller.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	routes := router.Routes()
	if routes.Revision != 1 || len(routes.ReadRoutes) != 0 || len(routes.WriteRoutes) != 0 {
		t.Fatalf("routes after lease expiry = %+v, want unroutable at revision 1", routes)
	}
	age, ok := heartbeatAge(t, api)
	if !ok || age < integrationNodeTTL.Seconds() {
		t.Fatalf("heartbeat age after lease expiry = %f, present = %t", age, ok)
	}
	t.Logf("lease expired at revision %d, heartbeat age still exposed at %.2fs", routes.Revision, age)

}

func testEtcdHeartbeatMembership(t *testing.T, options storeOptions) {
	store, _, _ := openTestStore(t, options)
	config := cpconfigtest.Config(1)
	publishConfig(t, store, config)
	client, api, _ := startTestCP(t, store)
	reportLeader(t, client)
	assertWriteReady := func(want bool) {
		t.Helper()
		view, err := store.LiveConfig(t.Context(), time.Now(), options.nodeTTL)
		if err != nil {
			t.Fatal(err)
		}
		if got := view.Config.Partitions[0].Writable; got != want {
			t.Fatalf("revision %d write readiness = %t, want %t", view.Revision, got, want)
		}
	}
	assertWriteReady(true)

	removed := config.Clone()
	removed.Revision = 2
	removed.Nodes = removed.Nodes[1:]
	for i := range removed.Partitions {
		partition := &removed.Partitions[i]
		if partition.Region == "eu" {
			partition.ReplicaNodeIDs = []string{"eu-2"}
			partition.LeaderNodeID = "eu-2"
		}
	}
	publishConfig(t, store, removed)
	reportLeader(t, client) // Reports from the removed pair must not leave receipts.
	if _, ok := heartbeatAge(t, api); ok {
		t.Fatal("removed node still has a heartbeat age metric")
	}
	config.Revision = 3
	publishConfig(t, store, config)
	if _, present, err := store.Heartbeat(t.Context(), "eu-1"); err != nil || !present {
		t.Fatalf("old lease must still exist to exercise rapid re-add: present=%t, err=%v", present, err)
	}
	if _, ok := heartbeatAge(t, api); ok {
		t.Fatal("re-added node regained a receipt from before it was configured")
	}
	assertWriteReady(false)
	reportLeader(t, client)
	if _, ok := heartbeatAge(t, api); !ok {
		t.Fatal("fresh heartbeat after re-add did not restore the age metric")
	}
	assertWriteReady(true)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := client.ReportHeartbeat(ctx, &controlplanev1.HeartbeatRequest{NodeId: "eu-1", Region: "us"})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("heartbeat not acknowledged: %v, %v", response, err)
	}
	assertWriteReady(true)
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", args[0], err, output)
	}
	return strings.TrimSpace(string(output))
}

func eventually(t *testing.T, wait time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("condition did not hold within %v", wait)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func openTestStore(t *testing.T, options storeOptions) (statestore.Store, uint64, func()) {
	t.Helper()
	store, revision, closeStore, err := openStore(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	closeStore = sync.OnceFunc(closeStore)
	t.Cleanup(closeStore)
	return store, revision, closeStore
}

func startTestCP(t *testing.T, store statestore.Store) (controlplanev1.ControlPlaneAPIClient, *cpapi.Server, func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, api := newServer(store, logger, cpapi.WithNodeTTL(integrationNodeTTL))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	stop := sync.OnceFunc(func() {
		server.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("CP server: %v", err)
		}
	})
	t.Cleanup(stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return controlplanev1.NewControlPlaneAPIClient(conn), api, stop
}

func publishConfig(t *testing.T, store statestore.Store, config *cpconfig.Config) {
	t.Helper()
	if err := store.ApplyConfig(t.Context(), config); err != nil {
		t.Fatal(err)
	}
}

func assertConfig(t *testing.T, client controlplanev1.ControlPlaneAPIClient, want *cpconfig.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := client.GetConfig(ctx, &controlplanev1.GetConfigRequest{
		NodeId: "eu-1", Kind: controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE,
	})
	if err != nil || response.GetRevision() != want.Revision || !proto.Equal(response.GetConfig(), want.ToProto()) {
		t.Fatalf("GetConfig revision = %d, error = %v, want snapshot at revision %d", response.GetRevision(), err, want.Revision)
	}
}

func reportLeader(t *testing.T, client controlplanev1.ControlPlaneAPIClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := client.ReportHeartbeat(ctx, &controlplanev1.HeartbeatRequest{
		NodeId: "eu-1", Region: "eu",
		Partitions: []*controlplanev1.PartitionStatus{{
			PartitionId: 0, Ready: true, Role: controlplanev1.RaftRole_RAFT_ROLE_LEADER, Term: 1,
		}},
	})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("heartbeat not accepted: %v, %v", response, err)
	}
}

func attachTestGateway(t *testing.T, client controlplanev1.ControlPlaneAPIClient) (*gateway.Poller, *gateway.Router) {
	t.Helper()
	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatal(err)
	}
	poller := gateway.NewPoller(client, router, "gw-eu")
	if err := poller.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	return poller, router
}

func heartbeatAge(t *testing.T, api *cpapi.Server) (float64, bool) {
	t.Helper()
	var metrics bytes.Buffer
	if err := api.Metrics().Render(&metrics); err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(metrics.String(), "\n") {
		if sample, ok := strings.CutPrefix(line, cpmetrics.HeartbeatAge+`{node_id="eu-1",region="eu"} `); ok {
			age, err := strconv.ParseFloat(sample, 64)
			if err != nil {
				t.Fatal(err)
			}
			return age, true
		}
	}
	return 0, false
}
