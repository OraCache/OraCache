//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cpv1 "github.com/OraCache/oracache/gen/controlplane/v1"
	gwv1 "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cplink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestEtcdHARuntime(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	pki, state := t.TempDir(), t.TempDir()
	output, err := exec.CommandContext(t.Context(), filepath.Join(root, "integration/e2e/runtime-test-pki.sh"), pki).CombinedOutput()
	if err != nil {
		t.Fatalf("generate test PKI: %v\n%s", err, output)
	}
	for _, name := range []string{"gateway", "node-1", "node-2", "node-3"} {
		dir := filepath.Join(state, name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o777); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("ORACACHE_TEST_PKI", pki)
	t.Setenv("ORACACHE_TEST_STATE", state)
	t.Setenv("ORACACHE_CP_CLUSTER_ID", "runtime-test")
	t.Setenv("ORACACHE_CP_TLS_CA", filepath.Join(pki, "cp-ca.pem"))
	project := fmt.Sprintf("oracache-ha-%d", os.Getpid())
	composeArgs := []string{"compose", "-p", project, "-f", filepath.Join(root, "compose.cp-runtime.yaml")}
	compose := func(args ...string) string {
		t.Helper()
		return docker(t, append(append([]string(nil), composeArgs...), args...)...)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(compose("logs", "--tail=25"))
		}
		compose("down", "--volumes", "--remove-orphans")
	})
	addr := func(service, port string) string { return compose("port", service, port) }
	compose("up", "-d", "etcd-1", "etcd-2", "etcd-3", "cp-region-a", "cp-region-b")
	compose("run", "--rm", "--no-deps", "bootstrap")
	apiA, apiB := addr("cp-region-a", "28081"), addr("cp-region-b", "28081")
	statusB := addr("cp-region-b", "9464")
	dial := func(address, leaf string) *cplink.Client {
		t.Helper()
		t.Setenv("ORACACHE_CP_TLS_CERT", filepath.Join(pki, leaf+".pem"))
		t.Setenv("ORACACHE_CP_TLS_KEY", filepath.Join(pki, leaf+".key"))
		client, err := cplink.Dial(address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	nodeA, nodeB := dial(apiA, "node-1"), dial(apiB, "node-1")
	gatewayIdentity := dial(apiA, "gw-local")
	gatewayOnB := dial(apiB, "gw-local")
	untrusted := dial(apiA, "untrusted")
	heartbeat := func(client *cplink.Client, node, region string) (*cpv1.HeartbeatResponse, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		return client.ReportHeartbeat(ctx, &cpv1.HeartbeatRequest{NodeId: node, Region: region})
	}
	for _, client := range []*cplink.Client{nodeA, nodeB} {
		eventually(t, 10*time.Second, func() bool {
			resp, err := heartbeat(client, "node-1", "region-a")
			return err == nil && resp.GetAccepted() && resp.GetConfigRevision() == 1
		})
		response, err := client.GetConfig(t.Context(), &cpv1.GetConfigRequest{NodeId: "node-1", Kind: cpv1.AgentKind_AGENT_KIND_STORAGE_NODE})
		if err != nil || response.GetConfig().GetRevision() != 1 {
			t.Fatalf("authoritative config: %v, %v", response, err)
		}
	}
	for _, identity := range []struct {
		client       *cplink.Client
		node, region string
	}{
		{nodeA, "node-2", "region-a"}, {nodeA, "node-1", "region-b"}, {gatewayIdentity, "gw-local", "region-a"},
	} {
		if _, err := heartbeat(identity.client, identity.node, identity.region); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unauthorized heartbeat: %v", err)
		}
	}
	if _, err := heartbeat(untrusted, "node-1", "region-a"); status.Code(err) != codes.Unavailable {
		t.Fatalf("untrusted client: %v", err)
	}
	t.Setenv("ORACACHE_CP_TLS_CA", filepath.Join(pki, "other-ca.pem"))
	wrongCA := dial(apiA, "node-1")
	if _, err := heartbeat(wrongCA, "node-1", "region-a"); status.Code(err) != codes.Unavailable {
		t.Fatalf("untrusted server: %v", err)
	}
	t.Setenv("ORACACHE_CP_TLS_CA", filepath.Join(pki, "cp-ca.pem"))
	t.Log("both regional APIs served revision 1; mTLS rejected untrusted peers and unauthorized heartbeat identities")

	compose("up", "-d", "node-1", "node-2", "node-3", "api-gateway")
	dataAddr, gwStatus := addr("api-gateway", "28080"), addr("api-gateway", "9466")
	conn, err := grpc.NewClient(dataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	gateway := gwv1.NewCacheGatewayClient(conn)
	put := func(key string) bool {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		resp, err := gateway.Put(ctx, &gwv1.PutRequest{Key: key, Data: []byte("v")})
		return err == nil && resp.GetStatus() == gwv1.PutStatus_PUT_STATUS_OK
	}
	get := func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		resp, err := gateway.Get(ctx, &gwv1.GetRequest{Key: "seed"})
		return err == nil && resp.GetStatus() == gwv1.GetStatus_GET_STATUS_HIT && string(resp.GetData()) == "v"
	}
	eventually(t, 25*time.Second, func() bool { return put("seed") && get() })
	for _, name := range []string{"gateway", "node-1", "node-2", "node-3"} {
		eventually(t, 10*time.Second, func() bool { _, err := os.Stat(filepath.Join(state, name, "config.json")); return err == nil })
	}

	compose("stop", "-t", "0", "cp-region-a")
	stoppedAt := time.Now()
	eventually(t, 15*time.Second, func() bool {
		if time.Since(stoppedAt) <= 5*time.Second {
			return false
		}
		config, err := gatewayOnB.GetConfig(t.Context(), &cpv1.GetConfigRequest{NodeId: "gw-local", Kind: cpv1.AgentKind_AGENT_KIND_GATEWAY})
		if err != nil || len(config.GetConfig().GetPartitions()) == 0 || !config.GetConfig().GetPartitions()[0].GetReady() {
			return false
		}
		_, metrics := runtimeHTTP(gwStatus, "/metrics")
		return freshGatewaySnapshot(metrics) && get() && put("cp-failover")
	})
	t.Log("Gateway and embedded agents stayed operational through the other regional API")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	rejected := exec.CommandContext(ctx, "docker", append(append([]string(nil), composeArgs...), "exec", "-T", "-e", "ETCDCTL_CERT=/pki/gw-local.pem", "-e", "ETCDCTL_KEY=/pki/gw-local.key", "etcd-1", "etcdctl", "--command-timeout=2s", "get", "/runtime/config/snapshot")...)
	err = rejected.Run()
	cancel()
	if err == nil {
		t.Fatal("etcd accepted a client certificate from the CP client CA")
	}
	compose("exec", "-T", "etcd-1", "etcdctl", "snapshot", "save", "/tmp/backup.db")
	container := compose("ps", "-q", "etcd-1")
	backup := t.TempDir()
	docker(t, "cp", container+":/tmp/backup.db", filepath.Join(backup, "backup.db"))
	compose("stop", "-t", "0", "etcd-3")
	eventually(t, 20*time.Second, func() bool {
		code, _ := runtimeHTTP(statusB, "/readyz")
		response, err := nodeB.GetConfig(t.Context(), &cpv1.GetConfigRequest{NodeId: "node-1", Kind: cpv1.AgentKind_AGENT_KIND_STORAGE_NODE})
		return code == 200 && err == nil && response.GetRevision() == 1 && get()
	})
	t.Log("two etcd members retained quorum")

	compose("stop", "-t", "0", "etcd-2")
	eventually(t, 15*time.Second, func() bool {
		live, _ := runtimeHTTP(statusB, "/healthz")
		ready, _ := runtimeHTTP(statusB, "/readyz")
		_, metrics := runtimeHTTP(statusB, "/metrics")
		_, err := nodeB.GetConfig(t.Context(), &cpv1.GetConfigRequest{NodeId: "node-1", Kind: cpv1.AgentKind_AGENT_KIND_STORAGE_NODE})
		return live == 200 && ready == 503 && status.Code(err) == codes.Unavailable &&
			strings.Contains(metrics, "oracache_cp_statestore_errors_total") && get() && put("etcd-outage")
	})
	t.Log("etcd quorum loss returned Unavailable and preserved data operations and diagnostics")

	compose("restart", "-t", "0", "api-gateway")
	gwStatus = addr("api-gateway", "9466")
	restarted, err := grpc.NewClient(addr("api-gateway", "28080"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	gateway = gwv1.NewCacheGatewayClient(restarted)
	eventually(t, 20*time.Second, func() bool { return get() })
	ready, _ := runtimeHTTP(gwStatus, "/readyz")
	if ready != 503 || put("cold") {
		t.Fatalf("cold Gateway authorized writes (ready code %d)", ready)
	}
	compose("start", "etcd-2")
	eventually(t, 20*time.Second, func() bool {
		code, _ := runtimeHTTP(gwStatus, "/readyz")
		return code == 200 && put("recovered")
	})
	t.Log("Gateway restored reads from disk, fenced writes across restart, then recovered from live CP")

	compose("stop", "-t", "0", "node-2", "node-3")
	eventually(t, 15*time.Second, func() bool { return !get() && !put("raft-no-quorum") })
	t.Log("loss of the Raft majority rejected strict reads and writes with the Control Plane available")

	testRuntimeRestore(t, backup, project)
}

func freshGatewaySnapshot(metrics string) bool {
	const name = "oracache_gateway_config_snapshot_age_seconds "
	for line := range strings.SplitSeq(metrics, "\n") {
		if value, ok := strings.CutPrefix(line, name); ok {
			age, err := strconv.ParseFloat(value, 64)
			return err == nil && age >= 0 && age < 4
		}
	}
	return false
}

func runtimeHTTP(address, path string) (int, string) {
	client := http.Client{Timeout: 6 * time.Second}
	response, err := client.Get("http://" + address + path)
	if err != nil {
		return 0, ""
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, ""
	}
	return response.StatusCode, string(data)
}

func testRuntimeRestore(t *testing.T, backup, project string) {
	volume := docker(t, "volume", "create", project+"-restore")
	t.Cleanup(func() { docker(t, "volume", "rm", volume) })
	const image = "quay.io/coreos/etcd:v3.6.16"
	docker(t, "run", "--rm", "-v", backup+":/backup:ro", "-v", volume+":/restore", image,
		"/usr/local/bin/etcdutl", "snapshot", "restore", "/backup/backup.db", "--data-dir=/restore/db", "--bump-revision=10000", "--mark-compacted")
	restored := docker(t, "run", "--detach", "--publish", "127.0.0.1::2379", "-v", volume+":/restore", image,
		"/usr/local/bin/etcd", "--data-dir=/restore/db", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://127.0.0.1:2379")
	t.Cleanup(func() { docker(t, "rm", "--force", restored) })
	options := storeOptions{backend: "etcd", endpoints: "http://" + docker(t, "port", restored, "2379/tcp"), prefix: "/runtime", nodeTTL: integrationNodeTTL}
	eventually(t, 10*time.Second, func() bool {
		client, err := cplink.DialEtcd(options.endpoints)
		if err != nil {
			return false
		}
		defer client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		response, err := client.Get(ctx, "/runtime/config/snapshot")
		return err == nil && len(response.Kvs) == 1 && response.Header.Revision >= 10000
	})
	store, revision, closeStore := openTestStore(t, options)
	defer closeStore()
	if revision != 1 {
		t.Fatalf("restored app revision = %d, want 1", revision)
	}
	api := cpapi.New(store, cpapi.WithMinimumRevision(3))
	request := &cpv1.GetConfigRequest{NodeId: "node-1", Kind: cpv1.AgentKind_AGENT_KIND_STORAGE_NODE}
	if _, err := api.GetConfig(t.Context(), request); status.Code(err) != codes.Unavailable || api.Ready(t.Context()) {
		t.Fatalf("restored stale configuration served: %v", err)
	}
	if err := recoverConfig(t.Context(), options, 3); err != nil {
		t.Fatal(err)
	}
	response, err := api.GetConfig(t.Context(), request)
	if err != nil || response.GetRevision() != 3 || !api.Ready(t.Context()) {
		t.Fatalf("recover config: %v, %v", response, err)
	}
	if h, ok, err := store.Heartbeat(t.Context(), "node-1"); ok || err != nil {
		t.Fatalf("restored heartbeat: %+v, %v", h, err)
	}
	t.Log("isolated snapshot restore bumped etcd MVCC independently; application floor rejected revision 1 until explicit recovery to 3")
}
