//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cpv1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/controller"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cplink"
	"github.com/OraCache/oracache/internal/scheduler"
	"github.com/OraCache/oracache/internal/statestore"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStaticTopologyController(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	pki, state := t.TempDir(), t.TempDir()
	output, err := exec.CommandContext(t.Context(), filepath.Join(root, "integration/e2e/runtime-test-pki.sh"), pki).CombinedOutput()
	if err != nil {
		t.Fatalf("test PKI: %v\n%s", err, output)
	}
	for _, name := range []string{"gateway", "node-1", "node-2", "node-3"} {
		path := filepath.Join(state, name)
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o777); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("ORACACHE_TEST_PKI", pki)
	t.Setenv("ORACACHE_TEST_STATE", state)
	project := fmt.Sprintf("oracache-topology-%d", os.Getpid())
	composeArgs := []string{"compose", "-p", project, "-f", filepath.Join(root, "compose.cp-runtime.yaml"), "-f", filepath.Join(root, "compose.cp-topology.yaml")}
	compose := func(args ...string) string {
		t.Helper()
		return docker(t, append(append([]string(nil), composeArgs...), args...)...)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(compose("logs", "--tail=15"))
		}
		compose("down", "--volumes", "--remove-orphans")
	})
	compose("up", "-d", "etcd-1", "etcd-2", "etcd-3", "cp-region-a", "cp-region-b", "controller-1", "controller-2")
	compose("run", "--rm", "--no-deps", "bootstrap")
	compose("up", "-d", "node-1", "node-2", "node-3", "api-gateway")
	const prefix = "/runtime"
	etcdEndpoint := "https://" + compose("port", "etcd-1", "2379")
	t.Setenv("ORACACHE_ETCD_TLS_CA", filepath.Join(pki, "etcd-ca.pem"))
	t.Setenv("ORACACHE_ETCD_TLS_CERT", filepath.Join(pki, "cp-etcd.pem"))
	t.Setenv("ORACACHE_ETCD_TLS_KEY", filepath.Join(pki, "cp-etcd.key"))
	client, err := cplink.DialEtcd(etcdEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cp, err := controller.New(client, prefix, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	store, err := statestore.NewEtcd(client, prefix, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var before controller.Status
	eventually(t, 20*time.Second, func() bool {
		var err error
		before, err = cp.Status(t.Context())
		return err == nil && before.Revision == 1 && before.ControllerID != ""
	})
	// Exercise the actual operator CLI in the fixture network.
	operator := func(command, input string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
		defer cancel()
		args := append(append([]string(nil), composeArgs...), "run", "-T", "--quiet", "--rm", "--no-deps", "--entrypoint", "/cp-topology", "controller-1", command,
			"--etcd-endpoints=https://etcd-1:2379,https://etcd-2:2379,https://etcd-3:2379", "--etcd-prefix="+prefix)
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdin = strings.NewReader(input)
		return cmd.Output()
	}
	desired, err := cp.Desired(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	desired.Nodes[0].Zone = "zone-one"
	inventory := scheduler.Input{Nodes: desired.Nodes, Policy: map[string]scheduler.Policy{"region-a": {RF: 3}}}
	input, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator("plan", string(input))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if _, err := operator("validate", string(plan)); err != nil {
		t.Fatalf("validate: %v", err)
	}
	value, err := operator("apply", string(plan))
	var result controller.Result
	if err != nil || json.Unmarshal(value, &result) != nil || !result.Changed || result.Revision != 2 {
		t.Fatalf("apply = %s, %v", value, err)
	}
	t.Logf("Controller %s published revision %d from the operator plan", before.ControllerID, result.Revision)
	t.Setenv("ORACACHE_CP_TLS_CA", filepath.Join(pki, "cp-ca.pem"))
	t.Setenv("ORACACHE_CP_TLS_CERT", filepath.Join(pki, "gw-local.pem"))
	t.Setenv("ORACACHE_CP_TLS_KEY", filepath.Join(pki, "gw-local.key"))
	t.Setenv("ORACACHE_CP_CLUSTER_ID", "runtime-test")
	for _, api := range []string{"cp-region-a", "cp-region-b"} {
		conn, err := cplink.Dial(compose("port", api, "28081"))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		eventually(t, 10*time.Second, func() bool {
			r, err := conn.GetConfig(t.Context(), &cpv1.GetConfigRequest{NodeId: "gw-local", Kind: cpv1.AgentKind_AGENT_KIND_GATEWAY})
			return err == nil && r.GetRevision() == 2
		})
	}
	gatewayStatus := compose("port", "api-gateway", "9466")
	eventually(t, 10*time.Second, func() bool {
		code, body := runtimeHTTP(gatewayStatus, "/routes")
		var routes struct {
			Revision   uint64                     `json:"revision"`
			ReadRoutes map[string]json.RawMessage `json:"read_routes"`
		}
		return code == 200 && json.Unmarshal([]byte(body), &routes) == nil && routes.Revision == 2 && len(routes.ReadRoutes) > 0
	})

	// Removing a voter from the live group appears in the plan and is rejected.
	membership, err := operator("dry-run", `{"policy":{"region-a":{"rf":2}}}`)
	if err != nil {
		t.Fatal(err)
	}
	var difference scheduler.Plan
	if err := json.Unmarshal(membership, &difference); err != nil || len(difference.Changes) != 1 {
		t.Fatalf("expected one membership change: %s, %v", membership, err)
	}
	if value, err := operator("apply", string(membership)); err == nil || !strings.Contains(string(value), "confirmed Raft membership") {
		t.Fatalf("membership apply=%s, %v", value, err)
	}
	desired, err = cp.Desired(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	desired.Regions[0].Role = cpconfig.RegionRoleSlave
	desired.Regions = append(desired.Regions, cpconfig.Region{ID: "region-b", Role: cpconfig.RegionRoleMaster})
	desired.Nodes = append(desired.Nodes, cpconfig.Node{ID: "b-1", Region: "region-b", Address: "b-1:28091"})
	promotion, err := json.Marshal(scheduler.Input{Regions: desired.Regions, Nodes: desired.Nodes,
		Policy: map[string]scheduler.Policy{"region-a": {RF: 3}, "region-b": {RF: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	promotionPlan, err := operator("plan", string(promotion))
	if err != nil {
		t.Fatalf("promotion plan: %v", err)
	}
	if value, err := operator("apply", string(promotionPlan)); err == nil || !strings.Contains(string(value), "changing the master role") {
		t.Fatalf("promotion apply=%s, %v", value, err)
	}
	published, mod, err := store.VersionedConfig(t.Context())
	if err != nil || published.Revision != 2 {
		t.Fatalf("upgrade changed revision: %d, %v", published.Revision, err)
	}
	election, err := client.Get(t.Context(), prefix+"/controller/election/", clientv3.WithFirstCreate()...)
	if err != nil || len(election.Kvs) != 1 {
		t.Fatalf("election: %v, %v", election, err)
	}
	previous := election.Kvs[0]
	old := statestore.ElectionFence{Key: string(previous.Key), Revision: previous.CreateRevision, Lease: clientv3.LeaseID(previous.Lease)}
	compose("kill", string(previous.Value))
	eventually(t, 12*time.Second, func() bool {
		now, err := cp.Status(t.Context())
		return err == nil && now.ControllerID != "" && now.ControllerID != string(previous.Value) && now.Revision == 2
	})
	// Keep the snapshot CAS and request CAS current so only the obsolete
	// election fence can reject the delayed transaction.
	pendingKey := prefix + "/test/stale"
	stale, err := client.Put(t.Context(), pendingKey, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	late := published.Config.Clone()
	late.Revision = 3
	if err := store.CommitIntent(t.Context(), late, mod, old, pendingKey, stale.Header.Revision, prefix+"/test/result", "{}"); !errors.Is(err, statestore.ErrPublicationConflict) {
		t.Fatalf("old election fence result: %v", err)
	}
	value, err = operator("apply", string(plan))
	if err != nil || json.Unmarshal(value, &result) != nil || result.Revision != 2 {
		t.Fatalf("replayed intent = %s, %v", value, err)
	}
	unchangedPlan, err := operator("plan", `{"policy":{"region-a":{"rf":3}}}`)
	if err != nil {
		t.Fatalf("plan after failover: %v", err)
	}
	value, err = operator("apply", string(unchangedPlan))
	if err != nil || json.Unmarshal(value, &result) != nil || result.Revision != 2 || result.Changed {
		t.Fatalf("new Controller no-op result = %s, %v", value, err)
	}
	beforeExpiry, err := cp.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	compose("stop", "-t", "0", "node-1", "node-2", "node-3")
	eventually(t, 12*time.Second, func() bool {
		state, err := cp.Status(t.Context())
		return err == nil && state.Revision == 2 && len(state.Differences) == 1 && len(state.Differences[0].ObservedReady) == 0
	})
	if beforeExpiry.Revision != 2 {
		t.Fatal("revision changed before expiry")
	}
	t.Log("both APIs and Gateway served revision 2; expired lease fenced the old Controller, retry retained revision 2, and heartbeat expiry changed only live-view")
}
