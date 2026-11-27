//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	membership "github.com/OraCache/oracache/gen/membership/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestStagedGeoDemo(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	project := "oracache-geo-e2e-" + uuid.NewString()
	image := os.Getenv("ORACACHE_CP_IMAGE")
	build := image == ""
	if build {
		image = project + ":local"
	}
	command := func(ctx context.Context, name string, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Dir = "../.."
		cmd.Env = append(os.Environ(), "COMPOSE_PROJECT_NAME="+project, "COMPOSE_PROFILES=", "ORACACHE_CP_IMAGE="+image, "ORACACHE_REPLICATION_PORT=0")
		out, err := cmd.Output()
		if exit, ok := err.(*exec.ExitError); ok {
			err = fmt.Errorf("%w: %s", err, exit.Stderr)
		}
		return strings.TrimSpace(string(out)), err
	}
	compose := func(ctx context.Context, args ...string) (string, error) {
		return command(ctx, "docker", append([]string{"compose", "-f", "compose.replication.yaml"}, args...)...)
	}
	makeDemo := func(targets ...string) {
		t.Helper()
		args := []string{"-j1"}
		if !build {
			args = append(args, "-o", "demo-build") // CI already built the image.
		}
		if out, err := command(ctx, "make", append(args, targets...)...); err != nil {
			t.Fatalf("make %v: %v\n%s", targets, err, out)
		}
	}
	run := func(args ...string) string {
		t.Helper()
		out, err := compose(ctx, args...)
		if err != nil {
			t.Fatalf("compose %v: %v\n%s", args, err, out)
		}
		return out
	}
	profiles := []string{"--profile", "learner"}
	bNodes := []string{"b-1", "b-2", "b-3"}
	t.Logf("Compose project: %s", project)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if t.Failed() {
			out, err := compose(cleanup, append(slices.Clone(profiles), "logs", "--tail=25")...)
			t.Logf("Compose logs: %v\n%s", err, out)
		}
		if out, err := command(cleanup, "make", "demo-down"); err != nil {
			t.Errorf("make demo-down: %v\n%s", err, out)
		}
		if build {
			// A failed build may not have created the image.
			if exec.CommandContext(cleanup, "docker", "image", "inspect", image).Run() == nil {
				if out, err := exec.CommandContext(cleanup, "docker", "image", "rm", image).CombinedOutput(); err != nil {
					t.Errorf("remove test image: %v\n%s", err, out)
				}
			}
		}
	})
	if !build {
		if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", image).CombinedOutput(); err != nil {
			t.Fatalf("prebuilt ORACACHE_CP_IMAGE %q required: %v\n%s", image, err, out)
		}
	}
	absent := func(services ...string) {
		t.Helper()
		present := strings.Fields(run(append(slices.Clone(profiles), "ps", "--all", "--services")...))
		for _, service := range services {
			if slices.Contains(present, service) {
				t.Fatalf("%s started before its stage; project services: %v", service, present)
			}
		}
	}
	port := func(service, internal string) string { return run("port", service, internal) }
	dial := func(endpoint string) *grpc.ClientConn {
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	getJSON := func(ctx context.Context, endpoint string, out any) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint, nil)
		if err != nil {
			return err
		}
		res, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: %s", endpoint, res.Status)
		}
		return json.NewDecoder(res.Body).Decode(out)
	}

	makeDemo("demo-up-a")
	absent(append([]string{"api-gateway-b"}, bNodes...)...)
	conn := dial(port("api-gateway", "28080"))
	cache := pb.NewCacheGatewayClient(conn)
	routes := "http://" + port("api-gateway", "9466") + "/routes"
	first, err := readCPRoutes(ctx, &http.Client{Timeout: time.Second}, routes)
	if err != nil {
		t.Fatalf("read write route after demo-up-a: %v", err)
	}
	aVoters := []string{"a-1", "a-2", "a-3"}
	if !slices.Contains(aVoters, first.Write[0].NodeID) {
		t.Fatalf("initial write route: %+v", first)
	}
	const key = "demo:geo"
	put := func(value string, expected uint64) *pb.PutResponse {
		t.Helper()
		rpc, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		out, err := cache.Put(rpc, &pb.PutRequest{Key: key, Data: []byte(value), ExpectedRev: proto.Uint64(expected)})
		if err != nil || out.GetStatus() != pb.PutStatus_PUT_STATUS_OK || out.GetRev() <= expected {
			t.Fatalf("Put %q (expected rev %d): response=%v error=%v", value, expected, out, err)
		}
		return out
	}
	read := func(ctx context.Context, cache pb.CacheGatewayClient, value string, rev uint64, node string) error {
		out, err := cache.Get(ctx, &pb.GetRequest{Key: key, Rev: rev})
		if err == nil && out.GetStatus() == pb.GetStatus_GET_STATUS_HIT && out.GetRev() == rev &&
			string(out.GetData()) == value && (node == "" || out.GetServedBy().GetInstance() == node) {
			return nil
		}
		return fmt.Errorf("Get: response=%v error=%v; want %q rev %d from %s", out, err, value, rev, node)
	}
	seed := put("before-b", 0)
	if seed.GetServedBy().GetInstance() != first.Write[0].NodeID {
		t.Fatalf("initial Put used %v, want %s", seed.GetServedBy(), first.Write[0].NodeID)
	}
	waitGeo(t, ctx, "seed through gateway", func(ctx context.Context) error {
		return read(ctx, cache, "before-b", seed.GetRev(), first.Write[0].NodeID)
	})

	prom := port("prometheus", "9090") + "/api/v1/"
	wantHealth := map[string]string{
		"cp-api:9464": "up", "api-gateway:9466": "up",
		"api-gateway-b:9466": "down", "a-4:28191": "down",
		"a-1:28191": "up", "a-2:28191": "up", "a-3:28191": "up",
		"b-1:28191": "down", "b-2:28191": "down", "b-3:28191": "down",
	}
	checkTargets := func() {
		t.Helper()
		waitGeo(t, ctx, fmt.Sprintf("Prometheus targets %v", wantHealth), func(ctx context.Context) error {
			var out struct {
				Status string
				Data   struct {
					ActiveTargets []struct {
						Labels map[string]string
						Health string
					}
				}
			}
			if err := getJSON(ctx, prom+"targets", &out); err != nil {
				return err
			}
			got := make(map[string]string)
			for _, target := range out.Data.ActiveTargets {
				got[target.Labels["instance"]] = target.Health
			}
			if out.Status != "success" || !maps.Equal(got, wantHealth) {
				return fmt.Errorf("status=%s targets=%v", out.Status, got)
			}
			return nil
		})
	}
	checkTargets()
	timelineStart := time.Now().Unix()
	grafana := port("grafana", "3000") + "/api/dashboards/uid/oracache-mvp"
	queries := make(map[int][]string)
	waitGeo(t, ctx, "MVP dashboard provisioned", func(ctx context.Context) error {
		var out struct {
			Meta      struct{ Provisioned bool }
			Dashboard struct {
				Panels []struct {
					ID      int
					Targets []struct{ Expr string }
				}
			}
		}
		if err := getJSON(ctx, grafana, &out); err != nil {
			return err
		}
		if !out.Meta.Provisioned {
			return fmt.Errorf("MVP dashboard is not provisioned")
		}
		clear(queries)
		for _, panel := range out.Dashboard.Panels {
			for _, target := range panel.Targets {
				queries[panel.ID] = append(queries[panel.ID], target.Expr)
			}
		}
		if len(queries[4]) != 1 || len(queries[11]) != 2 {
			return fmt.Errorf("missing role or outbox queries")
		}
		return nil
	})

	query := func(ctx context.Context, expr, label string) (map[string]string, error) {
		var out struct {
			Data struct {
				Result []struct {
					Metric map[string]string
					Value  []json.RawMessage
				}
			}
		}
		if err := getJSON(ctx, prom+"query?query="+url.QueryEscape(expr), &out); err != nil {
			return nil, err
		}
		got := make(map[string]string)
		for _, series := range out.Data.Result {
			var value string
			if len(series.Value) != 2 || json.Unmarshal(series.Value[1], &value) != nil {
				return nil, fmt.Errorf("unexpected sample: %+v", series)
			}
			key := series.Metric[label]
			if _, exists := got[key]; exists {
				return nil, fmt.Errorf("duplicate series for %s=%q", label, key)
			}
			got[key] = value
		}
		return got, nil
	}
	checkOutbox := func(expr, want string) {
		t.Helper()
		waitGeo(t, ctx, "outbox A → B = "+want, func(ctx context.Context) error {
			got, err := query(ctx, expr, "target_region")
			if err != nil {
				return err
			}
			if !maps.Equal(got, map[string]string{"region-b": want}) {
				return fmt.Errorf("got %v", got)
			}
			return nil
		})
	}
	checkOutbox(queries[11][0], "1")
	checkOutbox(queries[11][1], "10000")
	t.Logf("A accepted revision %d while B was absent", seed.GetRev())

	makeDemo("demo-up-b")
	routesB := "http://" + port("api-gateway-b", "9466") + "/routes"
	firstB, err := readCPRoutes(ctx, &http.Client{Timeout: time.Second}, routesB)
	if err != nil || !slices.Contains(bNodes, firstB.Read[0].NodeID) {
		t.Fatalf("local read route after demo-up-b: %+v, %v", firstB, err)
	}
	wantHealth["api-gateway-b:9466"] = "up"
	for _, node := range bNodes {
		wantHealth[node+":28191"] = "up"
	}
	checkTargets()
	cacheB := pb.NewCacheGatewayClient(dial(port("api-gateway-b", "28080")))
	verifyB := func(value string, rev uint64) {
		t.Helper()
		waitGeo(t, ctx, fmt.Sprintf("Gateway B serves %q at source revision %d", value, rev), func(ctx context.Context) error {
			out, err := cacheB.Get(ctx, &pb.GetRequest{Key: key, Rev: rev})
			if err != nil || out.GetStatus() != pb.GetStatus_GET_STATUS_HIT || out.GetRev() != rev ||
				string(out.GetData()) != value || !slices.Contains(bNodes, out.GetServedBy().GetInstance()) {
				return fmt.Errorf("Get through Gateway B: response=%v error=%v; want %q rev %d served by B", out, err, value, rev)
			}
			t.Logf("Gateway B confirmed revision %d served by %s", rev, out.GetServedBy().GetInstance())
			return nil
		})
	}
	verifyB("before-b", seed.GetRev())
	checkOutbox(queries[11][0], "0")
	waitGeo(t, ctx, "MVP role timeline shows one leader and two followers in each region", func(ctx context.Context) error {
		roles, err := query(ctx, queries[4][0], "instance")
		if err != nil {
			return err
		}
		if len(roles) != 7 || roles["a-4:28191"] != "-1" {
			return fmt.Errorf("roles: %v", roles)
		}
		for _, nodes := range [][]string{aVoters, bNodes} {
			leaders, followers := 0, 0
			for _, node := range nodes {
				switch roles[node+":28191"] {
				case "2":
					leaders++
				case "1":
					followers++
				}
			}
			if leaders != 1 || followers != 2 {
				return fmt.Errorf("roles: %v", roles)
			}
		}
		return nil
	})

	// A range query sees both pre-start and live states; they must share one series per node.
	var timeline struct {
		Data struct {
			Result []struct{ Metric map[string]string }
		}
	}
	timelineURL := fmt.Sprintf("%squery_range?query=%s&start=%d&end=%d&step=1", prom, url.QueryEscape(queries[4][0]), timelineStart, time.Now().Unix())
	if err := getJSON(ctx, timelineURL, &timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline.Data.Result) != 7 {
		t.Fatalf("MVP role timeline must have one series per node: %+v", timeline.Data.Result)
	}

	makeDemo("demo-up-learner")
	checkRole := func(node, want string) {
		t.Helper()
		waitGeo(t, ctx, node+" role "+want, func(ctx context.Context) error {
			got, err := query(ctx, queries[4][0], "instance")
			if err != nil {
				return err
			}
			if got[node+":28191"] != want {
				return fmt.Errorf("roles: %v", got)
			}
			return nil
		})
	}
	checkRole("a-4", "4")
	const group, region = "region-a/shard-0/replication-1", "region-a"
	admin := membership.NewMembershipAdminClient(conn)
	change := func(req *membership.ChangeMembershipRequest) uint64 {
		t.Helper()
		req.GroupId, req.Region = group, region
		var index uint64
		waitGeo(t, ctx, req.OperationId, func(ctx context.Context) error {
			out, err := admin.ChangeMembership(ctx, req)
			if err != nil {
				return err
			}
			index = out.GetChangeIndex()
			if index == 0 || out.GetOperationId() != req.OperationId {
				return fmt.Errorf("unexpected response: %v", out)
			}
			return nil
		})
		return index
	}
	added := change(&membership.ChangeMembershipRequest{OperationId: "add-a4", Action: "add-learner", Peer: &membership.Peer{Id: 4, NodeId: "a-4", Address: "a-4:28091", RaftAddress: "http://a-4:28191/raft"}})
	learnerStatus := port("a-4", "28191") + "/status"
	waitGeo(t, ctx, "a-4 applies add-learner and prior writes", func(ctx context.Context) error {
		var st struct {
			IsLearner, Member bool
			AppliedIndex      uint64
		}
		if err := getJSON(ctx, learnerStatus, &st); err != nil {
			return err
		}
		if !st.IsLearner || st.Member || st.AppliedIndex < added {
			return fmt.Errorf("learner status: %+v", st)
		}
		return nil
	})
	checkRole("a-4", "3")
	leader := waitCPRoute(t, ctx, routes, "").Write[0].NodeID
	peer := slices.Index(aVoters, leader) + 1
	if peer == 0 {
		t.Fatalf("unknown A leader %q", leader)
	}
	run("kill", "-s", "SIGKILL", leader)
	successor := waitCPRoute(t, ctx, routes, leader).Write[0].NodeID
	waitGeo(t, ctx, "successor serves the original revision", func(ctx context.Context) error {
		return read(ctx, cache, "before-b", seed.GetRev(), successor)
	})
	afterKill := put("after-kill", seed.GetRev())
	if afterKill.GetServedBy().GetInstance() != successor {
		t.Fatalf("post-crash write: %v", afterKill)
	}
	verifyB("after-kill", afterKill.GetRev())
	t.Logf("killed %s; successor %s committed revision %d", leader, successor, afterKill.GetRev())
	change(&membership.ChangeMembershipRequest{OperationId: "replace-dead-with-a4", Action: "replace", PeerId: 4, RemoveId: uint64(peer)})
	waitGeo(t, ctx, "replacement finalizes with a-4 as a voter", func(ctx context.Context) error {
		m, err := admin.GetMembership(ctx, &membership.GetMembershipRequest{GroupId: group, Region: region})
		if err != nil {
			return err
		}
		if !m.GetJoint() && len(m.GetMembers()) == 3 && slices.Contains(m.GetRemovedIds(), uint64(peer)) {
			for _, p := range m.GetMembers() {
				if p.GetPeer().GetId() == 4 && p.GetVoter() && !p.GetLearner() {
					return nil
				}
			}
		}
		return fmt.Errorf("membership: %v", m)
	})
	checkRole("a-4", "1")
	checkRole(leader, "-1")
	afterReplace := put("after-replace", afterKill.GetRev())
	verifyB("after-replace", afterReplace.GetRev())
	checkOutbox(queries[11][0], "0")
	t.Logf("a-4 replaced %s; revision %d reached B", leader, afterReplace.GetRev())

}

func waitGeo(t *testing.T, ctx context.Context, description string, probe func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := probe(attempt)
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() == nil {
			last = err
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	t.Fatalf("waiting for %s: %v (last probe: %v)", description, ctx.Err(), last)
}
