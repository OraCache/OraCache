package cpapi_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/statestore"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testServer(t *testing.T, now time.Time) (*cpapi.Server, *statestore.MemoryStore) {
	t.Helper()
	store := statestore.NewMemory()
	return cpapi.New(store, cpapi.WithClock(fixedClock{now: now})), store
}

func validHeartbeat() *controlplanev1.HeartbeatRequest {
	return &controlplanev1.HeartbeatRequest{
		NodeId:   "eu-1",
		Region:   "eu",
		Instance: "node-eu-1",
		SentAt:   timestamppb.New(time.Unix(100, 0).UTC()),
		Partitions: []*controlplanev1.PartitionStatus{
			{
				PartitionId:  0,
				Role:         controlplanev1.RaftRole_RAFT_ROLE_LEADER,
				Term:         7,
				CommitIndex:  10,
				AppliedIndex: 10,
				LeaderId:     "eu-1",
				Ready:        true,
			},
			{
				PartitionId: 1,
				Role:        controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER,
				Term:        7,
				LeaderId:    "eu-2",
				Ready:       true,
			},
		},
		Resources: &controlplanev1.NodeResources{CpuMillis: 4000, MemoryBytes: 1 << 30, DiskFreeBytes: 1 << 40},
	}
}

func TestHeartbeatAgeMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &mutableClock{now: time.Unix(1000, 0)}
	store := statestore.NewMemory()
	srv := cpapi.New(store, cpapi.WithClock(clock), cpapi.WithNodeTTL(10*time.Second))
	handler := srv.Metrics().Handler()
	assertAge := func(want ...float64) {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("metrics status = %d, body = %s", recorder.Code, recorder.Body)
		}
		parser := expfmt.NewTextParser(model.LegacyValidation)
		families, err := parser.TextToMetricFamilies(recorder.Body)
		if err != nil {
			t.Fatalf("parse metrics: %v", err)
		}
		samples := families["oracache_cp_heartbeat_age_seconds"].GetMetric()
		if len(samples) != len(want) {
			t.Fatalf("heartbeat age samples = %v, want %d sample(s)", samples, len(want))
		}
		for i, sample := range samples {
			labels := make(map[string]string)
			for _, pair := range sample.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if len(labels) != 2 || labels["node_id"] != "eu-extra" || labels["region"] != "eu" ||
				sample.GetGauge() == nil || sample.GetGauge().GetValue() != want[i] {
				t.Fatalf("heartbeat age sample = %v, want eu/eu-extra age %g", sample, want[i])
			}
		}
	}
	beat := func(nodeID, region string) {
		t.Helper()
		req := validHeartbeat()
		req.NodeId, req.Region = nodeID, region
		if _, err := srv.ReportHeartbeat(ctx, req); err != nil {
			t.Fatalf("ReportHeartbeat: %v", err)
		}
	}
	cfg := cpconfigtest.Config(1)
	cfg.Nodes = append(cfg.Nodes, cpconfig.Node{ID: "eu-extra", Region: "eu", Address: "10.0.0.3:7000"})
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	assertAge()
	beat("unknown", "eu")
	beat("eu-extra", "us")
	assertAge()
	beat("eu-extra", "eu")
	assertAge(0)
	clock.Advance(1500 * time.Millisecond)
	assertAge(1.5)
	clock.Advance(10 * time.Second)
	beat("eu-extra", "us")
	cfg.Revision = 2
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	assertAge(11.5)
	beat("eu-extra", "eu")
	clock.Advance(250 * time.Millisecond)
	assertAge(0.25)
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(3)); err != nil {
		t.Fatal(err)
	}
	assertAge()
}

func TestReportHeartbeatUsesServerReceiptTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	srv, store := testServer(t, now)
	req := validHeartbeat()
	// Часы клиента, ушедшие далеко в будущее, не должны влиять на liveness.
	req.SentAt = timestamppb.New(now.Add(24 * time.Hour))

	resp, err := srv.ReportHeartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatal("heartbeat not accepted")
	}
	if resp.GetConfigRevision() != 0 {
		t.Fatalf("config revision = %d, want 0", resp.GetConfigRevision())
	}
	if want := uint32(cpapi.DefaultHeartbeatInterval / time.Millisecond); resp.GetNextHeartbeatAfterMs() != want {
		t.Fatalf("next heartbeat = %d, want %d", resp.GetNextHeartbeatAfterMs(), want)
	}

	hb, ok, err := store.Heartbeat(context.Background(), "eu-1")
	if err != nil || !ok {
		t.Fatalf("stored heartbeat = %+v, %v, %v", hb, ok, err)
	}
	if !hb.ReceivedAt.Equal(now) {
		t.Fatalf("ReceivedAt = %v, want %v", hb.ReceivedAt, now)
	}
	if !hb.SentAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("SentAt = %v, want diagnostics value", hb.SentAt)
	}
	if len(hb.Partitions) != 2 || hb.Partitions[0].Role != statestore.RaftRoleLeader {
		t.Fatalf("partitions = %+v", hb.Partitions)
	}
	if hb.Resources.MemoryBytes != 1<<30 {
		t.Fatalf("resources = %+v", hb.Resources)
	}
}

func TestReportHeartbeatUsesConfiguredInterval(t *testing.T) {
	t.Parallel()
	store := statestore.NewMemory()
	srv := cpapi.New(store, cpapi.WithClock(fixedClock{now: time.Unix(1000, 0)}), cpapi.WithHeartbeatInterval(25*time.Millisecond))
	resp, err := srv.ReportHeartbeat(context.Background(), validHeartbeat())
	if err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	if resp.GetNextHeartbeatAfterMs() != 25 {
		t.Fatalf("next heartbeat = %d, want 25", resp.GetNextHeartbeatAfterMs())
	}
}

func TestReportHeartbeatReturnsCurrentRevision(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0).UTC()
	srv, store := testServer(t, now)
	if err := store.ApplyConfig(context.Background(), cpconfigtest.Config(5)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	resp, err := srv.ReportHeartbeat(context.Background(), validHeartbeat())
	if err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	if resp.GetConfigRevision() != 5 {
		t.Fatalf("config revision = %d, want 5", resp.GetConfigRevision())
	}
}

func TestReportHeartbeatValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*controlplanev1.HeartbeatRequest)
	}{
		{name: "nil", mutate: nil},
		{name: "empty_node", mutate: func(r *controlplanev1.HeartbeatRequest) { r.NodeId = "" }},
		{name: "long_node", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.NodeId = string(make([]byte, cpapi.MaxIDBytes+1))
		}},
		{name: "empty_region", mutate: func(r *controlplanev1.HeartbeatRequest) { r.Region = "" }},
		{name: "long_instance", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Instance = string(make([]byte, cpapi.MaxIDBytes+1))
		}},
		{name: "nil_partition", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions = append(r.Partitions, nil)
		}},
		{name: "partition_out_of_range", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions[0].PartitionId = cpconfig.DefaultPartitionCount
		}},
		{name: "duplicate_partition", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions[1].PartitionId = r.Partitions[0].PartitionId
		}},
		{name: "unknown_role", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions[0].Role = controlplanev1.RaftRole(99)
		}},
		{name: "long_last_error", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions[0].LastError = string(make([]byte, cpapi.MaxLastErrorBytes+1))
		}},
		{name: "too_many_partitions", mutate: func(r *controlplanev1.HeartbeatRequest) {
			r.Partitions = make([]*controlplanev1.PartitionStatus, cpconfig.DefaultPartitionCount+1)
			for i := range r.Partitions {
				r.Partitions[i] = &controlplanev1.PartitionStatus{PartitionId: uint32(i)}
			}
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := testServer(t, time.Unix(1000, 0))
			req := validHeartbeat()
			if tt.mutate == nil {
				req = nil
			} else {
				tt.mutate(req)
			}
			_, err := srv.ReportHeartbeat(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestGetConfigValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  *controlplanev1.GetConfigRequest
		code codes.Code
	}{
		{name: "nil", req: nil, code: codes.InvalidArgument},
		{name: "empty_node", req: &controlplanev1.GetConfigRequest{Kind: controlplanev1.AgentKind_AGENT_KIND_GATEWAY}, code: codes.InvalidArgument},
		{name: "missing_kind", req: &controlplanev1.GetConfigRequest{NodeId: "gw-1"}, code: codes.InvalidArgument},
		{name: "unknown_kind", req: &controlplanev1.GetConfigRequest{NodeId: "gw-1", Kind: controlplanev1.AgentKind(99)}, code: codes.InvalidArgument},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := testServer(t, time.Unix(1000, 0))
			_, err := srv.GetConfig(context.Background(), tt.req)
			if status.Code(err) != tt.code {
				t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tt.code, err)
			}
		})
	}
}

func TestGetConfigEmptyStore(t *testing.T) {
	t.Parallel()
	srv, _ := testServer(t, time.Unix(1000, 0))
	req := &controlplanev1.GetConfigRequest{
		NodeId:        "gw-1",
		KnownRevision: 0,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	}
	resp, err := srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if resp.GetRevision() != 0 || resp.GetConfig() != nil {
		t.Fatalf("response = %+v, want empty revision 0", resp)
	}

	req.KnownRevision = 1
	if _, err := srv.GetConfig(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("known-ahead code = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestGetConfigGatewayReturnsLiveViewAtSameRevision(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0).UTC()
	clock := &mutableClock{now: now.Add(-cpapi.DefaultNodeTTL)}
	store := statestore.NewMemory()
	srv := cpapi.New(store, cpapi.WithClock(clock))
	clock.Advance(cpapi.DefaultNodeTTL)
	if err := store.ApplyConfig(context.Background(), cpconfigtest.Config(3)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	if _, err := store.UpsertHeartbeat(context.Background(), statestore.Heartbeat{
		NodeID: "eu-1", Region: "eu", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 0, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
			{PartitionID: 2, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}

	resp, err := srv.GetConfig(context.Background(), &controlplanev1.GetConfigRequest{
		NodeId:        "gw-eu-1",
		KnownRevision: 3,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	})
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if resp.GetRevision() != 3 || resp.GetConfig() == nil {
		t.Fatalf("response = %+v, want full live view", resp)
	}
	states := make(map[string]string)
	for _, node := range resp.GetConfig().GetNodes() {
		states[node.GetNodeId()] = node.GetState()
	}
	if states["eu-1"] != cpconfig.NodeStateReady {
		t.Errorf("eu-1 state = %q, want ready", states["eu-1"])
	}
	if states["eu-2"] != cpconfig.NodeStateDead {
		t.Errorf("eu-2 state = %q, want dead", states["eu-2"])
	}
	if states["us-1"] != cpconfig.NodeStateDead {
		t.Errorf("us-1 state = %q, want dead", states["us-1"])
	}
	var euPartition *controlplanev1.PartitionAssignment
	for _, assignment := range resp.GetConfig().GetPartitions() {
		if assignment.GetRegion() == "eu" && assignment.GetPartitionId() == 0 {
			euPartition = assignment
		}
	}
	if euPartition == nil || !euPartition.GetReady() || !euPartition.GetReadable() || !euPartition.GetWritable() {
		t.Fatalf("eu/0 = %+v, want ready/readable/writable", euPartition)
	}
}

func TestGetConfigGatewayWaitsForInitialReplicaHeartbeatsAtKnownRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	cfg := cpconfigtest.Config(3)
	store := statestore.NewMemory()
	srv := cpapi.New(store, cpapi.WithClock(fixedClock{now: now}))
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	for _, node := range cfg.Nodes[:len(cfg.Nodes)-1] {
		if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
			NodeID: node.ID, Region: node.Region, ReceivedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	request := &controlplanev1.GetConfigRequest{
		NodeId: "gateway", Kind: controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
		KnownRevision: cfg.Revision,
	}
	resp, err := srv.GetConfig(ctx, request)
	if err != nil || resp.GetRevision() != cfg.Revision || resp.GetConfig() != nil {
		t.Fatalf("partial observations: GetConfig = %v, %v, want revision only", resp, err)
	}
	request.KnownRevision--
	resp, err = srv.GetConfig(ctx, request)
	if err != nil || resp.GetRevision() != cfg.Revision || resp.GetConfig() == nil {
		t.Fatalf("new revision: GetConfig = %v, %v, want config", resp, err)
	}
	node := cfg.Nodes[len(cfg.Nodes)-1]
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{
		NodeID: node.ID, Region: node.Region, ReceivedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	request.KnownRevision = cfg.Revision
	resp, err = srv.GetConfig(ctx, request)
	if err != nil || resp.GetConfig() == nil {
		t.Fatalf("initial observations complete: GetConfig = %v, %v, want live view", resp, err)
	}
}

func TestGetConfigGatewayExpiresStaleNode(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0).UTC()
	clock := &mutableClock{now: now}
	store := statestore.NewMemory()
	srv := cpapi.New(store, cpapi.WithClock(clock), cpapi.WithNodeTTL(10*time.Second))
	if err := store.ApplyConfig(context.Background(), cpconfigtest.Config(3)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	if _, err := store.UpsertHeartbeat(context.Background(), statestore.Heartbeat{
		NodeID: "eu-1", Region: "eu", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{
			{PartitionID: 0, Role: statestore.RaftRoleLeader, LeaderID: "eu-1", Ready: true},
		},
	}); err != nil {
		t.Fatalf("UpsertHeartbeat: %v", err)
	}
	req := &controlplanev1.GetConfigRequest{
		NodeId: "gw-eu-1",
		Kind:   controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	}
	resp, err := srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	state := ""
	for _, node := range resp.GetConfig().GetNodes() {
		if node.GetNodeId() == "eu-1" {
			state = node.GetState()
		}
	}
	if state != cpconfig.NodeStateReady {
		t.Fatalf("eu-1 state = %q, want ready", state)
	}

	req.KnownRevision = resp.GetRevision()
	clock.Advance(11 * time.Second)
	resp, err = srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig after TTL: %v", err)
	}
	state = ""
	for _, node := range resp.GetConfig().GetNodes() {
		if node.GetNodeId() == "eu-1" {
			state = node.GetState()
		}
	}
	if state != cpconfig.NodeStateDead {
		t.Fatalf("eu-1 state = %q, want dead", state)
	}
}

func TestGetConfigServesFullSnapshotAndRevisionOnly(t *testing.T) {
	t.Parallel()
	srv, store := testServer(t, time.Unix(1000, 0))
	cfg := cpconfigtest.Config(3)
	if err := store.ApplyConfig(context.Background(), cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	req := &controlplanev1.GetConfigRequest{
		NodeId:        "gw-eu-1",
		KnownRevision: 0,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	}
	resp, err := srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if resp.GetRevision() != 3 || resp.GetConfig() == nil {
		t.Fatalf("response = %+v", resp)
	}
	if resp.GetConfig().GetRevision() != 3 || resp.GetConfig().GetPartitionCount() != cpconfigtest.PartitionCount {
		t.Fatalf("config = %+v", resp.GetConfig())
	}
	if len(resp.GetConfig().GetPartitions()) != int(cpconfigtest.PartitionCount)*2 {
		t.Fatalf("assignments = %d", len(resp.GetConfig().GetPartitions()))
	}
	// У одной партиции независимые лидеры в eu и us.
	var euLeader, usLeader string
	for _, a := range resp.GetConfig().GetPartitions() {
		if a.GetPartitionId() != 0 {
			continue
		}
		switch a.GetRegion() {
		case "eu":
			euLeader = a.GetLeaderNodeId()
		case "us":
			usLeader = a.GetLeaderNodeId()
		}
	}
	if euLeader == "" || usLeader == "" || euLeader == usLeader {
		t.Fatalf("region-scoped leaders = eu %q, us %q", euLeader, usLeader)
	}

	// Revision совпадает у Storage Node: тело не передаётся, клиент сохраняет
	// last-known-good. Gateway в этом случае получает live-view.
	req.KnownRevision = 3
	req.Kind = controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE
	resp, err = srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig(equal): %v", err)
	}
	if resp.GetRevision() != 3 || resp.GetConfig() != nil {
		t.Fatalf("equal revision response = %+v", resp)
	}

	// Более старая известная revision получает полный snapshot.
	req.KnownRevision = 2
	req.Kind = controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE
	resp, err = srv.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("GetConfig(older): %v", err)
	}
	if resp.GetConfig() == nil {
		t.Fatal("older revision got no config")
	}

	// Клиент впереди CP — это ошибка конфигурации, а не устаревшее тело.
	req.KnownRevision = 4
	if _, err := srv.GetConfig(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("known-ahead code = %v, want FailedPrecondition", status.Code(err))
	}
}

type failingStore struct{ err error }

func (s failingStore) UpsertHeartbeat(context.Context, statestore.Heartbeat) (uint64, error) {
	return 0, s.err
}
func (s failingStore) Heartbeat(context.Context, string) (statestore.Heartbeat, bool, error) {
	return statestore.Heartbeat{}, false, s.err
}
func (s failingStore) HeartbeatReceipts(context.Context) ([]statestore.HeartbeatReceipt, error) {
	return nil, s.err
}
func (s failingStore) Config(context.Context) (statestore.Snapshot, error) {
	return statestore.Snapshot{}, s.err
}
func (s failingStore) LiveConfig(context.Context, time.Time, time.Duration) (statestore.Snapshot, error) {
	return statestore.Snapshot{}, s.err
}
func (s failingStore) ApplyConfig(context.Context, *cpconfig.Config) error { return s.err }

func TestStoreErrorsMapToUnavailable(t *testing.T) {
	t.Parallel()
	srv := cpapi.New(failingStore{err: errors.New("etcd down")}, cpapi.WithClock(fixedClock{now: time.Unix(1000, 0)}))
	if _, err := srv.ReportHeartbeat(context.Background(), validHeartbeat()); status.Code(err) != codes.Unavailable {
		t.Fatalf("heartbeat code = %v, want Unavailable", status.Code(err))
	}
	req := &controlplanev1.GetConfigRequest{NodeId: "gw-1", Kind: controlplanev1.AgentKind_AGENT_KIND_GATEWAY}
	if _, err := srv.GetConfig(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("config code = %v, want Unavailable", status.Code(err))
	}
}

func TestGRPCContractOverBufconn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	srv := cpapi.New(store, cpapi.WithClock(fixedClock{now: time.Unix(2000, 0)}))

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	controlplanev1.RegisterControlPlaneAPIServer(grpcServer, srv)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("grpc server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := controlplanev1.NewControlPlaneAPIClient(conn)
	if _, err := client.ReportHeartbeat(ctx, validHeartbeat()); err != nil {
		t.Fatalf("ReportHeartbeat: %v", err)
	}
	resp, err := client.GetConfig(ctx, &controlplanev1.GetConfigRequest{
		NodeId:        "gw-eu-1",
		KnownRevision: 0,
		Kind:          controlplanev1.AgentKind_AGENT_KIND_GATEWAY,
	})
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if resp.GetRevision() != 1 || resp.GetConfig() == nil {
		t.Fatalf("response = %+v", resp)
	}
}

func TestCPRPCDurationsIncludeRejectedCalls(t *testing.T) {
	t.Parallel()
	srv := cpapi.New(statestore.NewMemory())
	_, _ = srv.ReportHeartbeat(t.Context(), nil)
	_, _ = srv.GetConfig(t.Context(), nil)
	recorder := httptest.NewRecorder()
	srv.Metrics().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(recorder.Body)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"report_heartbeat": true, "get_config": true}
	for _, metric := range families["oracache_cp_rpc_duration_seconds"].GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "operation" && metric.GetHistogram().GetSampleCount() == 1 {
				delete(want, label.GetValue())
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing CP histogram samples: %v", want)
	}
}
