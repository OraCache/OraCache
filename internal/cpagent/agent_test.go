package cpagent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	delays []time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	c.delays = append(c.delays, d)
	c.mu.Unlock()
	return &fakeTimer{ch: make(chan time.Time)}
}

func (c *fakeClock) timerDelays() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.delays...)
}

type fakeTimer struct{ ch chan time.Time }

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool          { return true }

type fakeClient struct {
	mu sync.Mutex

	heartbeatFn func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error)
	configFn    func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error)

	heartbeats []*controlplanev1.HeartbeatRequest
	pulls      []*controlplanev1.GetConfigRequest
}

func (c *fakeClient) ReportHeartbeat(ctx context.Context, req *controlplanev1.HeartbeatRequest, _ ...grpc.CallOption) (*controlplanev1.HeartbeatResponse, error) {
	c.mu.Lock()
	c.heartbeats = append(c.heartbeats, req)
	c.mu.Unlock()
	if c.heartbeatFn == nil {
		return &controlplanev1.HeartbeatResponse{Accepted: true}, nil
	}
	return c.heartbeatFn(ctx, req)
}

func (c *fakeClient) GetConfig(ctx context.Context, req *controlplanev1.GetConfigRequest, _ ...grpc.CallOption) (*controlplanev1.GetConfigResponse, error) {
	c.mu.Lock()
	c.pulls = append(c.pulls, req)
	c.mu.Unlock()
	if c.configFn == nil {
		return &controlplanev1.GetConfigResponse{Revision: req.GetKnownRevision()}, nil
	}
	return c.configFn(ctx, req)
}

func (c *fakeClient) lastHeartbeat() *controlplanev1.HeartbeatRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.heartbeats) == 0 {
		return nil
	}
	return c.heartbeats[len(c.heartbeats)-1]
}

func (c *fakeClient) lastPull() *controlplanev1.GetConfigRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pulls) == 0 {
		return nil
	}
	return c.pulls[len(c.pulls)-1]
}

func (c *fakeClient) callCounts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.heartbeats), len(c.pulls)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testStatuses(_ context.Context) ([]*controlplanev1.PartitionStatus, error) {
	return []*controlplanev1.PartitionStatus{
		{
			PartitionId:  0,
			Role:         controlplanev1.RaftRole_RAFT_ROLE_LEADER,
			Term:         3,
			CommitIndex:  8,
			AppliedIndex: 8,
			LeaderId:     "eu-1",
			Ready:        true,
		},
	}, nil
}

func newTestAgent(t *testing.T, client *fakeClient, holder *cpconfig.Holder) (*Agent, *fakeClock) {
	t.Helper()
	if client == nil {
		client = &fakeClient{}
	}
	if holder == nil {
		holder = cpconfig.NewHolder()
	}
	clock := &fakeClock{now: time.Unix(1000, 0).UTC()}
	agent, err := New(Options{
		NodeID:             "eu-1",
		Region:             "eu",
		Instance:           "node-eu-1",
		Client:             client,
		Statuses:           StatusProviderFunc(testStatuses),
		Holder:             holder,
		Clock:              clock,
		Logger:             discardLogger(),
		DisableJitter:      true,
		HeartbeatInterval:  time.Hour,
		ConfigPullInterval: time.Hour,
		RPCTimeout:         time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return agent, clock
}

func TestNewValidationAndDefaults(t *testing.T) {
	t.Parallel()
	base := Options{
		NodeID:   "eu-1",
		Region:   "eu",
		Client:   &fakeClient{},
		Statuses: StatusProviderFunc(testStatuses),
	}
	if _, err := New(Options{}); err == nil {
		t.Fatal("New accepted empty options")
	}
	for name, mutate := range map[string]func(*Options){
		"missing_node":     func(o *Options) { o.NodeID = "" },
		"missing_region":   func(o *Options) { o.Region = "" },
		"missing_client":   func(o *Options) { o.Client = nil },
		"missing_statuses": func(o *Options) { o.Statuses = nil },
		"bad_jitter":       func(o *Options) { o.JitterFraction = 1.5 },
	} {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := base
			mutate(&opts)
			if _, err := New(opts); err == nil {
				t.Fatal("New accepted invalid options")
			}
		})
	}

	agent, err := New(base)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if agent.Config() != nil || agent.Revision() != 0 {
		t.Fatalf("empty agent config = %+v revision %d", agent.Config(), agent.Revision())
	}
	if agent.jitterFraction != DefaultJitterFraction {
		t.Fatalf("default jitter = %v, want %v", agent.jitterFraction, DefaultJitterFraction)
	}
	if err := agent.Run(nil); err == nil {
		t.Fatal("Run(nil) succeeded")
	}
}

func TestSendHeartbeatContract(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	if err := holder.Apply(cpconfigtest.Config(2)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	client := &fakeClient{heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
		return &controlplanev1.HeartbeatResponse{
			Accepted:             true,
			ConfigRevision:       2,
			NextHeartbeatAfterMs: 7000,
		}, nil
	}}
	agent, clock := newTestAgent(t, client, holder)
	next, err := agent.sendHeartbeat(context.Background())
	if err != nil {
		t.Fatalf("sendHeartbeat: %v", err)
	}
	if next != 7*time.Second {
		t.Fatalf("next interval = %v, want 7s", next)
	}
	req := client.lastHeartbeat()
	if req.GetNodeId() != "eu-1" || req.GetRegion() != "eu" || req.GetInstance() != "node-eu-1" {
		t.Fatalf("heartbeat identity = %+v", req)
	}
	if req.GetConfigRevision() != 2 {
		t.Fatalf("heartbeat config revision = %d, want 2", req.GetConfigRevision())
	}
	if !req.GetSentAt().AsTime().Equal(clock.Now()) {
		t.Fatalf("sent_at = %v, want %v", req.GetSentAt().AsTime(), clock.Now())
	}
	if len(req.GetPartitions()) != 1 || req.GetPartitions()[0].GetRole() != controlplanev1.RaftRole_RAFT_ROLE_LEADER {
		t.Fatalf("partitions = %+v", req.GetPartitions())
	}
	select {
	case <-agent.pullNow:
		t.Fatal("equal revision must not trigger an immediate pull")
	default:
	}
}

func TestHeartbeatTriggersPullWhenConfigRevisionIsNewer(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	if err := holder.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	client := &fakeClient{heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
		return &controlplanev1.HeartbeatResponse{Accepted: true, ConfigRevision: 2}, nil
	}}
	agent, _ := newTestAgent(t, client, holder)
	if _, err := agent.sendHeartbeat(context.Background()); err != nil {
		t.Fatalf("sendHeartbeat: %v", err)
	}
	select {
	case <-agent.pullNow:
	default:
		t.Fatal("newer heartbeat revision did not trigger an immediate pull")
	}
}

func TestHeartbeatFailures(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	tests := []struct {
		name     string
		statuses StatusProvider
		client   *fakeClient
	}{
		{name: "status_error", statuses: StatusProviderFunc(func(context.Context) ([]*controlplanev1.PartitionStatus, error) {
			return nil, errors.New("raft unavailable")
		}), client: &fakeClient{}},
		{name: "rpc_error", statuses: StatusProviderFunc(testStatuses), client: &fakeClient{heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
			return nil, errors.New("cp unavailable")
		}}},

		{name: "nil_response", statuses: StatusProviderFunc(testStatuses), client: &fakeClient{heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
			return nil, nil
		}}},
		{name: "rejected", statuses: StatusProviderFunc(testStatuses), client: &fakeClient{heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
			return &controlplanev1.HeartbeatResponse{Accepted: false}, nil
		}}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{now: time.Unix(1000, 0)}
			agent, err := New(Options{
				NodeID: "eu-1", Region: "eu",
				Client:   tt.client,
				Statuses: tt.statuses,
				Holder:   holder,
				Clock:    clock,
				Logger:   discardLogger(),
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if _, err := agent.sendHeartbeat(context.Background()); err == nil {
				t.Fatal("sendHeartbeat succeeded, want error")
			}
		})
	}
}

func TestPullConfigAppliesNewRevision(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	if err := holder.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	client := &fakeClient{configFn: func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if req.GetKnownRevision() != 1 {
			t.Fatalf("known revision = %d, want 1", req.GetKnownRevision())
		}
		return &controlplanev1.GetConfigResponse{Revision: 2, Config: cpconfigtest.Config(2).ToProto()}, nil
	}}
	agent, _ := newTestAgent(t, client, holder)
	if err := agent.pullConfig(context.Background()); err != nil {
		t.Fatalf("pullConfig: %v", err)
	}
	if agent.Revision() != 2 {
		t.Fatalf("revision = %d, want 2", agent.Revision())
	}
	req := client.lastPull()
	if req.GetNodeId() != "eu-1" || req.GetKind() != controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE {
		t.Fatalf("pull request = %+v", req)
	}
}

func TestPullConfigErrorsKeepLastKnownGood(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		response *controlplanev1.GetConfigResponse
		err      error
	}{
		{name: "client_error", err: errors.New("unavailable")},
		{name: "nil_response"},
		{name: "stale_response", response: &controlplanev1.GetConfigResponse{Revision: 0}},
		{name: "new_revision_without_config", response: &controlplanev1.GetConfigResponse{Revision: 2}},
		{name: "revision_mismatch", response: func() *controlplanev1.GetConfigResponse {
			r := &controlplanev1.GetConfigResponse{Revision: 2, Config: cpconfigtest.Config(2).ToProto()}
			r.Config.Revision = 1
			return r
		}()},
		{name: "invalid_config", response: func() *controlplanev1.GetConfigResponse {
			cfg := cpconfigtest.Config(2)
			cfg.Regions[0].Role = cpconfig.RegionRoleUnspecified
			return &controlplanev1.GetConfigResponse{Revision: 2, Config: cfg.ToProto()}
		}()},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			holder := cpconfig.NewHolder()
			if err := holder.Apply(cpconfigtest.Config(1)); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			client := &fakeClient{configFn: func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
				return tt.response, tt.err
			}}
			agent, _ := newTestAgent(t, client, holder)
			if err := agent.pullConfig(context.Background()); err == nil {
				t.Fatal("pullConfig succeeded, want error")
			}
			if agent.Revision() != 1 {
				t.Fatalf("last-known-good changed to %d", agent.Revision())
			}
		})
	}
}

func TestPullConfigIgnoresEqualRevision(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	if err := holder.Apply(cpconfigtest.Config(3)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	client := &fakeClient{configFn: func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		return &controlplanev1.GetConfigResponse{Revision: 3}, nil
	}}
	agent, _ := newTestAgent(t, client, holder)
	if err := agent.pullConfig(context.Background()); err != nil {
		t.Fatalf("pullConfig: %v", err)
	}
	if agent.Revision() != 3 {
		t.Fatalf("revision = %d, want 3", agent.Revision())
	}
}

func TestRunInitialLoopsAndShutdown(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	client := &fakeClient{
		heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
			return &controlplanev1.HeartbeatResponse{Accepted: true, ConfigRevision: 1}, nil
		},
		configFn: func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
			if req.GetKnownRevision() == 0 {
				return &controlplanev1.GetConfigResponse{Revision: 1, Config: cpconfigtest.Config(1).ToProto()}, nil
			}
			return &controlplanev1.GetConfigResponse{Revision: 1}, nil
		},
	}
	agent, _ := newTestAgent(t, client, holder)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()

	waitFor(t, 2*time.Second, func() bool {
		heartbeats, pulls := client.callCounts()
		return heartbeats > 0 && pulls > 0 && agent.Revision() == 1
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunPullsImmediatelyAfterNewRevisionHint(t *testing.T) {
	t.Parallel()
	holder := cpconfig.NewHolder()
	client := &fakeClient{
		heartbeatFn: func(context.Context, *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
			return &controlplanev1.HeartbeatResponse{Accepted: true, ConfigRevision: 2}, nil
		},
		configFn: func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
			switch req.GetKnownRevision() {
			case 0:
				return &controlplanev1.GetConfigResponse{Revision: 1, Config: cpconfigtest.Config(1).ToProto()}, nil
			case 1:
				return &controlplanev1.GetConfigResponse{Revision: 2, Config: cpconfigtest.Config(2).ToProto()}, nil
			default:
				return &controlplanev1.GetConfigResponse{Revision: req.GetKnownRevision()}, nil
			}
		},
	}
	agent, _ := newTestAgent(t, client, holder)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return agent.Revision() == 2 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()
	base := 5 * time.Second
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: -1, want: base},
		{failures: 0, want: base},
		{failures: 1, want: base},
		{failures: 2, want: 10 * time.Second},
		{failures: 3, want: 20 * time.Second},
		{failures: 6, want: maxBackoff},
		{failures: 100, want: maxBackoff},
	}
	for _, tt := range tests {
		if got := backoffDelay(base, tt.failures); got != tt.want {
			t.Errorf("backoffDelay(%v, %d) = %v, want %v", base, tt.failures, got, tt.want)
		}
	}
}

func TestJitterDelay(t *testing.T) {
	t.Parallel()
	base := 10 * time.Second
	for _, seed := range []string{"eu-1", "eu-2", "us-1"} {
		got := jitterDelay(base, 0.10, seed)
		if got < 9*time.Second || got > 11*time.Second {
			t.Errorf("jitterDelay(%q) = %v, want within +/-10%%", seed, got)
		}
		if again := jitterDelay(base, 0.10, seed); again != got {
			t.Errorf("jitterDelay(%q) is not deterministic: %v != %v", seed, got, again)
		}
	}
	if got := jitterDelay(base, 0, "eu-1"); got != base {
		t.Errorf("zero jitter = %v, want %v", got, base)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("condition was not met before deadline")
		}
	}
}
