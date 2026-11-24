package gateway_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
)

type configClientFunc func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error)

func (f configClientFunc) GetConfig(ctx context.Context, in *controlplanev1.GetConfigRequest, _ ...grpc.CallOption) (*controlplanev1.GetConfigResponse, error) {
	return f(ctx, in)
}

func newRouter(t *testing.T) *gateway.Router {
	t.Helper()
	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return router
}

func fullResponse(revision uint64) *controlplanev1.GetConfigResponse {
	return &controlplanev1.GetConfigResponse{
		Revision: revision,
		Config:   cpconfigtest.Config(revision).ToProto(),
	}
}

func TestPollerAppliesFullSnapshotAndSendsContractFields(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	var gotReq *controlplanev1.GetConfigRequest
	client := configClientFunc(func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		gotReq = req
		return fullResponse(4), nil
	})
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	if err := poller.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if router.Revision() != 4 {
		t.Fatalf("revision = %d, want 4", router.Revision())
	}
	if gotReq.GetNodeId() != "gw-eu-1" || gotReq.GetKnownRevision() != 0 ||
		gotReq.GetKind() != controlplanev1.AgentKind_AGENT_KIND_GATEWAY {
		t.Fatalf("request = %+v", gotReq)
	}
	leader, err := router.GetLeaderForKey("key")
	if err != nil || leader.Region != "eu" {
		t.Fatalf("route after refresh = %+v, %v", leader, err)
	}
}

func TestPollerKeepsLastKnownGoodOnEqualRevision(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	if err := router.Apply(cpconfigtest.Config(3)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	client := configClientFunc(func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if req.GetKnownRevision() != 3 {
			t.Fatalf("known revision = %d, want 3", req.GetKnownRevision())
		}
		return &controlplanev1.GetConfigResponse{Revision: 3}, nil
	})
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	if err := poller.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if router.Revision() != 3 {
		t.Fatalf("revision = %d, want 3", router.Revision())
	}
}

func TestPollerErrorsPreserveRoutingMap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		response *controlplanev1.GetConfigResponse
		err      error
	}{
		{name: "client_error", err: errors.New("unavailable")},
		{name: "nil_response"},
		{name: "stale_response", response: &controlplanev1.GetConfigResponse{Revision: 1}},
		{name: "new_revision_without_config", response: &controlplanev1.GetConfigResponse{Revision: 3}},
		{name: "revision_mismatch", response: func() *controlplanev1.GetConfigResponse {
			r := fullResponse(3)
			r.Config.Revision = 2
			return r
		}()},
		{name: "invalid_config", response: func() *controlplanev1.GetConfigResponse {
			cfg := cpconfigtest.Config(3)
			cfg.Regions[0].Role = cpconfig.RegionRoleUnspecified
			return &controlplanev1.GetConfigResponse{Revision: 3, Config: cfg.ToProto()}
		}()},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			router := newRouter(t)
			if err := router.Apply(cpconfigtest.Config(2)); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			client := configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
				return tt.response, tt.err
			})
			poller := gateway.NewPoller(client, router, "gw-eu-1")
			if err := poller.Refresh(context.Background()); err == nil {
				t.Fatal("Refresh succeeded, want error")
			}
			if router.Revision() != 2 {
				t.Fatalf("routing map changed to revision %d", router.Revision())
			}
		})
	}
}

func TestPollerRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	client := configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		return &controlplanev1.GetConfigResponse{}, nil
	})
	if err := (&gateway.Poller{}).Refresh(context.Background()); err == nil {
		t.Fatal("nil client Refresh succeeded")
	}
	if err := (&gateway.Poller{Client: client}).Refresh(context.Background()); err == nil {
		t.Fatal("nil router Refresh succeeded")
	}
	if err := (&gateway.Poller{Client: client, Router: router}).Refresh(context.Background()); err == nil {
		t.Fatal("empty node id Refresh succeeded")
	}
}

func TestPollerRunStopsWithContext(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var calls atomic.Int32
	first := make(chan struct{}, 1)
	client := configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if calls.Add(1) == 1 {
			first <- struct{}{}
		}
		return &controlplanev1.GetConfigResponse{Revision: 1}, nil
	})
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	poller.Interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("poller did not perform the initial refresh")
	}
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

func TestPollerRunSurvivesRefreshErrors(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	var calls atomic.Int32
	first := make(chan struct{}, 1)
	client := configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if calls.Add(1) == 1 {
			first <- struct{}{}
		}
		return nil, errors.New("cp unavailable")
	})
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	poller.Interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("poller did not attempt the initial refresh")
	}
	if router.Revision() != 1 {
		t.Fatalf("routing map changed to revision %d", router.Revision())
	}
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

func TestPollerAppliesSameRevisionLiveView(t *testing.T) {
	t.Parallel()
	router := newRouter(t)
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	view := cpconfigtest.Config(1)
	view.Nodes[0].State = cpconfig.NodeStateDead // eu-1
	client := configClientFunc(func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if req.GetKnownRevision() != 1 {
			t.Fatalf("known revision = %d, want 1", req.GetKnownRevision())
		}
		return &controlplanev1.GetConfigResponse{Revision: 1, Config: view.ToProto()}, nil
	})
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	if err := poller.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, err := router.GetLeader(0); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("GetLeader(0) error = %v, want ErrNoLeader", err)
	}
	if router.Revision() != 1 {
		t.Fatalf("revision = %d, want 1", router.Revision())
	}
}

func TestColdGatewayRequiresFreshCPForWritesAndKeepsTermFloor(t *testing.T) {
	router := newRouter(t)
	disk := cpconfig.Disk{Path: filepath.Join(t.TempDir(), "gateway.json"), Cluster: "test", Region: "eu", Client: "gateway/a", MaxAge: time.Hour}
	config := cpconfigtest.Config(9)
	for i := range config.Partitions {
		config.Partitions[i].ObservedTerm = 6
	}
	if err := disk.Save(cpconfig.Saved{Config: config, AcceptedAt: time.Now(),
		Observations: []cpconfig.Observation{{Region: "eu", Partition: 0, Term: 7, Leader: "eu-1"}}}); err != nil {
		t.Fatal(err)
	}
	returned := config.Clone()
	poller := gateway.NewPoller(configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		return &controlplanev1.GetConfigResponse{Revision: returned.Revision, Config: returned.ToProto()}, nil
	}), router, "a")
	poller.Disk = &disk
	if err := poller.Restore(); err != nil {
		t.Fatal(err)
	}
	if _, err := router.GetLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := router.WriteLeader(1); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("cold write = %v", err)
	}
	returned.Revision = 8
	if err := poller.Refresh(t.Context()); err == nil {
		t.Fatal("restored older CP revision accepted")
	}
	returned.Revision = 9
	if err := poller.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := router.WriteLeader(1); err != nil {
		t.Fatal(err)
	}
	if _, err := router.WriteLeader(0); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("older term write = %v", err)
	}
	returned.Partitions[0].ObservedTerm = 8
	if err := poller.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := router.WriteLeader(0); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayKeepsPublishedMapWhenCheckpointWriteFails(t *testing.T) {
	router := newRouter(t)
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatal(err)
	}
	disk := cpconfig.Disk{Path: filepath.Join(t.TempDir(), "missing-parent", "latest.json"),
		Cluster: "test", Region: "eu", Client: "gateway/a", MaxAge: time.Hour}
	poller := gateway.NewPoller(configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		return fullResponse(2), nil
	}), router, "a")
	poller.Disk = &disk
	if err := poller.Refresh(t.Context()); err == nil || router.Revision() != 1 {
		t.Fatalf("config was published without a durable checkpoint, revision %d, error %v", router.Revision(), err)
	}
}

func TestRestrictedGatewayDoesNotRestoreFollowerReadRoutes(t *testing.T) {
	router, err := gateway.NewRouter("us", gateway.WithMasterRegionReads())
	if err != nil {
		t.Fatal(err)
	}
	disk := cpconfig.Disk{Path: filepath.Join(t.TempDir(), "latest.json"), Cluster: "test", Region: "us", Client: "gateway/b", MaxAge: time.Hour}
	if err := disk.Save(cpconfig.Saved{Config: cpconfigtest.Config(2), AcceptedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	poller := gateway.NewPoller(configClientFunc(func(context.Context, *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		return fullResponse(2), nil
	}), router, "b")
	poller.Disk = &disk
	if err := poller.Restore(); err != nil {
		t.Fatal(err)
	}
	if _, err := router.GetLeader(0); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("restored follower read route: %v", err)
	}
	if err := poller.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := router.GetLeader(0); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("live follower read route: %v", err)
	}
	if _, err := router.WriteLeader(0); err != nil {
		t.Fatalf("master write route: %v", err)
	}
}
