package cpapi_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/OraCache/oracache/internal/statestore"
)

// TestPhase1HeartbeatAndConfigPull проверяет реальную gRPC-границу:
// CP API -> CP Agent heartbeat/pull и CP API -> Gateway routing-map pull.
func TestPhase1HeartbeatAndConfigPull(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := statestore.NewMemory()
	if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
		t.Fatalf("ApplyConfig(1): %v", err)
	}
	clock := &mutableClock{now: time.Now().UTC()}
	srv := cpapi.New(store, cpapi.WithHeartbeatInterval(25*time.Millisecond), cpapi.WithClock(clock))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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

	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := controlplanev1.NewControlPlaneAPIClient(conn)

	holder := cpconfig.NewHolder()
	agent, err := cpagent.New(cpagent.Options{
		NodeID:   "eu-1",
		Region:   "eu",
		Instance: "node-eu-1",
		Client:   client,
		Statuses: cpagent.StatusProviderFunc(func(context.Context) ([]*controlplanev1.PartitionStatus, error) {
			return []*controlplanev1.PartitionStatus{
				{PartitionId: 0, Role: controlplanev1.RaftRole_RAFT_ROLE_LEADER, LeaderId: "eu-1", Ready: true},
			}, nil
		}),
		Holder:             holder,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		DisableJitter:      true,
		HeartbeatInterval:  20 * time.Millisecond,
		ConfigPullInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("cpagent.New: %v", err)
	}
	agentCtx, stopAgent := context.WithCancel(ctx)
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(agentCtx) }()
	var stopOnce sync.Once
	stopAgentNow := func() {
		stopOnce.Do(func() {
			stopAgent()
			select {
			case <-agentDone:
			case <-time.After(2 * time.Second):
				t.Error("cp agent did not stop")
			}
		})
	}
	t.Cleanup(stopAgentNow)

	waitFor(t, 3*time.Second, func() bool {
		hb, ok, err := store.Heartbeat(ctx, "eu-1")
		return err == nil && ok && len(hb.Partitions) == 1 && agent.Revision() == 1
	})

	router, err := gateway.NewRouter("eu")
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	poller := gateway.NewPoller(client, router, "gw-eu-1")
	poller.RPCTimeout = time.Second
	if err := poller.Refresh(ctx); err != nil {
		t.Fatalf("gateway Refresh: %v", err)
	}
	if router.Revision() != 1 {
		t.Fatalf("gateway revision = %d, want 1", router.Revision())
	}
	leader, err := router.GetLeader(0)
	if err != nil {
		t.Fatalf("gateway GetLeader: %v", err)
	}
	if leader.NodeID != "eu-1" {
		t.Fatalf("initial leader = %+v, want eu-1", leader)
	}

	if err := store.ApplyConfig(ctx, cpconfigtest.Config(2)); err != nil {
		t.Fatalf("ApplyConfig(2): %v", err)
	}

	// Агент узнаёт новую revision из heartbeat и сразу делает pull.
	waitFor(t, 3*time.Second, func() bool { return agent.Revision() == 2 })

	if err := poller.Refresh(ctx); err != nil {
		t.Fatalf("gateway Refresh(2): %v", err)
	}
	if router.Revision() != 2 {
		t.Fatalf("gateway revision = %d, want 2", router.Revision())
	}
	leader, err = router.GetLeader(0)
	if err != nil {
		t.Fatalf("gateway GetLeader after update: %v", err)
	}
	if leader.NodeID != "eu-1" {
		t.Fatalf("live leader = %+v, want eu-1", leader)
	}

	// Останавливаем агент и переводим часы CP за TTL: gateway должен получить
	// live-view, в котором нода мертва, и перестать маршрутизировать.
	stopAgentNow()
	clock.Advance(time.Minute)
	if err := poller.Refresh(ctx); err != nil {
		t.Fatalf("gateway Refresh(expired): %v", err)
	}
	if _, err := router.GetLeader(0); !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("GetLeader after TTL error = %v, want ErrNoLeader", err)
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
