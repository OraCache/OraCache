package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"google.golang.org/grpc"

	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/statestore"
)

type slowMetricsStore struct {
	statestore.Store
	unavailable atomic.Bool
	waiting     chan struct{}
}

func (s *slowMetricsStore) Config(ctx context.Context) (statestore.Snapshot, error) {
	if err := s.wait(ctx); err != nil {
		return statestore.Snapshot{}, err
	}
	return s.Store.Config(ctx)
}

func (s *slowMetricsStore) HeartbeatReceipts(ctx context.Context) ([]statestore.HeartbeatReceipt, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Store.HeartbeatReceipts(ctx)
}

func (s *slowMetricsStore) wait(ctx context.Context) error {
	if !s.unavailable.Load() {
		return nil
	}
	select {
	case s.waiting <- struct{}{}:
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	return ctx.Err()
}

func TestMetricsEndpointSurvivesSlowStateStore(t *testing.T) {
	t.Parallel()
	store := &slowMetricsStore{Store: statestore.NewMemory(), waiting: make(chan struct{}, 1)}
	if err := store.ApplyConfig(t.Context(), cpconfigtest.Config(1)); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := cpapi.New(store, cpapi.WithLogger(logger))
	if _, err := api.ReportHeartbeat(t.Context(), &controlplanev1.HeartbeatRequest{NodeId: "eu-1", Region: "eu"}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.GetConfig(t.Context(), &controlplanev1.GetConfigRequest{
		NodeId: "eu-1", Kind: controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE,
	}); err != nil {
		t.Fatal(err)
	}
	address, err := serveMetrics(t.Context(), "127.0.0.1:0", api, logger)
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + address.String()
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	scrape := func() string {
		t.Helper()
		response, err := client.Get(url + "/metrics")
		if err != nil {
			t.Fatalf("one-second scrape lost the entire response with accumulated heartbeat and config-pull counters: %v", err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		body := string(data)
		if response.StatusCode != http.StatusOK ||
			!strings.Contains(body, "\noracache_cp_heartbeat_total{result=\"accepted\"} ") ||
			!strings.Contains(body, "\noracache_cp_config_pull_total{result=\"served\"} 1\n") {
			t.Fatalf("status = %d, expected accumulated CP counters:\n%s", response.StatusCode, body)
		}
		return body
	}
	assertReadiness := func(want int) {
		t.Helper()
		client := &http.Client{Timeout: 3 * time.Second}
		defer client.CloseIdleConnections()
		response, err := client.Get(url + "/readyz")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("/readyz = %d, want %d", response.StatusCode, want)
		}
	}
	awaitMetrics := func(wantReady, wantSeries string) string {
		t.Helper()
		const heartbeatAge = "\noracache_cp_heartbeat_age_seconds{"
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		timeout := time.NewTimer(4 * time.Second)
		defer timeout.Stop()
		for {
			body := scrape()
			hasAges := strings.Contains(body, heartbeatAge)
			if strings.Contains(body, "\noracache_cp_config_ready "+wantReady+"\n") &&
				((wantSeries == "" && !hasAges) || (wantSeries != "" && strings.Contains(body, heartbeatAge+wantSeries))) {
				return body
			}
			select {
			case <-timeout.C:
				t.Fatalf("expected readiness %s and heartbeat series %q:\n%s", wantReady, wantSeries, body)
			case <-ticker.C:
			}
		}
	}

	awaitMetrics("1", `node_id="eu-1",region="eu"} `)
	store.unavailable.Store(true)
	select {
	case <-store.waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("metrics poll never reached the slow StateStore")
	}
	scrape()
	if body := awaitMetrics("0", ""); !strings.Contains(body, "\noracache_cp_heartbeat_total{result=\"accepted\"} 1\n") {
		t.Fatalf("accepted heartbeat was lost during outage:\n%s", body)
	}
	assertReadiness(http.StatusServiceUnavailable)

	if _, err := api.ReportHeartbeat(t.Context(), &controlplanev1.HeartbeatRequest{NodeId: "eu-2", Region: "eu"}); err != nil {
		t.Fatal(err)
	}
	store.unavailable.Store(false)
	assertReadiness(http.StatusOK)
	awaitMetrics("1", `node_id="eu-2",region="eu"} `)
}

func newTestServer(t *testing.T) (*grpc.Server, *cpapi.Server) {
	t.Helper()
	server, api := newServer(statestore.NewMemory(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(server.Stop)
	return server, api
}

func TestNewServerRegistersControlPlaneAPI(t *testing.T) {
	t.Parallel()
	server, _ := newTestServer(t)
	services := server.GetServiceInfo()
	if _, ok := services["controlplane.v1.ControlPlaneAPI"]; !ok {
		t.Fatalf("ControlPlaneAPI is not registered: %v", services)
	}
}

func TestServeMetricsSkipsWhenDisabled(t *testing.T) {
	t.Parallel()
	_, api := newTestServer(t)
	if _, err := serveMetrics(t.Context(), "", api, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("serveMetrics(disabled) = %v, want nil", err)
	}
}

func TestServeMetricsRejectsBusyPort(t *testing.T) {
	t.Parallel()
	_, api := newTestServer(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	if _, err := serveMetrics(t.Context(), listener.Addr().String(), api, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("serveMetrics accepted an occupied address")
	}
}

func TestMetricsEndpointExposesRegistry(t *testing.T) {
	t.Parallel()
	_, api := newTestServer(t)
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.Metrics().Handler())

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "oracache_cp_config_revision") {
		t.Fatalf("metrics body missing CP metrics:\n%s", body)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/plain") {
		t.Fatalf("content-type = %q", got)
	}

	request = httptest.NewRequest(http.MethodPost, "/metrics", nil)
	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", recorder.Code)
	}
}
