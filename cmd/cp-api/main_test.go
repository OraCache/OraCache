package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/statestore"
)

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
	if err := serveMetrics("", api, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
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
	if err := serveMetrics(listener.Addr().String(), api, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
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
