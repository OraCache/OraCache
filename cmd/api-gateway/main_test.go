package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/gateway"
)

// disabledStop повторяет ветку main без listener'а.
func disabledStop() func() { return func() {} }

func testRouter(t *testing.T, region string) *gateway.Router {
	t.Helper()
	router, err := gateway.NewRouter(region)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return router
}

func TestStatusHandlerRoutes(t *testing.T) {
	t.Parallel()
	router := testRouter(t, "eu")
	handler := newStatusHandler(router, cpmetrics.NewRegistry())

	// До применения конфигурации карты нет.
	request := httptest.NewRequest(http.MethodGet, "/routes", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("routes status = %d, want 200", recorder.Code)
	}
	var empty struct {
		Revision       uint64 `json:"revision"`
		LocalRegion    string `json:"local_region"`
		PartitionCount uint32 `json:"partition_count"`
		ReadRoutes     map[uint32]struct {
			NodeID string `json:"node_id"`
		} `json:"read_routes"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&empty); err != nil {
		t.Fatalf("decode empty routes: %v", err)
	}
	if empty.Revision != 0 || empty.LocalRegion != "eu" || len(empty.ReadRoutes) != 0 {
		t.Fatalf("empty routes = %+v", empty)
	}

	if err := router.Apply(cpconfigtest.Config(2)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/routes", nil))
	var loaded gateway.RoutesSnapshot
	if err := json.NewDecoder(recorder.Body).Decode(&loaded); err != nil {
		t.Fatalf("decode routes: %v", err)
	}
	if !loaded.Ready() {
		t.Fatalf("loaded routes not ready: %+v", loaded)
	}
	if loaded.Revision != 2 || loaded.MasterRegion != "eu" {
		t.Fatalf("loaded metadata = %+v", loaded)
	}
	if len(loaded.ReadRoutes) != int(cpconfigtest.PartitionCount) ||
		len(loaded.WriteRoutes) != int(cpconfigtest.PartitionCount) {
		t.Fatalf("loaded routes read=%d write=%d", len(loaded.ReadRoutes), len(loaded.WriteRoutes))
	}
	leader, ok := loaded.ReadRoutes[0]
	if !ok || leader.NodeID != "eu-1" {
		t.Fatalf("partition 0 read route = %+v", leader)
	}
}

func TestStatusHandlerHealthz(t *testing.T) {
	t.Parallel()
	router := testRouter(t, "eu")
	handler := newStatusHandler(router, cpmetrics.NewRegistry())

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz without config = %d, want 503", recorder.Code)
	}

	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("healthz with config = %d, want 200", recorder.Code)
	}
	if body := recorder.Body.String(); body != "ok\n" {
		t.Fatalf("healthz body = %q", body)
	}
}

func TestStatusHandlerMissingRouteMetrics(t *testing.T) {
	t.Parallel()
	router := testRouter(t, "us")
	handler := newStatusHandler(router, cpmetrics.NewRegistry())
	assertMissing := func(want map[string]float64) {
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
		samples := families["oracache_gateway_partitions_without_route"].GetMetric()
		if len(samples) != len(want) {
			t.Fatalf("missing routes samples = %v, want %v", samples, want)
		}
		for _, sample := range samples {
			labels := sample.GetLabel()
			if len(labels) != 1 || labels[0].GetName() != "operation" || sample.GetGauge() == nil {
				t.Fatalf("invalid missing routes sample = %v", sample)
			}
			operation := labels[0].GetValue()
			if expected, ok := want[operation]; !ok || sample.GetGauge().GetValue() != expected {
				t.Fatalf("missing routes sample = %v, want %v", sample, want)
			}
		}
	}
	cfg := cpconfigtest.Config(1)
	assertMissing(nil)
	if err := router.ApplyView(cfg); err != nil {
		t.Fatalf("ApplyView: %v", err)
	}
	assertMissing(map[string]float64{"get": 0, "put": 0})
	for i := range cfg.Partitions {
		assignment := &cfg.Partitions[i]
		if assignment.Region == "us" && assignment.PartitionID == 0 {
			assignment.Readable = false
		}
		if assignment.Region == "eu" && assignment.PartitionID < 2 {
			assignment.Writable = false
		}
	}
	if err := router.ApplyView(cfg); err != nil {
		t.Fatalf("ApplyView at same revision: %v", err)
	}
	assertMissing(map[string]float64{"get": 1, "put": 2})
}

func TestStatusHandlerSlaveRegionRoutesToMaster(t *testing.T) {
	t.Parallel()
	router := testRouter(t, "us")
	handler := newStatusHandler(router, cpmetrics.NewRegistry())
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/routes", nil))
	var snapshot gateway.RoutesSnapshot
	if err := json.NewDecoder(recorder.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snapshot.LocalRegion != "us" || snapshot.MasterRegion != "eu" {
		t.Fatalf("regions = %s/%s, want us/eu", snapshot.LocalRegion, snapshot.MasterRegion)
	}
	readLeader, ok := snapshot.ReadRoutes[0]
	if !ok || readLeader.Region != "us" {
		t.Fatalf("slave read route = %+v", readLeader)
	}
	writeLeader, ok := snapshot.WriteRoutes[0]
	if !ok || writeLeader.Region != "eu" {
		t.Fatalf("slave write route = %+v, want master eu", writeLeader)
	}
}

func TestServeStatusDisabledReturnsNoop(t *testing.T) {
	t.Parallel()
	stop := disabledStop()
	stop()
}

func TestServeStatusServesRoutes(t *testing.T) {
	t.Parallel()
	router := testRouter(t, "eu")
	if err := router.Apply(cpconfigtest.Config(1)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stop := serveStatus(listener, router, cpmetrics.NewRegistry(), "eu", logger)
	defer stop()

	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/routes")
	if err != nil {
		t.Fatalf("get routes: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("routes status = %d", response.StatusCode)
	}
	var snapshot gateway.RoutesSnapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !snapshot.Ready() || len(snapshot.ReadRoutes) == 0 {
		t.Fatalf("served routes = %+v", snapshot)
	}

	health, err := client.Get("http://" + listener.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", health.StatusCode)
	}
}
