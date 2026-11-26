package gateway_test

import (
	"context"
	"errors"
	"testing"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/gateway"
	"github.com/prometheus/client_golang/prometheus"
)

func TestForwardMetricsCountActualNotLeaderRetry(t *testing.T) {
	f := newForwarder(t, map[string]*fakeNode{"a1": {err: notLeader(t, "a3")}, "a3": {rev: 3}})
	reg := prometheus.NewRegistry()
	f.Metrics = gateway.NewMetrics(reg)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := f.Put(ctx, &pb.PutRequest{Key: "k"}); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"initial", "not_leader"} {
		want := "oracache_gateway_forward_attempts_total{operation=\"put\",reason=\"" + reason + "\"}"
		if got := registeredValue(t, reg, "oracache_gateway_forward_attempts_total", map[string]string{"operation": "put", "reason": reason}); got != 1 {
			t.Errorf("%s = %g, want 1", want, got)
		}
	}
}

func TestPollMetricsPreserveSuccessOnFailureAndAcceptSameRevision(t *testing.T) {
	router := newRouter(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	simulateFailure := false
	poller := gateway.NewPoller(configClientFunc(func(_ context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
		if simulateFailure {
			return nil, errors.New("CP disconnected")
		}
		if req.GetKnownRevision() == 0 {
			return fullResponse(4), nil
		}
		return &controlplanev1.GetConfigResponse{Revision: 4}, nil
	}), router, "gw-eu-1")
	reg := prometheus.NewRegistry()
	poller.Metrics = gateway.NewMetrics(reg)
	const last = "oracache_gateway_config_last_success_timestamp_seconds"
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == last {
			t.Fatal("last success present before first poll")
		}
	}
	if err := poller.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	first := registeredValue(t, reg, last, nil)
	if err := poller.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	second := registeredValue(t, reg, last, nil)
	if first <= 0 || second <= first {
		t.Fatalf("successful poll timestamps: first=%g, same revision=%g", first, second)
	}
	simulateFailure = true
	if err := poller.Refresh(ctx); err == nil {
		t.Fatal("expected CP poll failure")
	}
	if got := registeredValue(t, reg, last, nil); got != second || router.Revision() != 4 {
		t.Errorf("failed refresh changed last success or routing revision: timestamp=%g, revision=%d", got, router.Revision())
	}
	for _, result := range []string{"applied_new_revision", "unchanged", "rpc_error"} {
		if got := registeredValue(t, reg, "oracache_gateway_config_refresh_total", map[string]string{"result": result}); got != 1 {
			t.Errorf("refresh count for %s = %g, want 1", result, got)
		}
	}
}

func registeredValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(labels) == 0 {
				return metric.GetGauge().GetValue()
			}
			matches := 0
			for _, pair := range metric.GetLabel() {
				if labels[pair.GetName()] == pair.GetValue() {
					matches++
				}
			}
			if matches == len(labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("missing metric %s with labels %v", name, labels)
	return 0
}
