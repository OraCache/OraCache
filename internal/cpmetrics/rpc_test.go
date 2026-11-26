package cpmetrics_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCacheRPCOutcomeAndDuration(t *testing.T) {
	t.Parallel()
	registry := cpmetrics.NewRegistry()
	intercept := cpmetrics.CacheRPCInterceptor(registry.Prometheus())
	tests := []struct {
		method, operation, outcome, code string
		response                         any
		err                              error
	}{
		{pb.CacheGateway_Get_FullMethodName, "get", "hit", "OK", &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT}, nil},
		{pb.CacheGateway_Get_FullMethodName, "get", "miss", "OK", &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}, nil},
		{pb.CacheGateway_Put_FullMethodName, "put", "conflict", "OK", &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_CONFLICT}, nil},
		{pb.CacheGateway_Delete_FullMethodName, "delete", "not_found", "OK", &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_NOT_FOUND}, nil},
		{pb.CacheGateway_Put_FullMethodName, "put", "none", "Unavailable", nil, status.Error(codes.Unavailable, "a message that must not become a label")},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for _, tc := range tests {
		got, err := intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
			return tc.response, tc.err
		})
		if got != tc.response || status.Code(err) != status.Code(tc.err) {
			t.Fatalf("interceptor changed response for %s/%s: %v, %v", tc.operation, tc.outcome, got, err)
		}
	}
	families, err := registry.Prometheus().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "oracache_cache_rpc_total":
			for _, tc := range tests {
				matches := 0
				for _, metric := range family.GetMetric() {
					labels := map[string]string{}
					for _, label := range metric.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					if len(labels) == 3 && labels["operation"] == tc.operation && labels["outcome"] == tc.outcome &&
						labels["grpc_code"] == tc.code && metric.GetCounter().GetValue() == 1 {
						matches++
					}
				}
				if matches != 1 {
					t.Errorf("want one counter for %s/%s/%s, found %d", tc.operation, tc.outcome, tc.code, matches)
				}
			}
		case "oracache_cache_rpc_duration_seconds":
			want := map[string]uint64{"get": 2, "put": 2, "delete": 1}
			for _, metric := range family.GetMetric() {
				operation := metric.GetLabel()[0].GetValue()
				if got := metric.GetHistogram().GetSampleCount(); got != want[operation] {
					t.Errorf("%s histogram count = %d, want %d", operation, got, want[operation])
				}
				delete(want, operation)
			}
			if len(want) != 0 {
				t.Errorf("missing histogram operations: %v", want)
			}
		}
	}
	if len(families) != 2 {
		t.Errorf("want counter and histogram, got %v", families)
	}
}
