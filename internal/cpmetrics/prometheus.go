package cpmetrics

import (
	"context"
	"strings"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Prometheus returns the process-local registry for standard collectors.
func (m *Metrics) Prometheus() *prometheus.Registry { return m.prometheus }

// RegisterProcessCollectors exposes Go runtime and OS process metrics.
func (m *Metrics) RegisterProcessCollectors() {
	m.prometheus.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// NewDuration registers a classic histogram in seconds, from sub-millisecond
// service time through the existing RPC deadlines.
func NewDuration(reg prometheus.Registerer, name, help string, labels ...string) *prometheus.HistogramVec {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: name, Help: help,
		Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, labels)
	reg.MustRegister(hist)
	return hist
}

// CacheRPCInterceptor observes one completed unary cache RPC at each server boundary.
func CacheRPCInterceptor(reg prometheus.Registerer) grpc.UnaryServerInterceptor {
	calls := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "oracache_cache_rpc_total", Help: "Completed unary cache RPCs by operation, domain outcome and gRPC code.",
	}, []string{"operation", "outcome", "grpc_code"})
	reg.MustRegister(calls)
	durations := NewDuration(reg, "oracache_cache_rpc_duration_seconds", "Unary cache RPC handler duration in seconds.", "operation")

	for operation, outcomes := range map[string][]string{
		"get":    {"hit", "miss", "version_not_found", "unspecified"},
		"put":    {"ok", "conflict", "too_large", "unspecified"},
		"delete": {"deleted", "not_found", "unspecified"},
	} {
		durations.WithLabelValues(operation)
		for _, outcome := range outcomes {
			calls.WithLabelValues(operation, outcome, codes.OK.String())
		}
		for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
			calls.WithLabelValues(operation, "none", code.String())
		}
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var operation string
		switch info.FullMethod {
		case pb.CacheGateway_Get_FullMethodName:
			operation = "get"
		case pb.CacheGateway_Put_FullMethodName:
			operation = "put"
		case pb.CacheGateway_Delete_FullMethodName:
			operation = "delete"
		default:
			return handler(ctx, req)
		}
		start := time.Now()
		response, err := handler(ctx, req)
		durations.WithLabelValues(operation).Observe(time.Since(start).Seconds())
		code := status.Code(err)
		if code > codes.Unauthenticated {
			code = codes.Unknown
		}
		outcome := "none"
		if err == nil {
			outcome = cacheOutcome(response)
		}
		calls.WithLabelValues(operation, outcome, code.String()).Inc()
		return response, err
	}
}

func cacheOutcome(response any) string {
	var name string
	switch res := response.(type) {
	case *pb.GetResponse:
		name = pb.GetStatus_name[int32(res.GetStatus())]
		name = strings.TrimPrefix(name, "GET_STATUS_")
	case *pb.PutResponse:
		name = pb.PutStatus_name[int32(res.GetStatus())]
		name = strings.TrimPrefix(name, "PUT_STATUS_")
	case *pb.DeleteResponse:
		name = pb.DeleteStatus_name[int32(res.GetStatus())]
		name = strings.TrimPrefix(name, "DELETE_STATUS_")
	}
	if name == "" {
		return "unspecified"
	}
	return strings.ToLower(name)
}
