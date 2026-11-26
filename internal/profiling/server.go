package profiling

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"runtime/metrics"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/stats"
)

// Listen is called once at process startup, before creating any gRPC servers.
func Listen() (func(), error) {
	addr := os.Getenv("ORACACHE_PPROF_ADDR")
	if addr == "" {
		return func() {}, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("ORACACHE_PPROF_ADDR requires a numeric loopback address and port")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/stages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		result := make(map[string]summary)
		snapshot := current.Load()
		if snapshot == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		for i := range snapshot {
			if s := snapshot[i].summary(); s.Count > 0 {
				result[names[i]] = s
			}
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("POST /debug/stages/reset", func(w http.ResponseWriter, _ *http.Request) {
		current.Store(new(window))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /debug/runtime", runtimeStats)
	current.Store(new(window))
	runtime.SetBlockProfileRate(1000000)
	old := runtime.SetMutexProfileFraction(5)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	go server.Serve(l)
	return func() {
		server.Close()
		current.Store(nil)
		runtime.SetMutexProfileFraction(old)
		runtime.SetBlockProfileRate(0)
	}, nil
}

type timedCodec struct{ encoding.Codec }

func (c timedCodec) Marshal(v any) ([]byte, error) {
	defer Encode.End(Start())
	return c.Codec.Marshal(v)
}

func (c timedCodec) Unmarshal(b []byte, v any) error {
	defer Decode.End(Start())
	return c.Codec.Unmarshal(b, v)
}

func Options(opts ...grpc.ServerOption) []grpc.ServerOption {
	if current.Load() == nil {
		return opts
	}
	return append(opts, grpc.StatsHandler(rpcStats{}), grpc.ForceServerCodec(timedCodec{encoding.GetCodec("proto")}),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, v any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			clock := ctx.Value(clockKey{}).(*rpcClock)
			defer func() { clock.response = time.Now() }()
			defer Handler.End(Start())
			return handler(ctx, v)
		}))
}

// TagRPC runs when gRPC dispatches the stream. InPayload follows codec.Unmarshal.
// End follows the transport write of the status, which can precede socket flush.
type clockKey struct{}
type rpcClock struct{ receive, response time.Time }
type rpcStats struct{}

func (rpcStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, clockKey{}, &rpcClock{receive: time.Now()})
}
func (rpcStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	switch e := event.(type) {
	case *stats.InPayload:
		Receive.End(ctx.Value(clockKey{}).(*rpcClock).receive)
	case *stats.End:
		Send.End(ctx.Value(clockKey{}).(*rpcClock).response)
		Unary.End(e.BeginTime)
	}
}
func (rpcStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (rpcStats) HandleConn(context.Context, stats.ConnStats)                       {}

func runtimeStats(w http.ResponseWriter, _ *http.Request) {
	samples := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
	}
	metrics.Read(samples)
	out := make(map[string]any, len(samples))
	for _, s := range samples {
		switch s.Value.Kind() {
		case metrics.KindUint64:
			out[s.Name] = s.Value.Uint64()
		case metrics.KindFloat64:
			out[s.Name] = s.Value.Float64()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
