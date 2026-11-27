package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type operationMetrics struct {
	scheduled, sent, schedulerLate, inflightLimit, sentLate prometheus.Counter
	completed                                               *prometheus.CounterVec
	latency                                                 prometheus.Observer
}

func serveMetrics(addr string) ([2]*operationMetrics, func(), error) {
	var ops [2]*operationMetrics
	registry := prometheus.NewRegistry()
	slots := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_load_slots_total",
		Help: "Measured slots scheduled, sent, skipped, or sent more than 5ms late (a subset of sent).",
	}, []string{"operation", "state"})
	completed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_load_rpc_completed_total",
		Help: "Completed measured RPCs by outcome, including transport and cache errors.",
	}, []string{"operation", "outcome"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storage_load_success_latency_seconds",
		Help:    "Time from scheduled start to successful response in seconds.",
		Buckets: []float64{.00025, .0005, .001, .0025, .005, .01, .02, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"operation"})
	registry.MustRegister(slots, completed, latency)
	for i, op := range []string{"Get", "Put"} {
		ops[i] = &operationMetrics{
			scheduled:     slots.WithLabelValues(op, "scheduled"),
			sent:          slots.WithLabelValues(op, "sent"),
			schedulerLate: slots.WithLabelValues(op, "scheduler_late"),
			inflightLimit: slots.WithLabelValues(op, "inflight_limit"),
			sentLate:      slots.WithLabelValues(op, "sent_over_5ms_late"),
			completed:     completed.MustCurryWith(prometheus.Labels{"operation": op}),
			latency:       latency.WithLabelValues(op),
		}
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return ops, nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()
	log.Printf("metrics listening on %s", ln.Addr())
	return ops, func() { _ = srv.Close() }, nil
}
