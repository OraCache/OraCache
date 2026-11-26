package gateway

import "github.com/prometheus/client_golang/prometheus"

// Metrics observes background config refreshes and outgoing Storage Node attempts.
type Metrics struct {
	forwards    *prometheus.CounterVec
	refreshes   *prometheus.CounterVec
	lastSuccess *prometheus.GaugeVec
}

// NewMetrics registers the Gateway-specific collectors in the process registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		forwards: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracache_gateway_forward_attempts_total",
			Help: "Started Storage Node RPC attempts, by why the attempt was made.",
		}, []string{"operation", "reason"}),
		refreshes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracache_gateway_config_refresh_total",
			Help: "Completed Gateway config refresh attempts by result.",
		}, []string{"result"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oracache_gateway_config_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last accepted CP response, including an unchanged revision.",
		}, nil),
	}
	reg.MustRegister(m.forwards, m.refreshes, m.lastSuccess)
	for _, result := range []string{"applied_new_revision", "applied_same_revision", "unchanged", "rpc_error", "rejected"} {
		m.refreshes.WithLabelValues(result)
	}
	for _, operation := range []string{"get", "put", "delete", "batch_get", "batch_put"} {
		for _, reason := range []string{"initial", "not_leader", "read_failover"} {
			m.forwards.WithLabelValues(operation, reason)
		}
	}
	return m
}
