package node

import (
	"strconv"
	"time"

	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/shardraft"
	"github.com/prometheus/client_golang/prometheus"
)

type raftMetrics struct {
	replica    *Node
	gauges     []*prometheus.Desc
	stages     map[shardraft.TimingStage]prometheus.Observer
	sendErrors prometheus.Counter
}

func newRaftMetrics(reg prometheus.Registerer, n *Node) *raftMetrics {
	labels := prometheus.Labels{
		"node_id": n.cfg.NodeID, "region": n.cfg.Region, "partition": strconv.FormatUint(uint64(n.cfg.ShardID), 10),
	}
	m := &raftMetrics{
		replica: n,
		gauges: []*prometheus.Desc{
			prometheus.NewDesc("oracache_raft_is_leader", "Replica reports its local role as leader.", nil, labels),
			prometheus.NewDesc("oracache_raft_is_follower", "Replica reports its local role as follower.", nil, labels),
			prometheus.NewDesc("oracache_raft_has_leader", "Replica has a leader ID in its local Raft snapshot.", nil, labels),
			prometheus.NewDesc("oracache_raft_ready", "Running replica knows a leader and has applied through its local commit index.", nil, labels),
			prometheus.NewDesc("oracache_raft_term", "Term in the local Raft snapshot.", nil, labels),
			prometheus.NewDesc("oracache_raft_commit_index", "Committed log position in the local Raft snapshot.", nil, labels),
			prometheus.NewDesc("oracache_raft_applied_index", "Applied log position in the local Raft snapshot.", nil, labels),
			prometheus.NewDesc("oracache_raft_snapshot_index", "Log position of the latest local checkpoint or installed snapshot.", nil, labels),
			prometheus.NewDesc("oracache_raft_last_index", "End of the local in-memory Raft log, including uncommitted entries.", nil, labels),
			prometheus.NewDesc("oracache_storage_keys", "Number of live keys in the local versioned Store.", nil, labels),
			prometheus.NewDesc("oracache_storage_accounted_bytes", "Bytes charged to the local Store memory budget.", nil, labels),
		},
		sendErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "oracache_raft_transport_send_errors_total", Help: "Failed outbound Raft transport sends.", ConstLabels: labels,
		}),
	}
	durations := cpmetrics.NewDuration(prometheus.WrapRegistererWith(labels, reg),
		"oracache_raft_stage_duration_seconds", "Raft stage duration in seconds.", "stage")
	m.stages = map[shardraft.TimingStage]prometheus.Observer{
		shardraft.StageApply:      durations.WithLabelValues("apply"),
		shardraft.StageReadIndex:  durations.WithLabelValues("read_index"),
		shardraft.StageCheckpoint: durations.WithLabelValues("checkpoint"),
		shardraft.StageRestore:    durations.WithLabelValues("restore"),
	}
	reg.MustRegister(m, m.sendErrors)
	return m
}

func (m *raftMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range m.gauges {
		ch <- desc
	}
}

func (m *raftMetrics) Collect(ch chan<- prometheus.Metric) {
	s := m.replica.Status()
	stats := m.replica.Store().Stats()
	for i, value := range [...]float64{
		boolGauge(s.Running && s.Role == "StateLeader"),
		boolGauge(s.Running && s.Role == "StateFollower"),
		boolGauge(s.Running && s.LeaderID != 0),
		boolGauge(s.Ready),
		float64(s.Term), float64(s.CommitIndex), float64(s.AppliedIndex), float64(s.SnapshotIndex), float64(s.LastIndex),
		float64(stats.Keys), float64(stats.Bytes),
	} {
		ch <- prometheus.MustNewConstMetric(m.gauges[i], prometheus.GaugeValue, value)
	}
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (m *raftMetrics) ObserveDuration(stage shardraft.TimingStage, duration time.Duration) {
	m.stages[stage].Observe(duration.Seconds())
}

func (m *raftMetrics) RecordSendError() { m.sendErrors.Inc() }
