package cpapi

import (
	"context"
	"time"

	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/statestore"
)

const (
	storeMetricsTimeout = 500 * time.Millisecond
	storeMetricsMaxAge  = 2 * time.Second
)

type storeMetricsSnapshot struct {
	checkedAt time.Time
	ready     bool
	receipts  []statestore.HeartbeatReceipt
}

// RefreshStoreMetrics samples StateStore outside the HTTP scrape path.
// The caller schedules refreshes serially.
func (s *Server) RefreshStoreMetrics(ctx context.Context) {
	check := &storeMetricsSnapshot{checkedAt: time.Now()}
	poll, cancel := context.WithTimeout(ctx, storeMetricsTimeout)
	defer cancel()
	check.ready = s.Ready(poll)
	if check.ready {
		receipts, err := s.store.HeartbeatReceipts(poll)
		if err != nil {
			s.storeError(poll, "heartbeat_receipts", err)
		} else {
			check.receipts = receipts
		}
	}
	s.storeMetrics.Store(check)
}

func (s *Server) currentStoreMetrics() *storeMetricsSnapshot {
	check := s.storeMetrics.Load()
	if check == nil || time.Since(check.checkedAt) >= storeMetricsMaxAge {
		return nil
	}
	return check
}

func (s *Server) readinessSamples(context.Context) ([]cpmetrics.GaugeSample, error) {
	sample := cpmetrics.GaugeSample{}
	if check := s.currentStoreMetrics(); check != nil && check.ready {
		sample.Value = 1
	}
	return []cpmetrics.GaugeSample{sample}, nil
}

func (s *Server) heartbeatAgeSamples(context.Context) ([]cpmetrics.GaugeSample, error) {
	check := s.currentStoreMetrics()
	if check == nil {
		return nil, nil
	}
	now := s.clock.Now()
	samples := make([]cpmetrics.GaugeSample, 0, len(check.receipts))
	for _, receipt := range check.receipts {
		samples = append(samples, cpmetrics.GaugeSample{
			Labels: map[string]string{"region": receipt.Region, "node_id": receipt.NodeID},
			Value:  max(0, now.Sub(receipt.ReceivedAt).Seconds()),
		})
	}
	return samples, nil
}
