package profiling

import (
	"math"
	"testing"
)

// Short waits on coarse clocks can round to zero; both extremes must fit
// the histogram, and published percentiles must enclose their samples.
func TestHistogramBounds(t *testing.T) {
	for _, ns := range []uint64{0, 1, 7, 8, 15, 16, 17, 31, 32, 1e6, 1e9, math.MaxInt64} {
		i := bucket(ns)
		if i < 0 || i >= len(histogram{}.buckets) || upper(i) < ns || (i > 0 && upper(i-1) >= ns) {
			t.Fatalf("wrong bucket for %d: %d", ns, i)
		}
	}
	var h histogram
	for i := uint64(1); i <= 100; i++ {
		h.sum.Add(i)
		h.buckets[bucket(i)].Add(1)
	}
	s := h.summary()
	if s.Count != 100 || s.Mean != 50.5 || s.P50 != upper(bucket(50)) || s.P95 != upper(bucket(95)) || s.P99 != upper(bucket(99)) {
		t.Fatal(s)
	}
}

func TestRejectPublicListener(t *testing.T) {
	for _, addr := range []string{":6060", "0.0.0.0:6060", "[::]:6060", "192.0.2.1:6060"} {
		t.Setenv("ORACACHE_PPROF_ADDR", addr)
		if close, err := Listen(); err == nil {
			close()
			t.Fatalf("accepted %q", addr)
		}
	}
}
