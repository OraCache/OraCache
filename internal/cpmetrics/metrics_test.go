package cpmetrics_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/OraCache/oracache/internal/cpmetrics"
)

func render(t *testing.T, m *cpmetrics.Metrics) string {
	t.Helper()
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return buf.String()
}

func TestRenderEmpty(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	out := render(t, m)
	if !strings.Contains(out, "# TYPE oracache_cp_heartbeat_total counter") {
		t.Fatalf("metadata missing:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE oracache_cp_config_revision gauge") {
		t.Fatalf("gauge metadata missing:\n%s", out)
	}
}

func TestCounterIncrements(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	m.Heartbeat(cpmetrics.HeartbeatAccepted)
	m.Heartbeat(cpmetrics.HeartbeatAccepted)
	m.Heartbeat(cpmetrics.HeartbeatInvalid)
	out := render(t, m)
	if strings.Count(out, `oracache_cp_heartbeat_total{result="accepted"} 2`) != 1 {
		t.Fatalf("accepted counter wrong:\n%s", out)
	}
	if strings.Count(out, `oracache_cp_heartbeat_total{result="invalid"} 1`) != 1 {
		t.Fatalf("invalid counter wrong:\n%s", out)
	}
}

func TestGaugeAndFamilyReplacement(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	m.SetConfigRevision(7)
	m.SetLiveNodes(map[string]uint64{
		cpmetrics.NodeStateReady: 2,
		cpmetrics.NodeStateDead:  1,
	})
	out := render(t, m)
	if !strings.Contains(out, "oracache_cp_config_revision 7") {
		t.Fatalf("revision gauge wrong:\n%s", out)
	}
	if !strings.Contains(out, `oracache_cp_live_nodes{state="ready"} 2`) ||
		!strings.Contains(out, `oracache_cp_live_nodes{state="dead"} 1`) {
		t.Fatalf("live nodes wrong:\n%s", out)
	}

	// Состояния меняются: устаревшие series должны исчезнуть.
	m.SetLiveNodes(map[string]uint64{cpmetrics.NodeStateReady: 1})
	out = render(t, m)
	if strings.Contains(out, `state="dead"`) {
		t.Fatalf("stale state series left:\n%s", out)
	}
	if !strings.Contains(out, `oracache_cp_live_nodes{state="ready"} 1`) {
		t.Fatalf("updated state series missing:\n%s", out)
	}
}

func TestLabelEscaping(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	m.Describe("test_metric", "gauge", "test")
	m.SetGauge("test_metric", map[string]string{"k": `a"b\c` + "\n"}, 1)
	out := render(t, m)
	if !strings.Contains(out, `test_metric{k="a\"b\\c\n"} 1`) {
		t.Fatalf("escaping wrong:\n%s", out)
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	for i := 0; i < 5; i++ {
		m.Heartbeat(cpmetrics.HeartbeatAccepted)
		m.ConfigPull(cpmetrics.PullServed)
	}
	m.SetConfigRevision(1)
	m.SetLiveNodes(map[string]uint64{cpmetrics.NodeStateReady: 1})
	first := render(t, m)
	for i := 0; i < 5; i++ {
		if got := render(t, m); got != first {
			t.Fatalf("render not deterministic at %d", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				m.Heartbeat(cpmetrics.HeartbeatAccepted)
				m.ConfigPull(cpmetrics.PullServed)
				m.SetConfigRevision(uint64(j))
				m.SetLiveNodes(map[string]uint64{cpmetrics.NodeStateReady: uint64(j)})
				var buf bytes.Buffer
				_ = m.Render(&buf)
			}
		}(i)
	}
	wg.Wait()
	out := render(t, m)
	if !strings.Contains(out, `oracache_cp_heartbeat_total{result="accepted"} 800`) {
		t.Fatalf("concurrent counter wrong:\n%s", out)
	}
}

func TestUnknownMetricWithoutMetadata(t *testing.T) {
	t.Parallel()
	m := cpmetrics.New()
	m.IncCounter("oracache_cp_custom_total", map[string]string{"a": "b"})
	out := render(t, m)
	if !strings.Contains(out, `oracache_cp_custom_total{a="b"} 1`) {
		t.Fatalf("untyped metric missing:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE oracache_cp_custom_total untyped") {
		t.Fatalf("untyped fallback missing:\n%s", out)
	}
}
