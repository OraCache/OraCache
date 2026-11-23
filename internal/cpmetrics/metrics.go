// Package cpmetrics exposes the established Control Plane metrics alongside standard Prometheus collectors.
package cpmetrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// Метрики Control Plane. Имена фиксируются: меняются только через отдельное
// изменение контракта наблюдаемости.
const (
	HeartbeatTotal  = "oracache_cp_heartbeat_total"
	HeartbeatAge    = "oracache_cp_heartbeat_age_seconds"
	ConfigPullTotal = "oracache_cp_config_pull_total"
	ConfigRevision  = "oracache_cp_config_revision"
	LiveNodes       = "oracache_cp_live_nodes"
)

// Результаты heartbeat и config pull. Набор закрыт: неограниченные значения
// label запрещены.
const (
	HeartbeatAccepted = "accepted"
	HeartbeatInvalid  = "invalid"
	HeartbeatError    = "error"

	PullServed    = "served"
	PullUnchanged = "unchanged"
	PullAhead     = "ahead"
	PullError     = "error"
)

// Состояния нод для oracache_cp_live_nodes.
const (
	NodeStateReady    = "ready"
	NodeStateStarting = "starting"
	NodeStateDead     = "dead"
	NodeStateDraining = "draining"
	NodeStateOther    = "other"
)

type metricMeta struct {
	typ     string
	help    string
	collect func(context.Context) ([]GaugeSample, error)
}

// GaugeSample is one value of a gauge family collected at scrape time.
type GaugeSample struct {
	Labels map[string]string
	Value  float64
}

type series struct {
	name   string
	labels string
}

// Metrics — потокобезопасный реестр метрик.
type Metrics struct {
	prometheus *prometheus.Registry
	mu         sync.Mutex
	metadata   map[string]metricMeta
	counters   map[series]uint64
	gauges     map[series]float64
}

// New создаёт реестр с зарегистрированными метриками Control Plane.
func New() *Metrics {
	m := NewRegistry()
	m.Describe(HeartbeatTotal, "counter", "Heartbeats received by the CP API.")
	m.Describe(ConfigPullTotal, "counter", "Config pulls served by the CP API.")
	m.Describe(ConfigRevision, "gauge", "Current Control Plane configuration revision.")
	m.Describe(LiveNodes, "gauge", "Nodes in the last live config view, by state.")
	return m
}

// NewRegistry creates an empty registry.
func NewRegistry() *Metrics {
	return &Metrics{
		prometheus: prometheus.NewRegistry(),
		metadata:   make(map[string]metricMeta),
		counters:   make(map[series]uint64),
		gauges:     make(map[series]float64),
	}
}

// RegisterGaugeFunc registers a family whose samples are collected on each
// scrape. The collector may be called concurrently by separate scrapes.
func (m *Metrics) RegisterGaugeFunc(name, help string, collect func(context.Context) ([]GaugeSample, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metadata[name] = metricMeta{typ: "gauge", help: help, collect: collect}
}

// Describe регистрирует метрику до первого использования.
func (m *Metrics) Describe(name, typ, help string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.metadata[name]; !ok {
		m.metadata[name] = metricMeta{typ: typ, help: help}
	}
}

// IncCounter увеличивает счётчик на 1.
func (m *Metrics) IncCounter(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := series{name: name, labels: canonicalLabels(labels)}
	m.counters[key]++
}

// AddCounter увеличивает счётчик на delta. Отрицательная delta для счётчика
// запрещена: exposition-формат не допускает убывающих счётчиков.
func (m *Metrics) AddCounter(name string, labels map[string]string, delta uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := series{name: name, labels: canonicalLabels(labels)}
	m.counters[key] += delta
}

// SetGauge задаёт значение одной series gauge.
func (m *Metrics) SetGauge(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := series{name: name, labels: canonicalLabels(labels)}
	m.gauges[key] = value
}

// SetGaugeFamily заменяет все series одной gauge-метрики. Используется для
// gauge с размерами, которые меняются между scrape (состояния нод): иначе
// оставались бы series исчезнувших значений.
func (m *Metrics) SetGaugeFamily(name string, values map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.gauges {
		if key.name == name {
			delete(m.gauges, key)
		}
	}
	for label, value := range values {
		key := series{name: name, labels: canonicalLabels(map[string]string{"state": label})}
		if _, ok := m.metadata[name]; ok {
			m.gauges[key] = value
		}
	}
}

// Heartbeat фиксирует результат приёма heartbeat.
func (m *Metrics) Heartbeat(result string) {
	m.IncCounter(HeartbeatTotal, map[string]string{"result": result})
}

// ConfigPull фиксирует результат config pull.
func (m *Metrics) ConfigPull(result string) {
	m.IncCounter(ConfigPullTotal, map[string]string{"result": result})
}

// SetConfigRevision публикует текущую revision конфигурации.
func (m *Metrics) SetConfigRevision(revision uint64) {
	m.SetGauge(ConfigRevision, nil, float64(revision))
}

// SetLiveNodes публикует число нод по состояниям последнего live-view.
func (m *Metrics) SetLiveNodes(counts map[string]uint64) {
	values := make(map[string]float64, len(counts))
	for state, count := range counts {
		values[state] = float64(count)
	}
	m.SetGaugeFamily(LiveNodes, values)
}

// Handler возвращает HTTP handler для /metrics. Отдаёт GET и HEAD;
// остальные методы получают 405.
func (m *Metrics) Handler() http.Handler { return metricsHandler{metrics: m} }

type metricsHandler struct{ metrics *Metrics }

func (h metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body bytes.Buffer
	if err := h.metrics.render(r.Context(), &body); err != nil {
		http.Error(w, "metrics collection failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if r.Method == http.MethodGet {
		_, _ = body.WriteTo(w)
	}
}

// Render пишет exposition в текстовом формате Prometheus. Вывод
// детерминирован: метрики и series сортируются.
func (m *Metrics) Render(w io.Writer) error {
	return m.render(context.Background(), w)
}

func (m *Metrics) render(ctx context.Context, w io.Writer) error {
	m.mu.Lock()
	metadata := maps.Clone(m.metadata)
	counters := maps.Clone(m.counters)
	gauges := maps.Clone(m.gauges)
	m.mu.Unlock()

	names := make(map[string]struct{}, len(metadata))
	for name, meta := range metadata {
		names[name] = struct{}{}
		if meta.collect == nil {
			continue
		}
		samples, err := meta.collect(ctx)
		if err != nil {
			return fmt.Errorf("collect %s: %w", name, err)
		}
		for _, sample := range samples {
			gauges[series{name: name, labels: canonicalLabels(sample.Labels)}] = sample.Value
		}
	}
	for key := range counters {
		names[key.name] = struct{}{}
	}
	for key := range gauges {
		names[key.name] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	for _, name := range ordered {
		meta, ok := metadata[name]
		if !ok {
			meta = metricMeta{typ: "untyped", help: name}
		}
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, meta.help, name, meta.typ); err != nil {
			return err
		}
		seriesList := make([]series, 0, len(counters)+len(gauges))
		for key := range counters {
			if key.name == name {
				seriesList = append(seriesList, key)
			}
		}
		sort.Slice(seriesList, func(i, j int) bool { return seriesList[i].labels < seriesList[j].labels })
		for _, key := range seriesList {
			if _, err := fmt.Fprintf(w, "%s%s %d\n", name, key.labels, counters[key]); err != nil {
				return err
			}
		}
		seriesList = seriesList[:0]
		for key := range gauges {
			if key.name == name {
				seriesList = append(seriesList, key)
			}
		}
		sort.Slice(seriesList, func(i, j int) bool { return seriesList[i].labels < seriesList[j].labels })
		for _, key := range seriesList {
			if _, err := fmt.Fprintf(w, "%s%s %s\n", name, key.labels, formatFloat(gauges[key])); err != nil {
				return err
			}
		}
	}
	families, err := m.prometheus.Gather()
	if err != nil {
		return fmt.Errorf("gather Prometheus collectors: %w", err)
	}
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(w, family); err != nil {
			return fmt.Errorf("encode metric family %s: %w", family.GetName(), err)
		}
	}
	return nil
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// canonicalLabels возвращает детерминированное представление labels.
func canonicalLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[key]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue экранирует значение label по правилам Prometheus.
func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return value
}
