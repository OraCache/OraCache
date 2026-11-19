// Package cpmetrics реализует минимальный Prometheus text-exposition реестр
// для метрик Control Plane.
//
// Реестр намеренно не тянет внешних зависимостей: экспонируемые метрики CP —
// несколько счётчиков и gauges, а формат exposition стабилен и прост. Если
// позже понадобятся гистограммы или pull-клиенты, реестр заменяется на
// prometheus/client_golang без изменения вызывающего кода.
package cpmetrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Метрики Control Plane. Имена фиксируются: меняются только через отдельное
// изменение контракта наблюдаемости.
const (
	HeartbeatTotal  = "oracache_cp_heartbeat_total"
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
	typ  string
	help string
}

type series struct {
	name   string
	labels string
}

// Metrics — потокобезопасный реестр метрик CP.
type Metrics struct {
	mu       sync.Mutex
	metadata map[string]metricMeta
	counters map[series]uint64
	gauges   map[series]float64
}

// New создаёт реестр с зарегистрированными метриками Control Plane.
func New() *Metrics {
	return &Metrics{
		metadata: map[string]metricMeta{
			HeartbeatTotal:  {typ: "counter", help: "Heartbeats received by the CP API."},
			ConfigPullTotal: {typ: "counter", help: "Config pulls served by the CP API."},
			ConfigRevision:  {typ: "gauge", help: "Current Control Plane configuration revision."},
			LiveNodes:       {typ: "gauge", help: "Nodes in the last live config view, by state."},
		},
		counters: make(map[series]uint64),
		gauges:   make(map[series]float64),
	}
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
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = h.metrics.Render(w)
}

// Render пишет exposition в текстовом формате Prometheus. Вывод
// детерминирован: метрики и series сортируются.
func (m *Metrics) Render(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make(map[string]struct{}, len(m.metadata))
	for name := range m.metadata {
		names[name] = struct{}{}
	}
	for key := range m.counters {
		names[key.name] = struct{}{}
	}
	for key := range m.gauges {
		names[key.name] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	for _, name := range ordered {
		meta, ok := m.metadata[name]
		if !ok {
			meta = metricMeta{typ: "untyped", help: name}
		}
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, meta.help, name, meta.typ); err != nil {
			return err
		}
		seriesList := make([]series, 0, len(m.counters)+len(m.gauges))
		for key := range m.counters {
			if key.name == name {
				seriesList = append(seriesList, key)
			}
		}
		sort.Slice(seriesList, func(i, j int) bool { return seriesList[i].labels < seriesList[j].labels })
		for _, key := range seriesList {
			if _, err := fmt.Fprintf(w, "%s%s %d\n", name, key.labels, m.counters[key]); err != nil {
				return err
			}
		}
		seriesList = seriesList[:0]
		for key := range m.gauges {
			if key.name == name {
				seriesList = append(seriesList, key)
			}
		}
		sort.Slice(seriesList, func(i, j int) bool { return seriesList[i].labels < seriesList[j].labels })
		for _, key := range seriesList {
			if _, err := fmt.Fprintf(w, "%s%s %s\n", name, key.labels, formatFloat(m.gauges[key])); err != nil {
				return err
			}
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
