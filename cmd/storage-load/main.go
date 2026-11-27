// storage-load measures unary gRPC against a disposable OraCache cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const defaultKeys = 256
const maxKeys = 10000
const payloadSize = 10 * 1024

type operation struct {
	write     bool
	key, node int
}

func request(i, leader, n, keys int, rr bool) operation {
	cycle, slot := i/21, i%21
	if slot == 20 {
		return operation{write: true, key: cycle % keys, node: leader}
	}
	readIndex := cycle*20 + slot
	o := operation{key: readIndex % keys, node: leader}
	if rr {
		o.node = readIndex % n
	}
	return o
}

func due(rate int, d time.Duration) int { return int(int64(rate) * int64(d) / int64(time.Second)) }

const lateThreshold = 5 * time.Millisecond

func dropLate(delay time.Duration, catchUp bool) bool { return !catchUp && delay > lateThreshold }

type result struct {
	class    string
	servedBy string
	latency  time.Duration
	late     bool
}
type row struct {
	Case      string         `json:"case"`
	Op        string         `json:"op"`
	TargetRPS float64        `json:"target_rps"`
	SentRPS   float64        `json:"sent_rps"`
	OKRPS     float64        `json:"ok_rps"`
	P50       float64        `json:"p50_ms"`
	P95       float64        `json:"p95_ms"`
	P99       float64        `json:"p99_ms"`
	Outcomes  map[string]int `json:"outcomes"`
	PerNode   map[string]int `json:"sent_per_node"`
	ServedBy  map[string]int `json:"responses_per_instance,omitempty"`
	LateSent  int            `json:"sent_over_5ms_late"`
}

type load struct {
	clients  []pb.CacheGatewayClient
	nodes    []string
	get      []*pb.GetRequest
	put      []*pb.PutRequest
	leader   int
	rr       bool
	timeout  time.Duration
	inFlight int
	catchUp  bool
	metrics  [2]*operationMetrics
}

func (l *load) call(o operation) (class, instance string) {
	ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
	defer cancel()
	c := l.clients[o.node]
	if o.write {
		p, e := c.Put(ctx, l.put[o.key])
		if e != nil {
			return classify(e), ""
		}
		if p.GetStatus() == pb.PutStatus_PUT_STATUS_OK {
			return "ok", p.GetServedBy().GetInstance()
		}
		return p.GetStatus().String(), p.GetServedBy().GetInstance()
	}
	p, e := c.Get(ctx, l.get[o.key])
	if e != nil {
		return classify(e), ""
	}
	if p.GetStatus() != pb.GetStatus_GET_STATUS_HIT {
		return p.GetStatus().String(), p.GetServedBy().GetInstance()
	}
	if len(p.GetData()) != payloadSize {
		return "bad_payload_size", p.GetServedBy().GetInstance()
	}
	return "ok", p.GetServedBy().GetInstance()
}

func classify(e error) string {
	s := status.Convert(e)
	if s.Code() == codes.ResourceExhausted && s.Message() == "too many pending requests" {
		return "ResourceExhausted: too many pending requests"
	}
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Reason == "NOT_LEADER" {
			return "NOT_LEADER"
		}
	}
	return s.Code().String()
}

func (l *load) prepare() error {
	deadline := time.Now().Add(15 * time.Second)
	l.leader = -1
	for l.leader < 0 && time.Now().Before(deadline) {
		for i := range l.clients {
			if c, _ := l.call(operation{write: true, node: i}); c == "ok" {
				l.leader = i
				break
			}
		}
		if l.leader < 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if l.leader < 0 {
		return errors.New("no endpoint accepted seed Put")
	}
	for i := 1; i < len(l.put); i++ {
		if c, _ := l.call(operation{write: true, node: l.leader, key: i}); c != "ok" {
			return fmt.Errorf("prefill key %d: %s", i, c)
		}
	}
	// Check every selected node has applied the tail of the prefill; in strict mode
	// only the leader is asked. This preflight is not part of the latency sample.
	for n := range l.clients {
		if !l.rr && n != l.leader {
			continue
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			c, _ := l.call(operation{node: n, key: len(l.get) - 1})
			if c == "ok" {
				break
			}
			if time.Now().After(deadline) {
				if l.rr {
					return fmt.Errorf("read probe on %s: %s (RR requires -read-mode=local on all nodes)", l.nodes[n], c)
				}
				return fmt.Errorf("read probe on %s: %s", l.nodes[n], c)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return nil
}

func (l *load) step(name string, rate int, d time.Duration, first int) []row {
	out := make([]result, due(rate, d))
	sem := make(chan struct{}, l.inFlight)
	var wg sync.WaitGroup
	start := time.Now()
	cursor := 0
	for elapsed := time.Millisecond; elapsed <= d; elapsed += time.Millisecond {
		planned := start.Add(elapsed)
		if wait := time.Until(planned); wait > 0 {
			time.Sleep(wait)
		}
		end := due(rate, elapsed)
		for ; cursor < end; cursor++ {
			i := cursor
			o := request(first+i, l.leader, len(l.clients), len(l.get), l.rr)
			m := l.metrics[0]
			if o.write {
				m = l.metrics[1]
			}
			if m != nil {
				m.scheduled.Inc()
			}
			delay := time.Since(planned)
			if dropLate(delay, l.catchUp) {
				out[i].class = "scheduler_late"
				if m != nil {
					m.schedulerLate.Inc()
				}
				continue
			}
			select {
			case sem <- struct{}{}:
				late := delay > lateThreshold
				if m != nil {
					m.sent.Inc()
					if late {
						m.sentLate.Inc()
					}
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					c, instance := l.call(o)
					latency := time.Since(planned)
					out[i] = result{class: c, servedBy: instance, latency: latency, late: late}
					if m != nil {
						m.completed.WithLabelValues(c).Inc()
						if c == "ok" {
							m.latency.Observe(latency.Seconds())
						}
					}
				}()
			default:
				out[i].class = "inflight_limit"
				if m != nil {
					m.inflightLimit.Inc()
				}
			}
		}
	}
	wg.Wait()
	rows := []row{{Case: name, Op: "Get"}, {Case: name, Op: "Put"}}
	var times [2][]time.Duration
	for j := range rows {
		rows[j].Outcomes = map[string]int{}
		rows[j].PerNode = map[string]int{}
	}
	for i, r := range out {
		o := request(first+i, l.leader, len(l.clients), len(l.get), l.rr)
		j := 0
		if o.write {
			j = 1
		}
		a := &rows[j]
		a.TargetRPS++
		a.Outcomes[r.class]++
		if r.servedBy != "" {
			if a.ServedBy == nil {
				a.ServedBy = map[string]int{}
			}
			a.ServedBy[r.servedBy]++
		}
		if r.class != "inflight_limit" && r.class != "scheduler_late" {
			a.SentRPS++
			if r.late {
				a.LateSent++
			}
			a.PerNode[l.nodes[o.node]]++
		}
		if r.class == "ok" {
			times[j] = append(times[j], r.latency)
		}
	}
	for j := range rows {
		a := &rows[j]
		a.TargetRPS /= d.Seconds()
		a.SentRPS /= d.Seconds()
		a.OKRPS = float64(a.Outcomes["ok"]) / d.Seconds()
		s := times[j]
		slices.Sort(s)
		if len(s) > 0 {
			q := func(p int) float64 { return float64(s[(p*len(s)+99)/100-1]) / float64(time.Millisecond) }
			a.P50 = q(50)
			a.P95 = q(95)
			a.P99 = q(99)
		}
	}
	return rows
}

func run() error {
	addresses := flag.String("nodes", "", "comma-separated storage gRPC endpoints (disposable cluster only)")
	gateway := flag.String("gateway", "", "single API Gateway gRPC endpoint (disposable cluster only; excludes -nodes and -case)")
	mode := flag.String("case", "", "direct storage mode: leader-linearizable, leader-local (default), or rr-local")
	keys := flag.Int("keys", defaultKeys, fmt.Sprintf("number of keys to prefill and exercise (1..%d)", maxKeys))
	steps := flag.String("read-rps", "1000,1000,1000", "comma-separated Get RPS steps, multiples of 20; Put RPS is 1/20 of Get")
	duration := flag.Duration("step", 20*time.Second, "duration per step (whole seconds)")
	timeout := flag.Duration("timeout", time.Second, "per-RPC timeout")
	inflight := flag.Int("inflight", 2048, "max concurrent RPCs; excess is counted, not queued")
	catchUp := flag.Bool("catch-up", false, "launch overdue slots immediately, still subject to the inflight cap")
	metricsAddr := flag.String("metrics-listen", "", "optional Prometheus HTTP listen address (e.g. 127.0.0.1:9091)")
	flag.Parse()
	if *keys < 1 || *keys > maxKeys {
		return fmt.Errorf("-keys must be in 1..%d", maxKeys)
	}
	if *duration < time.Second || *duration > time.Minute || *duration%time.Second != 0 || *timeout <= 0 || *inflight < 1 {
		return errors.New("invalid flags; see -help")
	}
	if *gateway != "" {
		if *addresses != "" || *mode != "" || strings.Contains(*gateway, ",") {
			return errors.New("-gateway requires one endpoint and cannot be combined with -nodes or -case")
		}
		*addresses, *mode = *gateway, "gateway"
	} else {
		if *mode == "" {
			*mode = "leader-local"
		}
		if *addresses == "" || (*mode != "leader-local" && *mode != "leader-linearizable" && *mode != "rr-local") {
			return errors.New("specify -gateway or -nodes with a valid storage case; see -help")
		}
	}
	var rates []int
	for _, s := range strings.Split(*steps, ",") {
		n, e := strconv.Atoi(s)
		if e != nil || n < 20 || n > 10000 || n%20 != 0 {
			return fmt.Errorf("bad read-rps %q (multiples of 20, range 20..10000)", s)
		}
		rates = append(rates, n+n/20)
	}
	l := load{nodes: strings.Split(*addresses, ","), rr: *mode == "rr-local", timeout: *timeout, inFlight: *inflight, catchUp: *catchUp}
	seen := map[string]bool{}
	for _, addr := range l.nodes {
		if addr == "" || seen[addr] {
			return fmt.Errorf("empty or duplicate endpoint %q", addr)
		}
		seen[addr] = true
		c, e := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
		if e != nil {
			return e
		}
		defer c.Close()
		l.clients = append(l.clients, pb.NewCacheGatewayClient(c))
	}
	if l.rr && len(l.nodes) < 2 {
		return errors.New("rr-local needs at least two nodes")
	}
	payload := make([]byte, payloadSize)
	_, _ = rand.New(rand.NewSource(42)).Read(payload)
	prefix := fmt.Sprintf("storage-load:%d:", time.Now().UnixNano())
	for i := 0; i < *keys; i++ {
		key := prefix + strconv.Itoa(i)
		l.get = append(l.get, &pb.GetRequest{Key: key})
		l.put = append(l.put, &pb.PutRequest{Key: key, Data: payload})
	}
	if e := l.prepare(); e != nil {
		return e
	}
	log.Printf("case=%s write_endpoint=%s seed_keys=%d", *mode, l.nodes[l.leader], len(l.get))
	_ = l.step(*mode, rates[0], time.Second, 0)
	if *metricsAddr != "" {
		metrics, closeMetrics, err := serveMetrics(*metricsAddr)
		if err != nil {
			return fmt.Errorf("metrics listener: %w", err)
		}
		defer closeMetrics()
		l.metrics = metrics
	}
	enc := json.NewEncoder(os.Stdout)
	first := 0
	for _, r := range rates {
		for _, line := range l.step(*mode, r, *duration, first) {
			if e := enc.Encode(line); e != nil {
				return e
			}
		}
		first += due(r, *duration)
	}
	return nil
}

func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
