// OraCache gRPC latency microbenchmark.
// Run from the root of the OraCache repository:
//
//	go run /path/to/oracache_bench.go -endpoint=127.0.0.1:28080 -mode=get -rps=500 -workers=32 -seconds=15
//
// It reuses a single gRPC connection and excludes Go compilation and client startup time.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gatewayv1 "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/benchcatalog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type sample struct {
	id          uint64
	latency     time.Duration
	err         string
	kind        string
	keyIndex    int
	op, outcome string
	missing     bool
	rev, token  uint64
	size        int
}

type getEntry struct {
	key  string
	data []byte
	rev  uint64
}

type workload struct {
	client  gatewayv1.CacheGatewayClient
	mode    string
	timeout time.Duration
	putKeys int
	entries []getEntry
	order   []int
	catalog *benchcatalog.Plan
	floors  []atomic.Uint64
}

func keyOrder(keys int, seed int64, replay string) ([]int, error) {
	if replay == "" {
		return rand.New(rand.NewSource(seed)).Perm(keys), nil
	}
	f, err := os.Open(replay)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var order []int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		i, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
		if err != nil || i < 0 || i >= keys {
			return nil, fmt.Errorf("sequence line %d: expected index 0..%d", len(order)+1, keys-1)
		}
		order = append(order, i)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(order) == 0 {
		return nil, fmt.Errorf("empty key sequence")
	}
	return order, nil
}

func newEntries(keys int, size int) []getEntry {
	entries := make([]getEntry, keys)
	for i := range entries {
		key := "oracache:benchmark:get"
		value := bytes.Repeat([]byte{'x'}, size)
		if i > 0 {
			key += ":" + strconv.Itoa(i)
			var tag [8]byte
			binary.LittleEndian.PutUint64(tag[:], uint64(i))
			copy(value, tag[:])
		}
		entries[i] = getEntry{key: key, data: value}
	}
	return entries
}

func (w *workload) seed() error {
	for i := range w.entries {
		e := &w.entries[i]
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		resp, err := w.client.Put(ctx, &gatewayv1.PutRequest{Key: e.key, Data: e.data})
		cancel()
		if err != nil {
			return fmt.Errorf("seed PUT key=%q (%d/%d completed): %w", e.key, i, len(w.entries), err)
		}
		if resp.GetStatus() != gatewayv1.PutStatus_PUT_STATUS_OK || resp.GetRev() == 0 {
			return fmt.Errorf("seed PUT key=%q status=%s rev=%d", e.key, resp.GetStatus(), resp.GetRev())
		}
		e.rev = resp.Rev
	}
	return nil
}

func (w *workload) rpc(id uint64, index int) sample {
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	s := sample{id: id, keyIndex: index}
	var err error
	var response *gatewayv1.GetResponse
	entry := &w.entries[index]
	start := time.Now()
	if w.mode != "put" {
		response, err = w.client.Get(ctx, &gatewayv1.GetRequest{Key: entry.key})
		if err == nil && (response.GetStatus() != gatewayv1.GetStatus_GET_STATUS_HIT || len(response.GetData()) != len(entry.data) || response.GetRev() != entry.rev) {
			err = fmt.Errorf("GET status %s, rev %d, bytes %d", response.GetStatus(), response.GetRev(), len(response.GetData()))
			if response.GetStatus() == gatewayv1.GetStatus_GET_STATUS_MISS {
				s.kind = "cache_miss"
			} else {
				s.kind = "invalid_response"
			}
		}
	} else {
		keyID := id
		if w.putKeys > 0 {
			keyID = (id - 1) % uint64(w.putKeys)
		}
		key := fmt.Sprintf("oracache:benchmark:put:%d", keyID)
		var resp *gatewayv1.PutResponse
		resp, err = w.client.Put(ctx, &gatewayv1.PutRequest{Key: key, Data: entry.data})
		if err == nil && resp.GetStatus() != gatewayv1.PutStatus_PUT_STATUS_OK {
			err = fmt.Errorf("PUT status %s", resp.GetStatus())
			s.kind = "invalid_response"
		}
	}
	s.latency = time.Since(start)
	cancel()
	// Full validation is outside the legacy RPC latency interval.
	if w.mode != "put" && err == nil && !bytes.Equal(response.Data, entry.data) {
		err = fmt.Errorf("GET payload mismatch for %q", entry.key)
		s.kind = "invalid_response"
	}
	if err != nil {
		s.err = err.Error()
		if s.kind == "" {
			s.kind = "rpc_error"
		}
	}
	return s
}

func (w *workload) warm(count int) error {
	for i := 0; i < count; i++ {
		if s := w.rpc(uint64(i+1), i%len(w.entries)); s.err != "" {
			return fmt.Errorf("warmup %s key=%q after %d calls: %s", w.mode, w.entries[s.keyIndex].key, i, s.err)
		}
	}
	return nil
}

func (w *workload) measured(id, position uint64) sample {
	if w.catalog != nil {
		return w.catalogRPC(id, w.catalog.At(position))
	}
	index := w.order[position%uint64(len(w.order))]
	return w.rpc(id, index)
}

// Capture latency across the RPC; verify bytes and correlate acknowledgements outside it.
func (w *workload) catalogRPC(id uint64, r benchcatalog.Request) sample {
	e := &w.entries[r.Key]
	s := sample{id: id, keyIndex: r.Key, missing: r.Missing, op: "get"}
	key := e.key
	if r.Missing {
		key = "missing:" + key
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	if r.Write {
		s.op = "put"
		data := benchcatalog.Payload(r.Key, len(e.data), id)
		start := time.Now()
		resp, err := w.client.Put(ctx, &gatewayv1.PutRequest{Key: key, Data: data})
		s.latency = time.Since(start)
		s.size = len(data)
		switch {
		case err != nil:
			s.kind, s.err = "rpc_error", err.Error()
		case resp.GetStatus() != gatewayv1.PutStatus_PUT_STATUS_OK || resp.GetRev() <= e.rev:
			s.kind, s.err = "invalid_response", fmt.Sprintf("PUT status %s rev %d", resp.GetStatus(), resp.GetRev())
		default:
			s.rev, s.token, s.outcome = resp.Rev, id, "updated"
			floor := &w.floors[r.Key]
			for old := floor.Load(); old < s.rev && !floor.CompareAndSwap(old, s.rev); old = floor.Load() {
			}
		}
		return s
	}
	floor := max(e.rev, w.floors[r.Key].Load())
	start := time.Now()
	resp, err := w.client.Get(ctx, &gatewayv1.GetRequest{Key: key})
	s.latency = time.Since(start)
	if err != nil {
		s.kind, s.err = "rpc_error", err.Error()
		return s
	}
	s.rev, s.size = resp.GetRev(), len(resp.GetData())
	if resp.GetStatus() == gatewayv1.GetStatus_GET_STATUS_MISS {
		if r.Missing && s.rev == 0 && resp.GetLatestRev() == 0 && s.size == 0 {
			s.outcome = "expected_miss"
		} else {
			s.kind, s.err = "cache_miss", "unexpected or malformed MISS"
		}
		return s
	}
	token, valid := benchcatalog.CheckPayload(r.Key, e.data, resp.GetData())
	if r.Missing || resp.GetStatus() != gatewayv1.GetStatus_GET_STATUS_HIT || s.rev < floor || s.rev != resp.GetLatestRev() || !valid {
		s.kind, s.err = "invalid_response", "invalid product, revision, or GET status"
		return s
	}
	s.outcome, s.token = "hit", token
	return s
}

// A GET may return before the PUT acknowledgement reaches another client worker.
// Resolve token -> (product, assigned Raft revision) after all workers have joined.
func (w *workload) correlate(all []sample) {
	type version struct {
		key int
		rev uint64
	}
	writes := make(map[uint64]version)
	for _, s := range all {
		if s.outcome == "updated" {
			writes[s.token] = version{s.keyIndex, s.rev}
		}
	}
	for i, s := range all {
		if s.outcome != "hit" {
			continue
		}
		want := version{s.keyIndex, w.entries[s.keyIndex].rev}
		if s.token != 0 {
			want = writes[s.token]
		}
		if want != (version{s.keyIndex, s.rev}) {
			all[i].err, all[i].kind, all[i].outcome = "GET revision does not match acknowledged write", "unconfirmed_version", ""
		}
	}
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:28080", "gRPC endpoint of gateway or current leader")
	mode := flag.String("mode", "get", "workload: get, put, or catalog")
	seconds := flag.Int("seconds", 15, "measurement duration in seconds")
	workers := flag.Int("workers", 32, "number of concurrent clients")
	rps := flag.Int("rps", 500, "maximum scheduled operations/sec; 0 = unlimited")
	payloadSize := flag.Int("bytes", 10240, "value length in bytes")
	warmup := flag.Int("warmup", 100, "warmup operations (excluded from measurements)")
	timeout := flag.Duration("timeout", 3*time.Second, "timeout for a single RPC")
	phase := flag.String("phase", "baseline", "label for results (e.g. baseline or checkpoint)")
	csvFile := flag.String("csv", "oracache-latency.csv", "path of CSV with individual RPC latencies")
	keys := flag.Int("put-keys", 0, "cycle unconditional writes across this many keys (0: new key per write)")
	getKeys := flag.Int("get-keys", 1, "prefilled keys (default 1 for get and 2048 for catalog)")
	seed := flag.Int64("seed", 1, "seed for the repeated GET permutation")
	replay := flag.String("key-sequence", "", "repeat a file of zero-based GET key indices")
	gate := flag.Bool("wait-for-start", false, "after warmup print READY and wait for a newline on stdin")
	var cat benchcatalog.Config
	flag.StringVar(&cat.Distribution, "distribution", "uniform", "uniform, hotspot, zipf")
	flag.Float64Var(&cat.GetPercent, "get-percent", 100, "percentage GET")
	flag.Float64Var(&cat.MissRate, "miss-rate", 0, "percentage of GET targeting absent products")
	flag.Float64Var(&cat.HotTraffic, "hot-traffic", 80, "percent of requests for hot products")
	flag.Float64Var(&cat.HotKeys, "hot-keys", 20, "percent of products in the hot set")
	flag.Float64Var(&cat.ZipfS, "zipf-s", 1.1, "finite Zipf exponent")
	flag.StringVar(&cat.Sizes, "sizes", "10240:100", "comma-separated size:percent")
	flag.Parse()
	if *mode == "catalog" {
		explicitKeys := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "get-keys" {
				explicitKeys = true
			}
		})
		if !explicitKeys {
			*getKeys = 2048
		}
		if *replay != "" {
			fatal(fmt.Errorf("key-sequence requires get mode"))
		}
	}

	if (*mode != "get" && *mode != "put" && *mode != "catalog") || *seconds < 1 || *workers < 1 || *rps < 0 || *rps > 100000 || *payloadSize < 1 || *payloadSize > 10240 || *timeout <= 0 || *keys < 0 || *getKeys < 1 || *warmup < 0 {
		fmt.Fprintln(os.Stderr, "invalid flags: mode=get|put|catalog, seconds/workers>=1, rps=0..100000, bytes=1..10240, warmup>=0")
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	client := gatewayv1.NewCacheGatewayClient(conn)
	count := 1
	warmCount := *warmup
	algorithm := "put-id"
	order := []int{0}
	if *mode == "get" {
		count = *getKeys
		warmCount = max(warmCount, count)
		order, err = keyOrder(count, *seed, *replay)
		if err != nil {
			fatal(err)
		}
		algorithm = "go-math-rand-perm-cycle-v1"
		if *replay != "" {
			algorithm = "index-file-cycle-v1"
		}
	}
	w := workload{client: client, mode: *mode, timeout: *timeout, putKeys: *keys,
		entries: newEntries(count, *payloadSize), order: order}
	if *mode == "catalog" {
		cat.Keys, cat.Seed = *getKeys, *seed
		w.catalog, err = benchcatalog.New(cat)
		if err != nil {
			fatal(err)
		}
		warmCount = max(*warmup, cat.Keys)
		w.entries = make([]getEntry, cat.Keys)
		w.floors = make([]atomic.Uint64, cat.Keys)
		for i := range w.entries {
			w.entries[i] = getEntry{key: fmt.Sprintf("product:%d", i), data: benchcatalog.Payload(i, w.catalog.Size(i), 0)}
		}
	}
	if w.catalog != nil {
		f, err := os.Create(filepath.Join(filepath.Dir(*csvFile), "products.csv"))
		if err != nil {
			fatal(err)
		}
		writer := csv.NewWriter(f)
		_ = writer.Write([]string{"index", "key", "size", "hot"})
		for i, e := range w.entries {
			_ = writer.Write([]string{strconv.Itoa(i), e.key, strconv.Itoa(len(e.data)), strconv.FormatBool(w.catalog.Hot(i))})
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			fatal(err)
		}
		if err := f.Close(); err != nil {
			fatal(err)
		}
	}
	if err := w.seed(); err != nil {
		fatal(err)
	}
	warmStart := time.Now()
	if err := w.warm(warmCount); err != nil {
		fatal(err)
	}
	warmElapsed := time.Since(warmStart)
	var sequence atomic.Uint64
	sequence.Store(uint64(warmCount))

	if *gate {
		fmt.Println("READY")
		var b [1]byte
		if _, err := os.Stdin.Read(b[:]); err != nil || b[0] != '\n' {
			fatal(fmt.Errorf("expected start signal"))
		}
	}
	var mu sync.Mutex
	all := make([]sample, 0, *workers*1000)
	var ticker *time.Ticker
	if *rps > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*rps))
		defer ticker.Stop()
	}
	began := time.Now()
	deadline := began.Add(time.Duration(*seconds) * time.Second)
	runCtx, stopRun := context.WithDeadline(context.Background(), deadline)
	defer stopRun()
	var wg sync.WaitGroup
	wg.Add(*workers)
	for range *workers {
		go func() {
			defer wg.Done()
			local := make([]sample, 0, 1024)
			for {
				if time.Now().After(deadline) {
					break
				}
				if ticker != nil {
					select {
					case <-ticker.C:
					case <-runCtx.Done():
						mu.Lock()
						all = append(all, local...)
						mu.Unlock()
						return
					}
				}
				if time.Now().After(deadline) {
					break
				}
				id := sequence.Add(1)
				local = append(local, w.measured(id, id-uint64(warmCount)-1))
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(began)
	sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })
	if w.catalog != nil {
		w.correlate(all)
	}
	if err := writeCSV(*csvFile, *phase, *mode, all); err != nil {
		fatal(err)
	}

	stats := summarize(all, elapsed)
	if w.catalog != nil {
		slots := (*seconds) * (*rps)
		fmt.Printf("elapsed_ns=%d theoretical_slots=%d unsent_slots=%d\n", elapsed.Nanoseconds(), slots, max(0, slots-len(all)))
	}
	if w.catalog != nil {
		fmt.Printf("endpoint=%s mode=catalog phase=%s sizes=%s workers=%d rps_limit=%d duration=%.2fs\n", *endpoint, *phase, cat.Sizes, *workers, *rps, elapsed.Seconds())
		var expected int
		for _, s := range all {
			if s.outcome == "expected_miss" {
				expected++
			}
		}
		fmt.Printf("total=%d success=%d errors=%d actual_RPS=%.1f actual_success_RPS=%.1f expected_miss=%d unexpected_miss=%d\n", len(all), stats.success, stats.errors, stats.actualRPS, stats.successRPS, expected, stats.misses)
	} else {
		fmt.Printf("endpoint=%s mode=%s phase=%s bytes=%d workers=%d rps_limit=%d duration=%.2fs\n", *endpoint, *mode, *phase, *payloadSize, *workers, *rps, elapsed.Seconds())
		fmt.Printf("total=%d success=%d errors=%d actual_RPS=%.1f actual_success_RPS=%.1f cache_miss=%d other_errors=%d\n", len(all), stats.success, stats.errors, stats.actualRPS, stats.successRPS, stats.misses, stats.errors-stats.misses)
	}
	if stats.success > 0 {
		fmt.Printf("latency_ms mean=%.3f p50=%.3f p95=%.3f p99=%.3f max=%.3f\n", stats.mean, stats.p50, stats.p95, stats.p99, stats.max)
	}
	if *mode == "get" {
		seen := make([]bool, count)
		distinct := 0
		for _, s := range all {
			if !seen[s.keyIndex] {
				distinct++
				seen[s.keyIndex] = true
			}
		}
		fmt.Printf("keys=%d measured_distinct=%d seed=%d key_algorithm=%s seed_puts=%d\n", count, distinct, *seed, algorithm, count)
	}
	fmt.Printf("warmup_calls=%d warmup_seconds=%.6f\n", warmCount, warmElapsed.Seconds())
	if stats.errors > 0 {
		fmt.Println("WARNING: percentile values exclude failed requests; inspect CSV for errors")
	}
	fmt.Printf("raw CSV: %s\n", *csvFile)
}

type statistics struct {
	success, errors, misses  int
	actualRPS, successRPS    float64
	mean, p50, p95, p99, max float64
}

func summarize(all []sample, elapsed time.Duration) statistics {
	var s statistics
	durations := make([]float64, 0, len(all))
	for _, call := range all {
		if call.err != "" {
			s.errors++
			if call.kind == "cache_miss" {
				s.misses++
			}
			continue
		}
		durations = append(durations, float64(call.latency)/float64(time.Millisecond))
	}
	s.success = len(durations)
	s.actualRPS = float64(len(all)) / elapsed.Seconds()
	s.successRPS = float64(s.success) / elapsed.Seconds()
	if s.success > 0 {
		sort.Float64s(durations)
		for _, d := range durations {
			s.mean += d
		}
		s.mean /= float64(s.success)
		s.p50, s.p95, s.p99 = percentile(durations, .5), percentile(durations, .95), percentile(durations, .99)
		s.max = durations[len(durations)-1]
	}
	return s
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func writeCSV(path, phase, mode string, samples []sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	header := []string{"phase", "operation", "request_id", "latency_ms", "success", "error", "error_kind", "key_index"}
	if mode == "catalog" {
		header = append(header, "absent_key", "outcome", "payload_bytes", "revision", "write_token")
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, s := range samples {
		ok := strconv.FormatBool(s.err == "")
		errMsg := strings.ReplaceAll(s.err, "\n", " ")
		index := ""
		if mode != "put" {
			index = strconv.Itoa(s.keyIndex)
		}
		op := mode
		if mode == "catalog" {
			op = s.op
		}
		row := []string{phase, op, strconv.FormatUint(s.id, 10), fmt.Sprintf("%.6f", s.latency.Seconds()*1000), ok, errMsg, s.kind, index}
		if mode == "catalog" {
			row = append(row, strconv.FormatBool(s.missing), s.outcome, strconv.Itoa(s.size), strconv.FormatUint(s.rev, 10), strconv.FormatUint(s.token, 10))
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
