package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/grpc"
)

// Record calls at the same generated client boundary used by the workers.
type benchClient struct {
	pb.CacheGatewayClient
	values     map[string]*pb.GetResponse
	puts, gets int
	fail       error
}

func (c *benchClient) Put(_ context.Context, r *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	c.puts++
	if c.fail != nil {
		return nil, c.fail
	}
	rev := uint64(c.puts)
	c.values[r.Key] = &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: rev, Data: bytes.Clone(r.Data)}
	return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_OK, Rev: rev}, nil
}

func (c *benchClient) Get(_ context.Context, r *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	c.gets++
	if c.fail != nil {
		return nil, c.fail
	}
	if value, ok := c.values[r.Key]; ok {
		return value, nil
	}
	return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}, nil
}

func TestGETWorkingSet(t *testing.T) {
	for _, size := range []int{1, 2048} {
		c := &benchClient{values: make(map[string]*pb.GetResponse)}
		order, err := keyOrder(size, 17, "")
		if err != nil {
			t.Fatal(err)
		}
		w := workload{client: c, mode: "get", timeout: time.Second, entries: newEntries(size, 10240), order: order}
		if err := w.seed(); err != nil {
			t.Fatal(err)
		}
		if len(c.values) != size || c.puts != size {
			t.Fatalf("seeded %d keys with %d PUTs", len(c.values), c.puts)
		}
		for _, v := range c.values {
			if len(v.Data) != 10240 {
				t.Fatal("incorrect seed payload size")
			}
		}
		if err := w.warm(size); err != nil {
			t.Fatal(err)
		}
		counts := make([]int, size)
		for i := 0; i < 2*size; i++ {
			s := w.measured(uint64(size+1+i), uint64(i))
			if s.err != "" {
				t.Fatal(s)
			}
			counts[s.keyIndex]++
		}
		for i, count := range counts {
			if count != 2 {
				t.Fatalf("key %d received %d GETs", i, count)
			}
		}
		if c.gets != size*3 || c.puts != size {
			t.Fatalf("after reads: %d GET, %d PUT", c.gets, c.puts)
		}
		if w.entries[0].key != "oracache:benchmark:get" || !bytes.Equal(w.entries[0].data, bytes.Repeat([]byte{'x'}, 10240)) {
			t.Fatal("legacy GET key or value changed")
		}
	}
}

func TestKeyOrder(t *testing.T) {
	a, _ := keyOrder(2048, 71, "")
	b, _ := keyOrder(2048, 71, "")
	c, _ := keyOrder(2048, 72, "")
	if !slices.Equal(a, b) || slices.Equal(a, c) {
		t.Fatal("seed mismatch")
	}
	path := filepath.Join(t.TempDir(), "order")
	for _, input := range []string{"", "2\n", "-1\n", "bad\n", "1\n1\n0\n"} {
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		order, err := keyOrder(2, 1, path)
		if input == "1\n1\n0\n" {
			if err != nil || !slices.Equal(order, []int{1, 1, 0}) {
				t.Fatal(order, err)
			}
		} else if err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestGETValidation(t *testing.T) {
	c := &benchClient{values: make(map[string]*pb.GetResponse)}
	w := workload{client: c, mode: "get", entries: newEntries(1, 10240), order: []int{0}, timeout: time.Second}
	index := w.entries[0].key
	if s := w.measured(1, 0); s.kind != "cache_miss" {
		t.Fatal(s)
	}
	if err := w.warm(1); err == nil {
		t.Fatal("miss passed warmup")
	}
	if err := w.seed(); err != nil {
		t.Fatal(err)
	}
	c.values[index].Data[9000]++
	if s := w.measured(1, 0); s.kind != "invalid_response" {
		t.Fatal("same length, same revision, corrupt bytes", s)
	}
	c.values[index].Data[9000]--
	c.values[index].Rev++
	if s := w.measured(1, 0); s.kind != "invalid_response" {
		t.Fatal("wrong rev", s)
	}
	c.fail = errors.New("unavailable")
	if s := w.measured(1, 0); s.kind != "rpc_error" {
		t.Fatal(s)
	}
	if err := w.seed(); err == nil {
		t.Fatal("seed ignored PUT failure")
	}
}

func TestLegacyPUTKeys(t *testing.T) {
	for _, limit := range []int{0, 2} {
		c := &benchClient{values: make(map[string]*pb.GetResponse)}
		w := workload{client: c, mode: "put", entries: newEntries(1, 20), order: []int{0}, timeout: time.Second, putKeys: limit}
		if err := w.seed(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if s := w.measured(uint64(i+1), uint64(i)); s.err != "" {
				t.Fatal(s)
			}
		}
		want := 4
		if limit == 2 {
			want = 3
		}
		if len(c.values) != want || c.puts != 4 || c.gets != 0 {
			t.Fatal(limit, c)
		}
	}
}

func TestStatistics(t *testing.T) {
	calls := []sample{{err: "miss", kind: "cache_miss", latency: time.Hour}, {err: "transport", kind: "rpc_error"}}
	for i := 100; i > 0; i-- {
		calls = append(calls, sample{latency: time.Duration(i) * time.Millisecond})
	}
	s := summarize(calls, 2*time.Second)
	if s != (statistics{success: 100, errors: 2, misses: 1, actualRPS: 51, successRPS: 50, mean: 50.5, p50: 50, p95: 95, p99: 99, max: 100}) {
		t.Fatal(s)
	}
}
