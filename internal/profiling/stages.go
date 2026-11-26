// Package profiling records wall-clock intervals only in opted-in server processes.
package profiling

import (
	"math/bits"
	"sync/atomic"
	"time"
)

type Stage int

const (
	Decode Stage = iota
	Encode
	Handler
	Prepare
	Handoff
	Command
	Propose
	Commit
	Apply
	ReadBarrier
	RequestDecode
	Execute
	StoreLock
	StoreBody
	Clone
	LRU
	Retention
	Checkpoint
	Route
	Upstream
	Receive
	Send
	Unary
	stageCount
)

var names = [stageCount]string{"grpc.decode", "grpc.encode", "grpc.handler", "raft.prepare", "raft.handoff", "raft.encode_command", "raft.propose", "raft.commit_observed", "raft.apply", "raft.read_barrier", "raft.decode_request", "raft.execute", "store.lock", "store.body", "store.clone", "store.lru", "store.retention_evict", "raft.checkpoint", "gateway.route_order", "gateway.upstream_rpc", "grpc.receive_decode", "grpc.respond", "grpc.unary"}

// Eight buckets per power of two. Counters have fixed storage regardless of load.
type histogram struct {
	buckets [512]atomic.Uint64
	sum     atomic.Uint64
}

type window [stageCount]histogram

var current atomic.Pointer[window]

func Start() time.Time {
	if current.Load() == nil {
		return time.Time{}
	}
	return time.Now()
}

func (s Stage) End(start time.Time) {
	if start.IsZero() {
		return
	}
	w := current.Load()
	if w == nil {
		return
	}
	ns := uint64(time.Since(start))
	h := &w[s]
	h.sum.Add(ns)
	h.buckets[bucket(ns)].Add(1)
}

func bucket(ns uint64) int {
	if ns < 8 {
		return int(ns)
	}
	shift := bits.Len64(ns) - 4
	return 8*shift + int(ns>>shift)
}

func upper(i int) uint64 {
	if i < 8 {
		return uint64(i)
	}
	shift := i/8 - 1
	return (uint64(9+i%8) << shift) - 1
}

type summary struct {
	Count uint64  `json:"count"`
	Mean  float64 `json:"mean_ns"`
	P50   uint64  `json:"p50_ns"`
	P95   uint64  `json:"p95_ns"`
	P99   uint64  `json:"p99_ns"`
}

func (h *histogram) summary() summary {
	var cumulative [512]uint64
	var count uint64
	for i := range h.buckets {
		count += h.buckets[i].Load()
		cumulative[i] = count
	}
	if count == 0 {
		return summary{}
	}
	quantile := func(percent uint64) uint64 {
		for i, n := range cumulative {
			if n >= (count*percent+99)/100 {
				return upper(i)
			}
		}
		return 0
	}
	return summary{count, float64(h.sum.Load()) / float64(count), quantile(50), quantile(95), quantile(99)}
}
