package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/benchcatalog"
	"google.golang.org/grpc"
)

// Delay returning the PUT acknowledgement until a concurrent GET sees its value.
type delayedClient struct {
	pb.CacheGatewayClient
	data         []byte
	rev          uint64
	visible, ack chan struct{}
}

func (c *delayedClient) Put(_ context.Context, r *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	c.data = r.Data
	close(c.visible)
	<-c.ack
	return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_OK, Rev: c.rev}, nil
}
func (c *delayedClient) Get(_ context.Context, r *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	if r.Key == "missing:product:0" {
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}, nil
	}
	return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: c.rev, LatestRev: c.rev, Data: c.data}, nil
}

func TestCatalogConcurrentRevision(t *testing.T) {
	c := &delayedClient{rev: 24, visible: make(chan struct{}), ack: make(chan struct{})}
	w := workload{client: c, entries: []getEntry{{key: "product:0", rev: 17, data: benchcatalog.Payload(0, 256, 0)}}, timeout: time.Second, floors: make([]atomic.Uint64, 1)}
	done := make(chan sample, 1)
	go func() { done <- w.catalogRPC(100, benchcatalog.Request{Write: true}) }()
	<-c.visible
	read := w.catalogRPC(101, benchcatalog.Request{})
	close(c.ack)
	calls := []sample{read, <-done, w.catalogRPC(102, benchcatalog.Request{Missing: true})}
	w.correlate(calls)
	if calls[0].outcome != "hit" || calls[1].outcome != "updated" || calls[2].outcome != "expected_miss" || summarize(calls, time.Second).success != 3 {
		t.Fatal(calls)
	}
	calls[0].rev++
	w.correlate(calls)
	if calls[0].kind != "unconfirmed_version" {
		t.Fatal(calls)
	}
	c.rev = 23
	if s := w.catalogRPC(103, benchcatalog.Request{}); s.kind != "invalid_response" {
		t.Fatal("stale revision", s)
	}
	c.rev = 24
	c.data[30] ^= 1
	if s := w.catalogRPC(104, benchcatalog.Request{}); s.kind != "invalid_response" {
		t.Fatal("corruption", s)
	}
}
