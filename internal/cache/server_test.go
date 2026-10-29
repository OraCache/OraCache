package cache

import (
	"context"
	"sync"
	"testing"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func ptr(v uint64) *uint64 { return &v }
func TestVersionsAndConditions(t *testing.T) {
	s := New()
	get := func(rev uint64) *pb.GetResponse {
		t.Helper()
		r, e := s.Get(ctx, &pb.GetRequest{Key: "k", Rev: rev})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	put := func(expected *uint64) *pb.PutResponse {
		t.Helper()
		r, e := s.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("payload"), ExpectedRev: expected})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	if get(42).Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("absent key must MISS")
	}
	if r := put(ptr(0)); r.Rev != 1 || r.Status != pb.PutStatus_PUT_STATUS_OK {
		t.Fatal(r)
	}
	if r := put(ptr(0)); r.Status != pb.PutStatus_PUT_STATUS_CONFLICT || r.Rev != 1 {
		t.Fatal(r)
	}
	for i := uint64(2); i <= 5; i++ {
		if r := put(ptr(i - 1)); r.Rev != i {
			t.Fatal(r)
		}
	}
	if r := get(1); r.Status != pb.GetStatus_GET_STATUS_HIT || r.LatestRev != 5 || string(r.Data) != "payload" || r.ExpiresAt != nil || r.HardExpiresAt != nil {
		t.Fatal(r)
	}
	if r := get(99); r.Status != pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND || r.LatestRev != 5 {
		t.Fatal(r)
	}
	r, e := s.Delete(ctx, &pb.DeleteRequest{Key: "k", Rev: 5})
	if e != nil || r.Status != pb.DeleteStatus_DELETE_STATUS_DELETED {
		t.Fatal(r, e)
	}
	if get(0).Rev != 4 {
		t.Fatal("latest must resolve to highest remaining revision")
	}
	if r := put(ptr(4)); r.Rev != 6 {
		t.Fatal("must not reuse deleted revision", r)
	}
	s.Delete(ctx, &pb.DeleteRequest{Key: "k"})
	if get(6).Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("empty key must MISS")
	}
	if r := put(ptr(0)); r.Rev != 7 {
		t.Fatal("delete all must not reset sequence", r)
	}
}
func TestBoundariesAndCopies(t *testing.T) {
	s := New()
	data := make([]byte, 10*1024)
	r, e := s.Put(ctx, &pb.PutRequest{Key: "k", Data: data})
	if e != nil || r.Status != pb.PutStatus_PUT_STATUS_OK {
		t.Fatal(r, e)
	}
	data[0] = 1
	g, _ := s.Get(ctx, &pb.GetRequest{Key: "k"})
	if g.Data[0] != 0 {
		t.Fatal("request data aliased")
	}
	g.Data[0] = 2
	g, _ = s.Get(ctx, &pb.GetRequest{Key: "k"})
	if g.Data[0] != 0 {
		t.Fatal("response data aliased")
	}
	r, e = s.Put(ctx, &pb.PutRequest{Key: "k", Data: make([]byte, 10*1024+1)})
	if e != nil || r.Status != pb.PutStatus_PUT_STATUS_TOO_LARGE {
		t.Fatal(r, e)
	}
	r, _ = s.Put(ctx, &pb.PutRequest{Key: "k"})
	if r.Rev != 2 {
		t.Fatal("oversize consumed revision", r)
	}
	d, e := s.Delete(ctx, &pb.DeleteRequest{Key: "missing"})
	if e != nil || d.Status != pb.DeleteStatus_DELETE_STATUS_NOT_FOUND {
		t.Fatal(d, e)
	}
	d, _ = s.Delete(ctx, &pb.DeleteRequest{Key: "k", Rev: 99})
	if d.Status != pb.DeleteStatus_DELETE_STATUS_NOT_FOUND {
		t.Fatal(d)
	}
}
func TestValidationAndBatches(t *testing.T) {
	s := New()
	for _, call := range []func() error{
		func() error { _, e := s.Get(ctx, &pb.GetRequest{}); return e },
		func() error { _, e := s.Put(ctx, &pb.PutRequest{}); return e },
		func() error { _, e := s.Delete(ctx, &pb.DeleteRequest{}); return e },
		func() error {
			_, e := s.BatchGet(ctx, &pb.BatchGetRequest{Items: make([]*pb.GetRequest, 1001)})
			return e
		},
		func() error {
			_, e := s.BatchPut(ctx, &pb.BatchPutRequest{Items: make([]*pb.PutRequest, 1001)})
			return e
		},
		func() error {
			_, e := s.BatchPut(ctx, &pb.BatchPutRequest{Items: []*pb.PutRequest{{Key: "untouched"}, {}}})
			return e
		},
	} {
		if e := call(); status.Code(e) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument: %v", e)
		}
	}
	g, _ := s.Get(ctx, &pb.GetRequest{Key: "untouched"})
	if g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("invalid batch partially applied")
	}
	r, e := s.BatchPut(ctx, &pb.BatchPutRequest{Items: []*pb.PutRequest{{Key: "k", ExpectedRev: ptr(0)}, {Key: "k", ExpectedRev: ptr(0)}, {Key: "k"}, {Key: "k", Data: make([]byte, 10241)}}})
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Items) != 4 || r.Items[0].Rev != 1 || r.Items[1].Status != pb.PutStatus_PUT_STATUS_CONFLICT || r.Items[2].Rev != 2 || r.Items[3].Status != pb.PutStatus_PUT_STATUS_TOO_LARGE {
		t.Fatal(r)
	}
	b, e := s.BatchGet(ctx, &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "k", Rev: 1}, {Key: "none"}, {Key: "k"}, {Key: "k", Rev: 3}}})
	if e != nil || len(b.Items) != 4 || b.Items[0].Rev != 1 || b.Items[1].Status != pb.GetStatus_GET_STATUS_MISS || b.Items[2].Rev != 2 || b.Items[3].Status != pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND {
		t.Fatal(b, e)
	}
	for _, n := range []int{0, 1000} {
		items := make([]*pb.PutRequest, n)
		for i := range items {
			items[i] = &pb.PutRequest{Key: "bulk"}
		}
		b, e := s.BatchPut(ctx, &pb.BatchPutRequest{Items: items})
		if e != nil || len(b.Items) != n {
			t.Fatal(e)
		}
	}
}
func TestConcurrentConditionalCreate(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	results := make(chan *pb.PutResponse, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Put(ctx, &pb.PutRequest{Key: "k", ExpectedRev: ptr(0)})
			if e != nil {
				t.Error(e)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	wins := 0
	for r := range results {
		if r.Status == pb.PutStatus_PUT_STATUS_OK {
			wins++
		}
	}
	if wins != 1 {
		t.Fatal(wins)
	}
}
