// Package cache implements a timeless, unlimited, local-only cache gateway.
package cache

import (
	"context"
	"math"
	"sync"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const MaxPayload = 10 * 1024
const MaxBatch = 1000

type entry struct {
	sequence uint64
	versions map[uint64][]byte
}

// Server serializes RPC state access. No TTL, retention limit or replication
// is performed. Empty entries preserve their sequence after deletion.
type Server struct {
	pb.UnimplementedCacheGatewayServer
	mu   sync.Mutex
	keys map[string]*entry
}

func New() *Server { return &Server{keys: make(map[string]*entry)} }
func validKey(key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	return nil
}
func latest(e *entry) uint64 {
	var rev uint64
	if e != nil {
		for r := range e.versions {
			if r > rev {
				rev = r
			}
		}
	}
	return rev
}
func (s *Server) get(r *pb.GetRequest) *pb.GetResponse {
	e := s.keys[r.Key]
	last := latest(e)
	if last == 0 {
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}
	}
	rev := r.Rev
	if rev == 0 {
		rev = last
	}
	data, ok := e.versions[rev]
	if !ok {
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND, LatestRev: last}
	}
	return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: rev, LatestRev: last, Data: append([]byte(nil), data...)}
}
func (s *Server) put(r *pb.PutRequest) (*pb.PutResponse, error) {
	if len(r.Data) > MaxPayload {
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_TOO_LARGE}, nil
	}
	e := s.keys[r.Key]
	last := latest(e)
	if r.ExpectedRev != nil && *r.ExpectedRev != last {
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_CONFLICT, Rev: last}, nil
	}
	if e == nil {
		e = &entry{versions: make(map[uint64][]byte)}
		s.keys[r.Key] = e
	}
	if e.sequence == math.MaxUint64 {
		return nil, status.Error(codes.ResourceExhausted, "revision sequence exhausted")
	}
	e.sequence++
	e.versions[e.sequence] = append([]byte(nil), r.Data...)
	return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_OK, Rev: e.sequence}, nil
}
func (s *Server) Get(_ context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(r), nil
}
func (s *Server) Put(_ context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.put(r)
}
func (s *Server) Delete(_ context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validKey(r.GetKey()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.keys[r.Key]
	result := &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_NOT_FOUND}
	if latest(e) == 0 {
		return result, nil
	}
	if r.Rev == 0 {
		clear(e.versions)
	} else {
		if _, ok := e.versions[r.Rev]; !ok {
			return result, nil
		}
		delete(e.versions, r.Rev)
	}
	result.Status = pb.DeleteStatus_DELETE_STATUS_DELETED
	return result, nil
}
func (s *Server) BatchGet(_ context.Context, r *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	if r == nil || len(r.Items) > MaxBatch {
		return nil, status.Error(codes.InvalidArgument, "batch must contain at most 1000 items")
	}
	for _, item := range r.Items {
		if err := validKey(item.GetKey()); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &pb.BatchGetResponse{Items: make([]*pb.GetResponse, 0, len(r.Items))}
	for _, item := range r.Items {
		out.Items = append(out.Items, s.get(item))
	}
	return out, nil
}
func (s *Server) BatchPut(_ context.Context, r *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	if r == nil || len(r.Items) > MaxBatch {
		return nil, status.Error(codes.InvalidArgument, "batch must contain at most 1000 items")
	}
	for _, item := range r.Items {
		if err := validKey(item.GetKey()); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &pb.BatchPutResponse{Items: make([]*pb.PutResponse, 0, len(r.Items))}
	for _, item := range r.Items {
		result, err := s.put(item)
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, result)
	}
	return out, nil
}
