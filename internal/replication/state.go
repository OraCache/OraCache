package replication

import (
	"encoding/json"
	"fmt"
	"math"

	pb "github.com/OraCache/oracache/gen/replication/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config describes a fixed, one-way replication stream for one shard.
type Config struct {
	TargetRegion  string
	SourceRegion  string
	SourceGroupID string
	OutboxLimit   int // zero selects 10000 events
}

// StateOptions supplies the local group identity and stream configuration.
type StateOptions struct {
	Region  string
	ShardID uint32
	GroupID string
	Config  Config
}

type identity struct {
	Region  string
	ShardID uint32
	GroupID string
	Config  Config
}

type outboxItem struct {
	ID  uint64
	Key string
}

// stateData is the private snapshot representation. Its JSON shape is stable.
type stateData struct {
	Identity  identity
	NextID    uint64
	AckedID   uint64
	Outbox    []outboxItem
	AppliedID uint64
}

// State owns the outbox and incoming position. All methods must be called by
// the Raft owner loop; State does not provide concurrent access.
// The zero value represents disabled replication.
type State struct {
	data stateData
}

func NewState(opts StateOptions) (*State, error) {
	rc := opts.Config
	if rc.OutboxLimit == 0 {
		rc.OutboxLimit = 10000
	}
	if rc.OutboxLimit < 1 {
		return nil, fmt.Errorf("replication: outbox limit must be positive")
	}
	if rc.TargetRegion != "" {
		if rc.TargetRegion == opts.Region || rc.SourceRegion != "" || rc.SourceGroupID != "" {
			return nil, fmt.Errorf("replication: source needs a different target region without an incoming stream")
		}
	}
	if (rc.SourceRegion == "") != (rc.SourceGroupID == "") || (rc.SourceRegion != "" && rc.SourceRegion == opts.Region) {
		return nil, fmt.Errorf("replication: incoming stream needs a different source region and its group ID")
	}
	id := identity{Config: rc}
	if rc.TargetRegion != "" || rc.SourceRegion != "" {
		if opts.Region == "" || opts.GroupID == "" {
			return nil, fmt.Errorf("replication: region and group ID are required")
		}
		id.Region, id.ShardID, id.GroupID = opts.Region, opts.ShardID, opts.GroupID
	}
	return &State{data: stateData{Identity: id}}, nil
}

func (s *State) Config() Config {
	return s.data.Identity.Config
}

func (s *State) Configured() bool {
	return s.data.Identity.Region != ""
}

func (s *State) CanWrite() error {
	if s.Config().SourceRegion != "" {
		return status.Error(codes.FailedPrecondition, "replica region rejects client writes")
	}
	if s.Config().TargetRegion != "" && (len(s.data.Outbox) >= s.Config().OutboxLimit || s.data.NextID == math.MaxUint64) {
		return status.Error(codes.ResourceExhausted, "replication outbox is full or sequence exhausted")
	}
	return nil
}

func (s *State) Enqueue(key string) error {
	if err := s.CanWrite(); err != nil {
		return err
	}
	if s.Config().TargetRegion == "" {
		return nil
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "replication key is required")
	}
	s.data.NextID++
	s.data.Outbox = append(s.data.Outbox, outboxItem{ID: s.data.NextID, Key: key})
	return nil
}

func (s *State) Peek() (*pb.Entry, error) {
	if s.Config().TargetRegion == "" {
		return nil, status.Error(codes.FailedPrecondition, "outbox not configured")
	}
	if len(s.data.Outbox) == 0 {
		return nil, nil
	}
	item := s.data.Outbox[0]
	return &pb.Entry{Id: item.ID, Key: item.Key}, nil
}

func (s *State) Acknowledge(id uint64) error {
	if s.Config().TargetRegion == "" {
		return status.Error(codes.FailedPrecondition, "outbox not configured")
	}
	if id <= s.data.AckedID {
		return nil
	}
	if len(s.data.Outbox) == 0 || id != s.data.Outbox[0].ID {
		return status.Error(codes.FailedPrecondition, "outbox ACK must confirm the oldest item")
	}
	s.data.Outbox[0] = outboxItem{}
	s.data.Outbox = s.data.Outbox[1:]
	s.data.AckedID = id
	return nil
}

func (s *State) Apply(req *pb.ReplicateRequest, apply func(*pb.Entry) error) (*pb.ReplicateResponse, error) {
	id := s.data.Identity
	if req == nil || id.Config.SourceRegion == "" || req.SourceRegion != id.Config.SourceRegion ||
		req.StreamId != id.Config.SourceGroupID || req.TargetRegion != id.Region || req.ShardId != id.ShardID {
		return nil, status.Error(codes.FailedPrecondition, "replication source, stream, region or shard mismatch")
	}
	if req.Entry == nil || req.Entry.Id == 0 || req.Entry.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "replication entry is required")
	}
	if req.Entry.Id <= s.data.AppliedID {
		return &pb.ReplicateResponse{AppliedId: s.data.AppliedID}, nil
	}
	if s.data.AppliedID == math.MaxUint64 || req.Entry.Id != s.data.AppliedID+1 {
		return nil, status.Error(codes.FailedPrecondition, "gap in replication stream")
	}
	if err := apply(req.Entry); err != nil {
		return nil, err
	}
	s.data.AppliedID = req.Entry.Id
	return &pb.ReplicateResponse{AppliedId: s.data.AppliedID}, nil
}

func (s *State) Checkpoint() ([]byte, error) {
	return json.Marshal(s.data)
}

func (s *State) Restored(data []byte) (*State, error) {
	var r stateData
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Identity != s.data.Identity || r.AckedID > r.NextID ||
		uint64(len(r.Outbox)) != r.NextID-r.AckedID || len(r.Outbox) > r.Identity.Config.OutboxLimit {
		return nil, fmt.Errorf("replication: invalid checkpoint or identity")
	}
	if (r.Identity.Config.TargetRegion == "" && r.NextID != 0) || (r.Identity.Config.SourceRegion == "" && r.AppliedID != 0) {
		return nil, fmt.Errorf("replication: checkpoint has wrong role")
	}
	for i, item := range r.Outbox {
		if item.Key == "" || item.ID != r.AckedID+uint64(i)+1 {
			return nil, fmt.Errorf("replication: invalid checkpoint outbox order")
		}
	}
	return &State{data: r}, nil
}
