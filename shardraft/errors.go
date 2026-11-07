package shardraft

import (
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NotLeaderError is returned before a request is proposed. The hint is advisory.
// A timeout or leadership loss AFTER proposing is not this error and is unsafe
// to retry automatically without application-level deduplication.
type NotLeaderError struct {
	LeaderID uint64
	Address  string
	Term     uint64
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not leader (leader=%d, term=%d)", e.LeaderID, e.Term)
}
func (e *NotLeaderError) GRPCStatus() *status.Status {
	s := status.New(codes.FailedPrecondition, e.Error())
	withDetails, err := s.WithDetails(&errdetails.ErrorInfo{
		Reason: "NOT_LEADER", Domain: "oracache.shardraft",
		Metadata: map[string]string{"leader_id": strconv.FormatUint(e.LeaderID, 10), "address": e.Address, "term": strconv.FormatUint(e.Term, 10)},
	})
	if err != nil {
		return s
	}
	return withDetails
}

func unavailable(message string) error { return status.Error(codes.Unavailable, message) }
